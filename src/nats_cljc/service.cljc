(ns nats-cljc.service
  "Portable public facade for NATS Services — hosting a request-reply micro-service
   under one `.cljc` API (ADR 0024). The same consumer code compiles and runs on the
   JVM, the browser, and Node.

   Services is pure convenience over core request-reply: a queue-subscribed handler
   per endpoint plus the auto-responders on `$SRV.PING|INFO|STATS.*`. So, unlike
   `nats-cljc.kv` / `nats-cljc.jetstream`, there is NO service context and NOTHING
   is verified at entry — `create` hangs directly off the Connection, because there
   is no server feature to round-trip against (ADR 0024). A client invokes an
   endpoint with plain `nats-cljc.core/request`; there is no new verb for the
   caller.

   Requiring this namespace loads the per-leg service impl — and, on CLJS, pulls the
   `@nats-io/services` bundle bytes — which a consumer who never requires it does not
   pay for (ADR 0016/0026). The impl require is for that load side-effect only (it
   `extend`s the Service protocol onto the platform Connection record); this facade
   calls the record through the protocol."
  (:require [nats-cljc.codec :as codec]
            [nats-cljc.impl.msg :as msg]
            [nats-cljc.impl.protocol :as proto]
            [nats-cljc.service.impl.config :as config]
            #?(:clj  [nats-cljc.impl.jvm :as impl]
               :cljs [nats-cljc.impl.js :as impl])
            #?(:clj  [nats-cljc.service.impl.jvm]
               :cljs [nats-cljc.service.impl.js])))

;; The private key the handler-delivered message carries its per-leg native service
;; message under: `respond` reads it back to route the reply through the native
;; message so native per-endpoint stats stay correct (ADR 0024). Namespaced and
;; undocumented — a consumer threads the whole `msg` through to `respond`, never
;; this key directly, exactly as `core/reply` threads `:reply`.
(def ^:private ^:no-doc native-key ::native)

;; The two wire headers a service error reply carries — the same case-sensitive
;; names on both legs (jnats `ServiceMessage.NATS_SERVICE_ERROR*`, nats.js
;; `ServiceErrorHeader*`): the description and the integer code (carried as its
;; string form on the wire). `error` reads them back off a reply Message; the impls
;; set them when answering an error (ADR 0025).
(def ^:private ^:no-doc error-header "Nats-Service-Error")
(def ^:private ^:no-doc error-code-header "Nats-Service-Error-Code")

(defn- decode-request
  "Lift one raw request map `{:subject :bytes :reply :headers ::native}` into the
   public message the endpoint handler sees: `{:subject :data :reply ::native}`,
   `:data` decoded with the Service's bound `codec`. Mirrors core's `decode-msg`,
   but keeps the native service message under `::native` so `respond` can route
   through it (ADR 0024); `:headers` is added only when the request carried some."
  [codec {:keys [subject bytes reply headers] :as raw}]
  (cond-> {:subject subject
           :reply   reply
           native-key (get raw native-key)
           :data    (codec/decode codec bytes)}
    (seq headers) (assoc :headers (msg/trim-headers headers))))

(defn- prepare-endpoint
  "Wrap one declared endpoint for the impl: default `:subject` to `:name`, and wrap
   the public ADR-0007 `:handler` in the low-level handler the impl subscribes —
   decoding the raw request with the Service's `codec` and presenting the public
   message (the handler's returned promise, if any, flows straight back out so the
   impl's native backpressure engages, ADR 0007)."
  [codec {:keys [name subject handler] :as endpoint}]
  (assoc endpoint
         :subject (or subject name)
         :handler (fn [raw] (handler (decode-request codec raw)))))

(defn create
  "Create and start a Service on `conn` from the portable `config`, returning a
   platform-native promise (CompletableFuture on the JVM, js/Promise on CLJS) that
   resolves to a running Service — the value `stop` tears down. There is no context
   and no server feature to verify at entry: the promise resolves as soon as the
   Service's endpoints are subscribed (ADR 0024). The config IS validated pre-flight,
   before any native or wire call (ADR 0015): an omitted `:name` or `:version`
   rejects the promise with `:type :missing-required-key` carrying the offending
   `:key`, a malformed service or endpoint `:name` with `:invalid-name`, a non-semver
   `:version` with `:invalid-version` carrying the offending `:version`, and two
   endpoints sharing a `:name` with `:duplicate-endpoint` carrying the offending
   `:name`. Endpoint `:subject` syntax stays native/server-enforced; empty or absent
   `:endpoints` is legal.

   Config keys: `:name` (required), `:version` (required), `:description`,
   `:metadata`, and `:endpoints`, a vector of endpoint maps. Each endpoint is
   `{:name :subject :handler :queue-group :metadata}`: `:name` and `:handler` are
   required, `:subject` is the subject the endpoint listens on and DEFAULTS to
   `:name` when omitted, and `:queue-group` joins the endpoint's subscription to a
   queue group (the server load-balances each request to one member). `:handler`
   is an ordinary ADR-0007 push Handler, invoked per request with the decoded
   message `{:subject :data :reply}` and answering it with `respond`; a returned
   promise applies per-endpoint backpressure. There is no Group noun — a consumer
   composes a grouped subject directly (ADR 0024).

   The Service binds `conn`'s default codec at create, used to decode every request
   and encode every reply (the `:codec` create override is the codec slice)."
  [conn {:keys [endpoints] :as config}]
  (let [codec (:codec conn)]
    (-> (impl/resolved nil)
        (impl/then (fn [_] (config/validate-config config)))
        (impl/bind (fn [_]
                     (proto/-create-service
                      conn (assoc config :endpoints (mapv #(prepare-endpoint codec %) endpoints))))))))

(defn respond
  "Reply to a request message `msg` (the one an endpoint handler received) with
   `data`, encoding it with `conn`'s codec and answering through the request's
   native service message so the owning endpoint's native stats stay correct (ADR
   0024). Sugar over the native respond, the service analog of `core/reply`;
   returns nil. `opts` may set `:codec` to override the connection default, so a
   polyglot reply can match the request's codec (ADR 0011)."
  ([conn msg data] (respond conn msg data {}))
  ([conn msg data opts]
   (proto/-respond conn (get msg native-key)
                   (codec/encode (msg/effective-codec conn opts) data))
   nil))

(defn respond-error
  "Reply to a request message `msg` with a first-class service error (ADR 0025):
   an integer `code` and a string `description`, optionally carrying `data` as the
   reply body (encoded with `conn`'s codec, as `respond`), routed through the
   request's native service message so the owning endpoint's native error stats
   stay correct. Sets the `Nats-Service-Error` / `Nats-Service-Error-Code` headers
   the caller reads back with `error`; conn is threaded as in `respond`. Returns nil.

   This is a SUCCESSFUL reply carrying an error payload, not a transport failure —
   the caller's plain `core/request` resolves normally and branches on
   `(service/error reply)`, which is `{:code … :description …}` here (ADR 0025). A
   handler that throws or returns a rejected promise auto-replies the same shape
   with code 500; this is the explicit form. `opts` may set `:codec` to override the
   connection default for the `data` encode (ADR 0011)."
  ([conn msg code description] (respond-error conn msg code description nil {}))
  ([conn msg code description data] (respond-error conn msg code description data {}))
  ([conn msg code description data opts]
   (proto/-respond-error conn (get msg native-key) code description
                         (some->> data (codec/encode (msg/effective-codec conn opts))))
   nil))

(defn error
  "Read the service error a reply Message `msg` carries (ADR 0025): `nil` when the
   reply is a normal success, or `{:code <int> :description <string>}` when the
   Service answered with an error (`respond-error`, or the auto-500 on a thrown /
   rejected handler). The opt-in reader of the `Nats-Service-Error` /
   `Nats-Service-Error-Code` headers `core/request` leaves intact on the reply — a
   service error is data the caller branches on, NOT a thrown transport failure, so
   `core/request` resolves normally and never sniffs these headers (ADR 0025). The
   `:code` is returned as an integer, parsed from the header's wire string form."
  [msg]
  (let [headers (:headers msg)]
    (when-let [description (first (get headers error-header))]
      {:code        (#?(:clj Long/parseLong :cljs js/parseInt)
                     (first (get headers error-code-header)))
       :description description})))

(defn stop
  "Stop the Service `svc`, returning a platform-native promise that settles once it
   has stopped — tearing the Service's endpoints down (enough for test teardown;
   full drain semantics and a `:stopped` promise are the lifecycle slice, ADR 0024)."
  [svc]
  (proto/-stop-service svc))
