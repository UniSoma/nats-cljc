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
            #?(:clj  [nats-cljc.service.impl.jvm]
               :cljs [nats-cljc.service.impl.js])))

;; The private key the handler-delivered message carries its per-leg native service
;; message under: `respond` reads it back to route the reply through the native
;; message so native per-endpoint stats stay correct (ADR 0024). Namespaced and
;; undocumented — a consumer threads the whole `msg` through to `respond`, never
;; this key directly, exactly as `core/reply` threads `:reply`.
(def ^:private ^:no-doc native-key ::native)

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
   and nothing is verified at entry: the promise resolves as soon as the Service's
   endpoints are subscribed (ADR 0024).

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
    (proto/-create-service
     conn (assoc config :endpoints (mapv #(prepare-endpoint codec %) endpoints)))))

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

(defn stop
  "Stop the Service `svc`, returning a platform-native promise that settles once it
   has stopped — tearing the Service's endpoints down (enough for test teardown;
   full drain semantics and a `:stopped` promise are the lifecycle slice, ADR 0024)."
  [svc]
  (proto/-stop-service svc))
