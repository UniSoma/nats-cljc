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
  (:require #?(:clj  [nats-cljc.impl.jvm :as impl]
               :cljs [nats-cljc.impl.js :as impl])
    #?(:clj  [nats-cljc.service.impl.jvm]
       :cljs [nats-cljc.service.impl.js])
    [nats-cljc.codec :as codec]
    [nats-cljc.impl.msg :as msg]
    [nats-cljc.impl.protocol :as proto]
    [nats-cljc.service.impl.config :as config]))

;; The private key the handler-delivered message carries its per-leg native service
;; message under: `respond` reads it back to route the reply through the native
;; message so native per-endpoint stats stay correct (ADR 0024). Namespaced and
;; undocumented — a consumer threads the whole `msg` through to `respond`, never
;; this key directly, exactly as `core/reply` threads `:reply`.
(def ^:private ^:no-doc native-key ::native)

;; The private key the handler-delivered message carries the Service's bound codec
;; under (ADR 0011): `create` binds one codec — the connection default unless
;; `:codec` in the create config overrides it — used to decode every request and
;; encode every reply. `respond` / `respond-error` read it back off the `msg` the
;; handler threads through, so a reply encodes in the Service's codec, not the bare
;; connection default — exactly as `decode-request` decoded the request with it. A
;; per-call `:codec` in `respond` opts still overrides this (the polyglot reply).
;; Namespaced and undocumented, threaded only via the whole `msg`, like `::native`.
(def ^:private ^:no-doc codec-key ::codec)

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
   through it (ADR 0024) and the Service's bound `codec` under `::codec` so
   `respond` / `respond-error` encode the reply with it (ADR 0011); `:headers` is
   added only when the request carried some."
  [codec {:keys [subject bytes reply headers] :as raw}]
  (cond-> {:subject subject
           :reply   reply
           native-key (get raw native-key)
           codec-key  codec
           :data    (codec/decode codec bytes)}
    (seq headers) (assoc :headers (msg/trim-headers headers))))

(defn- wire-metadata
  "Normalize a portable `:metadata` map (service- or endpoint-level) to its wire
   form — string keys and string values, a keyword contributing its NAME (no
   leading-colon artifact), anything else its string form — so a Service hosted on
   either leg serializes the same JSON object and discovery lifts it back
   identically (ADR 0024). nil passes through (no metadata declared)."
  [metadata]
  (when metadata
    (reduce-kv (fn [m k v]
                 (assoc m
                   (if (keyword? k) (name k) (str k))
                   (if (keyword? v) (name v) (str v))))
      {} metadata)))

(defn- prepare-endpoint
  "Wrap one declared endpoint for the impl: default `:subject` to `:name`, and wrap
   the public ADR-0007 `:handler` in the low-level handler the impl subscribes —
   decoding the raw request with the Service's `codec` and presenting the public
   message (the handler's returned promise, if any, flows straight back out so the
   impl's native backpressure engages, ADR 0007). Endpoint `:metadata` is
   normalized to its wire form here, so the impls only ever host string-keyed
   string-valued maps."
  [codec {:keys [name subject handler metadata] :as endpoint}]
  (assoc endpoint
    :subject (or subject name)
    :metadata (wire-metadata metadata)
    :handler (fn [raw] (handler (decode-request codec raw)))))

(defn create
  "Create and start a Service on `conn` from the portable `config`, returning a
   platform-native promise (CompletableFuture on the JVM, js/Promise on CLJS) that
   resolves to a running Service — the value [[stop]] tears down. There is no context
   and no server feature to verify at entry: the promise resolves as soon as the
   Service's endpoints are subscribed (ADR 0024). The config IS validated pre-flight,
   before any native or wire call (ADR 0015): an omitted `:name` or `:version`
   rejects the promise with `:type :missing-required-key` carrying the offending
   `:key`, a malformed service or endpoint `:name` with `:invalid-name`, a non-semver
   `:version` with `:invalid-version` carrying the offending `:version`, and two
   endpoints sharing a `:name` with `:duplicate-endpoint` carrying the offending
   `:name`. Endpoint `:subject` syntax stays native/server-enforced; empty or absent
   `:endpoints` is legal. Past pre-flight, issued on a closed connection the promise
   rejects with the canonical transport `:type :connection-closed` (ADR 0006).

   `config` keys:

   - `:name` — string, required; the Service's name.
   - `:version` — semver string, required.
   - `:description` — string, optional.
   - `:metadata` — map, optional; see below.
   - `:codec` — registry keyword or `ICodec`, optional; see below.
   - `:endpoints` — vector of endpoint maps, optional (empty or absent is legal).

   Each endpoint map is `{:name :subject :handler :queue-group :metadata}`:

   - `:name` — string, required; the endpoint's name.
   - `:handler` — required; an ordinary ADR-0007 push Handler (see below).
   - `:subject` — string; the subject the endpoint listens on, DEFAULTS to `:name`
     when omitted.
   - `:queue-group` — string; joins the endpoint's subscription to a queue group (the
     server load-balances each request to one member).
   - `:metadata` — map, optional; see below.

   `:handler` is invoked per request with the decoded message `{:subject :data
   :reply}` and answers it with [[respond]] (or [[respond-error]]); a returned promise
   applies per-endpoint backpressure. That backpressure contract holds identically on
   both legs but is realized differently (ADR 0007): the JVM blocks the dispatcher
   thread on the returned promise, while CLJS drives the endpoint's iterator and
   awaits the promise between pulls. There is no Group noun — a consumer composes a
   grouped subject directly (ADR 0024).

   `:metadata` — at the service level and per endpoint — is a flat map serialized
   onto the wire as a JSON object of STRING keys and STRING values, identically on
   both legs: a keyword key or value contributes its name (no leading colon),
   anything else its string form. Discovery ([[ping]]/[[info]]/[[stats]]) lifts it
   back as that same string-keyed map of strings.

   The Service binds ONE codec at create — `conn`'s default codec, unless `:codec`
   in `config` (a registry keyword or `ICodec` instance) overrides it — used to
   decode every request and encode every reply across all the Service's endpoints
   (ADR 0011). A single [[respond]] / [[respond-error]] may still override the codec
   per call. An unresolvable `:codec` rejects the create promise pre-flight with
   `:type :codec-error`.

   The resolved Service handle carries a `:stopped` key holding a platform-native
   promise that resolves to nil once the Service stops for any reason — the
   react-to-shutdown signal the lifecycle parallel of the Watch handle's
   `:initialized` (ADR 0024), so a consumer awaits it instead of polling. Pass the
   handle to [[stop]] to tear the Service down.

   Example (JVM; on ClojureScript await the promise instead of deref):

   ```clojure
   @(create conn
            {:name        \"calc\"
             :version     \"1.0.0\"
             :description \"adds numbers\"
             :metadata    {:team \"math\"}
             :endpoints   [{:name    \"add\"
                            :subject \"calc.add\"
                            :handler (fn [{:keys [data] :as msg}]
                                       (respond conn msg (apply + data)))}]})
   ```"
  [conn {:keys [endpoints] :as config}]
  (-> (impl/resolved nil)
      ;; Resolve the bound codec in a chain stage alongside validation, so an
      ;; unresolvable `:codec` rejects the create promise pre-flight (ADR 0011/0015)
      ;; rather than throwing synchronously: the create `:codec` override, resolved
      ;; once into a `Prepared` (as `connect` does for the default), else the
      ;; connection's already-resolved default.
    (impl/then (fn [_]
                 (config/validate-config config)
                 (if (contains? config :codec)
                   (codec/prepare (:codec config))
                   (:codec conn))))
    (impl/bind (fn [codec]
                 (proto/-create-service
                   conn (assoc config
                          :metadata (wire-metadata (:metadata config))
                          :endpoints (mapv #(prepare-endpoint codec %) endpoints)))))))

(defn- reply-codec
  "The codec a `respond` / `respond-error` reply encodes with: the per-call `:codec`
   in `opts` overrides everything, else the Service's bound codec the handler's `msg`
   carries under `::codec` (the create default or its `:codec` override), else — for
   a `msg` not from a Service handler — the connection default (ADR 0011)."
  [conn msg opts]
  (or (:codec opts) (get msg codec-key) (:codec conn)))

(defn respond
  "Reply to a request message `msg` (the one an endpoint handler received) with
   `data`, encoding it with the Service's bound codec and answering through the
   request's native service message so the owning endpoint's native stats stay
   correct (ADR 0024). Sugar over the native respond, the service analog of
   [[nats-cljc.core/reply]]; returns nil.

   `opts` (optional map):

   | key      | type                | default               | effect |
   |----------|---------------------|-----------------------|--------|
   | `:codec` | keyword or `ICodec` | Service's bound codec | Override encoding for this single reply. |

   Throws synchronously (encode is eager, not deferred to a promise) with `:type
   :codec-error` if the resolved codec cannot encode `data` — including an
   unresolvable per-call `:codec`."
  ([conn msg data] (respond conn msg data {}))
  ([conn msg data opts]
    (proto/-respond conn (get msg native-key)
      (codec/encode (reply-codec conn msg opts) data))
    nil))

(defn respond-error
  "Reply to a request message `msg` with a first-class service error (ADR 0025):
   an integer `code` and a string `description`, optionally carrying `data` as the
   reply body (encoded with the Service's bound codec, as `respond`), routed through the
   request's native service message so the owning endpoint's native error stats
   stay correct. Sets the `Nats-Service-Error` / `Nats-Service-Error-Code` headers
   the caller reads back with [[error]]; conn is threaded as in [[respond]]. Returns
   nil.

   This is a SUCCESSFUL reply carrying an error payload, not a transport failure —
   the caller's plain [[nats-cljc.core/request]] resolves normally and branches on
   `(service/error reply)`, which is `{:code … :description …}` here (ADR 0025). A
   handler that throws or returns a rejected promise auto-replies the same shape
   with code 500; this is the explicit form.

   `opts` (optional map):

   | key      | type                | default               | effect |
   |----------|---------------------|-----------------------|--------|
   | `:codec` | keyword or `ICodec` | Service's bound codec | Override encoding for this single error reply's `data`. |

   When `data` is given, throws synchronously with `:type :codec-error` if the
   resolved codec cannot encode it (including an unresolvable per-call `:codec`);
   with no `data` no encode runs and the reply body is empty.

   NOT terminal: like [[respond]] (and [[nats-cljc.core/reply]]) the handler keeps running after
   it, and exactly one reply reaches the wire. It does NOT move the endpoint's
   `num_errors` — both natives tally an endpoint error only on an UNCAUGHT handler
   failure (the auto-500 path), never on an error REPLY, and jnats offers no other
   lever on its counter, so the portable stats relay that native truth (ADR 0025)."
  ([conn msg code description] (respond-error conn msg code description nil {}))
  ([conn msg code description data] (respond-error conn msg code description data {}))
  ([conn msg code description data opts]
    (proto/-respond-error conn (get msg native-key) code description
      (some->> data (codec/encode (reply-codec conn msg opts))))
    nil))

(defn error
  "Read the service error carried by reply message `msg`.

   `msg` is the reply map returned by [[nats-cljc.core/request]]. Returns nil for a
   normal success, or `{:code <int> :description <string>}` when the Service
   answered with an error via [[respond-error]] or the auto-500 path for a thrown /
   rejected handler (ADR 0025). The `:code` is parsed from the
   `Nats-Service-Error-Code` header's wire string form.

   Takes no options. It does not throw for a normal reply with no service-error
   headers; malformed error headers may surface platform parse errors unchanged."
  [msg]
  (let [headers (:headers msg)]
    (when-let [description (first (get headers error-header))]
      {:code        (#?(:clj Long/parseLong :cljs js/parseInt)
                      (first (get headers error-code-header)))
       :description description})))

(defn stop
  "Stop Service `svc`.

   `svc` is the handle returned by [[create]]. Returns a platform-native promise
   that resolves to nil once the endpoints are torn down (ADR 0024). Stop drains
   in-flight requests: a handler running when `stop` is called may finish and send
   its reply. After stop settles, a fresh request to an endpoint rejects with core
   `:type :no-responders` (ADR 0006).

   Takes no options. Idempotent: a second `stop` is a safe no-op. There is no
   `reset` in v1."
  [svc]
  (proto/-stop-service svc))

(defn ping
  "Discover the running Services `conn` can reach, resolving a platform-native
   promise of a VECTOR of identity maps `{:name :id :version}` (and `:metadata` —
   a string-keyed map of string values, the one portable metadata shape — when a
   Service declared some) — the client side of the surface, hanging directly off
   the Connection (ADR 0024). There is no Discovery handle and no local introspection
   of a Service this same connection hosts: self-inspection is a wire request like
   any other, narrowed by `opts`.

   `opts` (optional map, every key optional):

   | key            | type       | default | effect |
   |----------------|------------|---------|--------|
   | `:name`        | string     | none    | Narrow to Services of that name. |
   | `:id`          | string     | none    | Narrow to a single instance, alone or with `:name`; an `:id` without a `:name` broadcasts then filters the gathered vector client-side on the instance id. |
   | `:max-results` | int        | `10`    | Stop the `$SRV.PING` fan-out after that many replies, so the gather terminates even when the Service count is unknown. |
   | `:timeout-ms`  | integer ms | `5000`  | Stop the fan-out after that long; whichever bound is hit first. |

   A zero-endpoint Service still answers, so it is discoverable here. A narrowed
   fan-out that reaches nobody resolves to an empty vector, NOT an error.

   Rejects with `:type :connection-closed` when issued on a closed connection, or
   `:type :drained` when the connection is draining (ADR 0006) — same canonical
   shape as [[nats-cljc.core/request]]. The result is normalized byte-identically across legs:
   kebab-case EDN with the wire `type` discriminator dropped."
  ([conn] (ping conn {}))
  ([conn opts] (proto/-ping conn opts)))

(defn info
  "Discover what the running Services `conn` can reach offer, resolving a platform-native
   promise of a VECTOR of info maps — each [[ping]] identity (`{:name :id :version}`,
   plus `:metadata` when declared) plus `:description` and `:endpoints`, a vector of
   `{:name :subject}` (and `:queue-group`/`:metadata` when set).

   `opts` (optional map, every key optional):

   | key            | type       | default | effect |
   |----------------|------------|---------|--------|
   | `:name`        | string     | none    | Narrow to Services of that name. |
   | `:id`          | string     | none    | Narrow to one instance; without `:name`, broadcasts then filters client-side. |
   | `:max-results` | int        | `10`    | Cap the fan-out reply count. |
   | `:timeout-ms`  | integer ms | `5000`  | Cap the fan-out duration; whichever bound is hit first. |

   A fan-out that reaches nobody resolves to an empty vector. Rejects with `:type
   :connection-closed` (closed connection) or `:type :drained` (draining connection)
   per ADR 0006. Normalized byte-identically across legs (ADR 0024)."
  ([conn] (info conn {}))
  ([conn opts] (proto/-info conn opts)))

(defn stats
  "Discover the running Services' instrumentation, resolving a platform-native promise of a
   VECTOR of stats maps — each [[ping]] identity (`{:name :id :version}`, plus
   `:metadata` when declared) plus `:started` (the canonical timestamp string, same
   form as KV `:created`) and `:endpoints`, a vector of per-endpoint counter maps
   `{:name :subject :num-requests :num-errors :processing-time-ns
   :average-processing-time-ns}` (and `:queue-group`/`:last-error` when set). A
   handled request moves `:num-requests`; an UNCAUGHT handler failure — a throw or a
   rejected promise, the auto-500 path — moves `:num-errors`; an explicit
   [[respond-error]] does not (ADR 0025). Durations are integer NANOSECONDS; the
   per-endpoint custom `:data` blob, when a Service supplies one, passes through as
   parsed JSON→EDN — NOT via the connection codec.

   `opts` (optional map, every key optional):

   | key            | type       | default | effect |
   |----------------|------------|---------|--------|
   | `:name`        | string     | none    | Narrow to Services of that name. |
   | `:id`          | string     | none    | Narrow to one instance; without `:name`, broadcasts then filters client-side. |
   | `:max-results` | int        | `10`    | Cap the fan-out reply count. |
   | `:timeout-ms`  | integer ms | `5000`  | Cap the fan-out duration; whichever bound is hit first. |

   A fan-out that reaches nobody resolves to an empty vector. Rejects with `:type
   :connection-closed` (closed connection) or `:type :drained` (draining connection)
   per ADR 0006. Normalized byte-identically across legs (ADR 0024)."
  ([conn] (stats conn {}))
  ([conn opts] (proto/-stats conn opts)))
