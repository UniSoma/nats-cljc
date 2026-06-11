(ns ^:no-doc nats-cljc.service.impl.js
  "ClojureScript Services implementation (ADR 0024). This is the ONE namespace that
   imports `@nats-io/services`; it is required only by the service facade, so a
   core-only consumer who never touches the facade keeps a service-free browser
   bundle — shadow-cljs's module graph excludes the unreachable npm dep (ADR
   0016/0026). It `extend`s the Service protocol onto the core `JsConnection`
   record (defined in `nats-cljc.impl.js`), mirroring the JVM confinement.

   Unlike KV/JetStream there is no context and no entry verification: `new Svcm(nc)`
   is a thin local factory and `.add(config)` only subscribes the endpoints — there
   is no server feature to round-trip against (ADR 0024)."
  (:require [nats-cljc.impl.protocol :as proto]
            [nats-cljc.impl.js :as core]
            ["@nats-io/services" :as services]))

;; The Service handle the facade resolves to: a thin wrapper over the native
;; nats.js `Service`. nats.js does NOT export its `ServiceImpl` class, so the
;; lifecycle protocol can't be extended onto it directly (the JVM leg extends the
;; referenceable `io.nats.service.Service` interface instead); wrap it in a record
;; here so `-stop-service` has a concrete type to dispatch on. The `stopped` field
;; is the facade-read `(:stopped handle)` — nats.js' own `svc.stopped` promise
;; (resolves to null|Error once the Service ends) mapped to nil, the lifecycle
;; parallel of the Watch handle's `initialized` (ADR 0024). Opaque to the consumer.
(defrecord JsService [^js svc stopped])

(defn- msg->raw
  "Lift a nats.js `ServiceMsg` into the raw map the facade decodes (the core
   `msg->raw` shape) plus `::native` — the message itself — under
   `nats-cljc.service/native`, which the facade's `respond` routes the reply
   through so the endpoint's native stats stay correct (ADR 0024)."
  [^js msg]
  {:subject               (.-subject msg)
   :bytes                 (.-data msg)
   :reply                 (.-reply msg)
   :headers               nil
   :nats-cljc.service/native msg})

(defn- add-endpoint!
  "Add one prepared endpoint to the started Service `svc` (`:subject` already
   defaulted by the facade, `:handler` already the low-level decode wrapper). The
   function handler makes nats.js dispatch via a callback subscription, invoking
   `(err, msg)` per request — the err arm closes the subscription, so deliver only
   on a normal message. nats.js auto-replies a 500 on a SYNCHRONOUS handler throw
   but does not await the handler's returned promise, so a rejected promise is
   awaited here to auto-reply 500 too (ADR 0025); per-endpoint backpressure (gating
   the next delivery on it) is still the serialization-gate slice's job."
  [^js svc {:keys [name subject handler queue-group]}]
  (let [opts #js {:subject subject
                  :handler (fn [err ^js msg]
                             (when-not err
                               ;; nats.js auto-replies a 500 on a SYNCHRONOUS handler
                               ;; throw, but does NOT await the handler's returned
                               ;; promise — so a handler that REJECTS would leave the
                               ;; caller hanging. Await it here and auto-reply 500
                               ;; ourselves on rejection, so both failure modes land
                               ;; the same error reply the JVM gets for free (ADR 0025).
                               (let [r (handler (msg->raw msg))]
                                 (when (and r (fn? (.-then r)))
                                   (.catch r (fn [e]
                                               (.respondError msg 500 (str (or (.-message e) e)) (js/Uint8Array.))))))))}]
    (when queue-group (set! (.-queue opts) queue-group))
    (.addEndpoint svc name opts)))

(extend-type core/JsConnection
  proto/Service
  (-create-service [{:keys [client]} {:keys [name version description metadata endpoints]}]
    ;; No context, no entry verification (ADR 0024): `new Svcm(nc).add(config)`
    ;; resolves to a started Service, then add each declared endpoint. The endpoints
    ;; can only be added post-`add` on nats.js (declare-then-create is the portable
    ;; intersection, ADR 0024), so chain them on.
    (let [svcm   (services/Svcm. client)
          config (cond-> #js {:name name :version version}
                   description (doto (aset "description" description))
                   metadata    (doto (aset "metadata" (clj->js metadata))))]
      (-> (.add svcm config)
          (.then (fn [^js svc]
                   (doseq [ep endpoints] (add-endpoint! svc ep))
                   ;; `svc.stopped` resolves to null|Error once the Service ends for
                   ;; any reason; map it to nil so `(:stopped handle)` is the same
                   ;; portable signal the JVM leg's future carries (ADR 0024).
                   (->JsService svc (.then (.-stopped svc) (fn [_] nil))))))))
  (-respond [_ ^js native bytes]
    ;; Route through the native ServiceMsg (not a bare publish to the reply
    ;; subject) so the owning endpoint's native stats stay correct (ADR 0024).
    (.respond native bytes)
    nil)
  (-respond-error [_ ^js native code description bytes]
    ;; nats.js' own `respondError(code, description, data?, opts?)` sets the two
    ;; error headers and routes through the ServiceMsg, so reuse it rather than
    ;; rebuilding the headers — an empty body when no `data` was given (ADR 0025).
    (.respondError native code (str description) (or bytes (js/Uint8Array.)))
    nil))

(extend-type JsService
  proto/ServiceLifecycle
  (-stop-service [{:keys [^js svc]}]
    ;; nats.js' Service.stop() DRAINS: it drains each endpoint subscription before
    ;; resolving, so an in-flight request runs to completion and its reply lands
    ;; before teardown, never dropped mid-request (ADR 0024). It returns the same
    ;; `stopped` promise (null|Error); map it to nil so stop resolves to nil after
    ;; teardown, idempotently (a second stop is a native no-op).
    (.then (.stop svc) (fn [_] nil))))

;; ── Discovery (ADR 0024) ─────────────────────────────────────────────────────
;; `new Svcm(nc).client(opts)` hands back a ServiceClient whose ping/info/stats
;; resolve a QueuedIterator of the JSON-decoded $SRV.* replies. The `opts` are
;; nats.js' RequestManyOptions — the bounded fan-out: `count` strategy stops after
;; `maxMessages` replies OR `maxWait` ms, whichever first, exactly the portable
;; `:max-results`/`:timeout-ms` bound. Default both to 10 / 5000 to match the JVM
;; leg's jnats DEFAULT_DISCOVERY_* so an unbounded call terminates identically.

(defn- ->request-many-opts
  "Build a nats.js RequestManyOptions from the portable discovery `opts`."
  [{:keys [max-results timeout-ms]}]
  #js {:strategy    "count"
       :maxMessages (or max-results 10)
       :maxWait     (or timeout-ms 5000)})

(defn- drain
  "Drain a nats.js QueuedIterator (the discovery fan-out) into a native promise of a
   VECTOR, lifting each already-JSON-decoded reply with `f` as it arrives. The
   iterator completes itself once the bounded requestMany strategy terminates, which
   ends the loop — the CLJS analog of the JVM Discovery List being fully gathered."
  [^js qi f]
  (let [it (.call (unchecked-get qi (.-asyncIterator js/Symbol)) qi)]
    (letfn [(step [acc]
              (.then (.next it)
                     (fn [^js r]
                       (if (.-done r)
                         acc
                         (step (conj acc (f (.-value r))))))))]
      (step []))))

(defn- assoc-some
  "Assoc `k`→`v` only when present (non-nil, non-empty for strings/maps), so an
   absent native field (nats.js undefined) drops the key — byte-identical with the
   JVM leg's `assoc-some`."
  [m k v]
  (cond-> m (and (some? v) (not (and (or (string? v) (map? v)) (empty? v)))) (assoc k v)))

(defn- ping->edn
  "Lift a JSON-decoded $SRV.PING reply (a plain JS object) to the portable identity
   map, dropping the wire `type` discriminator (ADR 0024)."
  [^js p]
  (-> {:name (.-name p) :id (.-id p) :version (.-version p)}
      (assoc-some :metadata (some-> (.-metadata p) (js->clj :keywordize-keys true)))))

(defn- info-endpoint->edn
  [^js e]
  (-> {:name (.-name e) :subject (.-subject e)}
      (assoc-some :queue-group (.-queue_group e))
      (assoc-some :metadata (some-> (.-metadata e) (js->clj :keywordize-keys true)))))

(defn- info->edn
  [^js i]
  (-> {:name (.-name i) :id (.-id i) :version (.-version i)
       :description (.-description i)
       :endpoints (mapv info-endpoint->edn (.-endpoints i))}
      (assoc-some :metadata (some-> (.-metadata i) (js->clj :keywordize-keys true)))))

(defn- stats-endpoint->edn
  [^js es]
  (-> {:name                       (.-name es)
       :subject                    (.-subject es)
       :num-requests               (.-num_requests es)
       :num-errors                 (.-num_errors es)
       :processing-time-ns         (.-processing_time es)
       :average-processing-time-ns (.-average_processing_time es)}
      (assoc-some :queue-group (.-queue_group es))
      (assoc-some :last-error (.-last_error es))
      ;; the per-endpoint custom `:data` blob arrives already JSON-decoded (nats.js'
      ;; `m.json()`); `js->clj` finishes the JSON→EDN lift WITHOUT the connection
      ;; codec (ADR 0024).
      (assoc-some :data (some-> (.-data es) (js->clj :keywordize-keys true)))))

(defn- stats->edn
  [^js s]
  (-> {:name (.-name s) :id (.-id s) :version (.-version s)
       ;; nats.js' `started` is an ISO date string; round-trip it through Date so
       ;; `:started` is byte-identical with the JVM leg and KV's `:created` form.
       :started (.toISOString (js/Date. (.-started s)))
       :endpoints (mapv stats-endpoint->edn (.-endpoints s))}
      (assoc-some :metadata (some-> (.-metadata s) (js->clj :keywordize-keys true)))))

(defn- gather
  "Drive one discovery verb: build a bounded ServiceClient, run `(verb-fn client)`
   (it resolves a QueuedIterator), drain it through `f`, and normalize a
   no-responders rejection — a narrowed fan-out that reached nobody — to an empty
   VECTOR, so the gather matches the JVM `Discovery` List that swallows the same
   condition (ADR 0024)."
  [client opts verb-fn f]
  (let [c (.client (services/Svcm. client) (->request-many-opts opts))]
    (-> (verb-fn c)
        (.then (fn [qi] (drain qi f)))
        (.catch (fn [e] (if (core/no-responders? e) [] (throw e)))))))

(extend-type core/JsConnection
  proto/Discovery
  (-ping [{:keys [client]} {:keys [name id] :as opts}]
    (gather client opts #(.ping ^js % (or name "") (or id "")) ping->edn))
  (-info [{:keys [client]} {:keys [name id] :as opts}]
    (gather client opts #(.info ^js % (or name "") (or id "")) info->edn))
  (-stats [{:keys [client]} {:keys [name id] :as opts}]
    (gather client opts #(.stats ^js % (or name "") (or id "")) stats->edn)))
