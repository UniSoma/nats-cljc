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
;; here so `-stop-service` has a concrete type to dispatch on. Opaque to the
;; consumer either way.
(defrecord JsService [^js svc])

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
   on a normal message; the handler's return flows back (its promise is not awaited
   natively here — per-endpoint backpressure is the serialization-gate slice)."
  [^js svc {:keys [name subject handler queue-group]}]
  (let [opts #js {:subject subject
                  :handler (fn [err ^js msg]
                             (when-not err (handler (msg->raw msg))))}]
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
                   (->JsService svc))))))
  (-respond [_ ^js native bytes]
    ;; Route through the native ServiceMsg (not a bare publish to the reply
    ;; subject) so the owning endpoint's native stats stay correct (ADR 0024).
    (.respond native bytes)
    nil))

(extend-type JsService
  proto/ServiceLifecycle
  (-stop-service [{:keys [^js svc]}]
    ;; nats.js' Service.stop() already returns a Promise; enough for test teardown —
    ;; full drain semantics are the lifecycle slice.
    (.stop svc)))
