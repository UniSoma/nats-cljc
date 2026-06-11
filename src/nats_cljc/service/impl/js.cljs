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
