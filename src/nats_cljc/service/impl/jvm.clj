(ns ^:no-doc nats-cljc.service.impl.jvm
  "JVM Services implementation (ADR 0024). jnats `io.nats.service` interop is
   quarantined here (ADR 0005), kept out of the core `nats-cljc.impl.jvm`: this ns
   `extend`s the Service protocol onto that ns's `JvmConnection` record, so
   `(service/create conn …)` hosts a Service without the core record itself
   depending on services — the structural mirror of the CLJS confinement that keeps
   a core-only bundle service-free (ADR 0016/0026).

   `io.nats.service` ships in the jnats jar, so there is no extra JVM dependency
   (ADR 0026). Unlike KV/JetStream there is no context and no entry verification:
   `Service.builder().connection(conn)…build().startService()` is the whole of
   create (ADR 0024)."
  (:require [nats-cljc.impl.protocol :as proto]
            [nats-cljc.impl.jvm :as core])
  (:import [nats_cljc.impl.jvm JvmConnection]
           [io.nats.client Connection]
           [io.nats.client.impl Headers]
           [io.nats.service Service ServiceEndpoint Endpoint ServiceMessage ServiceMessageHandler]
           [java.util.concurrent CompletableFuture CompletionStage]))

(defn- msg->raw
  "Lift a jnats `ServiceMessage` into the raw map the facade decodes (the core
   `msg->raw` shape) plus `::native` — the message itself — under
   `nats-cljc.service/native`, which the facade's `respond` routes the reply
   through so the endpoint's native stats stay correct (ADR 0024). The headers/
   reply/subject lift reuses the core `JvmConnection` helper rather than rebuilding
   it; `ServiceMessage` is not a `Message` subtype, so its fields are read directly."
  [^ServiceMessage msg]
  {:subject               (.getSubject msg)
   :bytes                 (.getData msg)
   :reply                 (.getReplyTo msg)
   :headers               nil
   :nats-cljc.service/native msg})

(defn- ->endpoint
  "Build a jnats `ServiceEndpoint` from one prepared endpoint map (`:subject`
   already defaulted by the facade, `:handler` already the low-level decode wrapper).
   The handler's `onMessage` blocks the dispatcher thread on a returned
   `CompletionStage` (ADR 0007 road 2, as core subscribe), so promise-return
   backpressure and serial per-endpoint delivery fall out for free."
  [{:keys [name subject handler queue-group metadata]}]
  (let [^ServiceMessageHandler smh
        (reify ServiceMessageHandler
          (onMessage [_ msg]
            (let [r (handler (msg->raw msg))]
              (when (instance? CompletionStage r)
                (.join (.toCompletableFuture ^CompletionStage r))))))
        eb (-> (Endpoint/builder)
               (.name name)
               (.subject subject))
        _  (when metadata (.metadata eb ^java.util.Map metadata))
        seb (-> (ServiceEndpoint/builder)
                (.endpoint (.build eb))
                (.handler smh))]
    (when queue-group (.endpointQueueGroup seb ^String queue-group))
    (.build seb)))

;; The Service handle the facade resolves to: the jnats `Service` to stop, plus the
;; `stopped` future the facade's consumers read as `(:stopped handle)` — the
;; lifecycle parallel of the Watch handle's `initialized` (ADR 0024). `startService`
;; hands back a CompletableFuture that completes when the Service stops for any
;; reason, so `stopped` is that future mapped to nil. `stopped?` makes stop
;; idempotent OUR way (ADR 0012 spirit): a second stop is a silent no-op.
(defrecord JvmService [^Service svc ^CompletableFuture stopped stopped?]
  proto/ServiceLifecycle
  (-stop-service [_]
    ;; jnats' Service.stop() is void and DRAINS by default — an in-flight handler
    ;; (the dispatcher thread blocked in `onMessage`) runs to completion and its
    ;; reply lands before teardown, never dropped mid-request (ADR 0024). Run it
    ;; off-thread (ADR 0002), then resolve the returned promise once `stopped`
    ;; settles, so the promise resolves AFTER teardown completes.
    (core/then
     (do (when (compare-and-set! stopped? false true)
           (CompletableFuture/runAsync (reify Runnable (run [_] (.stop svc)))))
         stopped)
     (fn [_] nil))))

(extend-type JvmConnection
  proto/Service
  (-create-service [{:keys [^Connection client]} {:keys [name version description metadata endpoints]}]
    ;; No context, no entry verification (ADR 0024): build the Service with its
    ;; endpoints and start it. `startService` returns a CompletableFuture that
    ;; completes when the Service stops; we carry it on the handle as `stopped`
    ;; (mapped to nil) and resolve the handle now — the endpoints' subscriptions
    ;; are live synchronously once `build` returns.
    (core/then
     (core/resolved nil)
     (fn [_]
       (let [sb (-> (Service/builder)
                    (.connection client)
                    (.name name)
                    (.version version))
             _  (when description (.description sb ^String description))
             _  (when metadata (.metadata sb ^java.util.Map metadata))
             _  (doseq [ep endpoints] (.addServiceEndpoint sb (->endpoint ep)))
             ^Service svc (.build sb)
             native-stopped (.startService svc)
             stopped (core/then native-stopped (fn [_] nil))]
         (->JvmService svc stopped (atom false))))))
  (-respond [{:keys [^Connection client]} ^ServiceMessage native ^bytes bytes]
    (.respond native client bytes)
    nil)
  (-respond-error [{:keys [^Connection client]} ^ServiceMessage native code description ^bytes bytes]
    ;; jnats' `respondStandardError` carries no body, so build the error headers
    ;; ourselves and route through the regular `respond(conn, bytes, headers)` —
    ;; same reply path, so the endpoint's native error stats count it and an
    ;; optional `data` body rides along (ADR 0025). The two header names are jnats'
    ;; own public constants; the code is its string wire form.
    (let [h (doto (Headers.)
              (.add ServiceMessage/NATS_SERVICE_ERROR ^java.util.Collection [(str description)])
              (.add ServiceMessage/NATS_SERVICE_ERROR_CODE ^java.util.Collection [(str code)]))]
      (.respond native client ^bytes (or bytes (byte-array 0)) h))
    nil))
