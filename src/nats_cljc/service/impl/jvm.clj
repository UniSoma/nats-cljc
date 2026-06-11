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
           [io.nats.client.support JsonValue JsonValue$Type]
           [io.nats.service Service ServiceEndpoint Endpoint ServiceMessage ServiceMessageHandler
            Discovery PingResponse InfoResponse StatsResponse EndpointStats]
           [java.time.format DateTimeFormatter DateTimeFormatterBuilder]
           [java.time ZonedDateTime]
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

;; ── Discovery (ADR 0024) ─────────────────────────────────────────────────────
;; jnats' `Discovery(conn, maxTimeMillis, maxResults)` IS the bounded fan-out: it
;; gathers $SRV.* replies into a List, terminating after `maxResults` replies or
;; `maxTimeMillis`, whichever first — exactly the portable `:max-results`/`:timeout-ms`
;; bound. We default both to jnats' own DEFAULT_DISCOVERY_* (5000ms / 10) so an
;; unbounded call still terminates predictably and matches the CLJS leg's defaults.

;; The one canonical timestamp format on this leg (UTC, exactly three fractional
;; digits) — the same `:started` form KV's `:created` uses, built the same way the
;; KV/JetStream impls pin theirs (duplicated, not shared, so no impl ns owns
;; another's date seam).
(def ^:private ^DateTimeFormatter canonical-instant
  (-> (DateTimeFormatterBuilder.) (.appendInstant 3) .toFormatter))

(defn- ->canonical-timestamp
  "Normalize a jnats ZonedDateTime to the canonical portable timestamp string."
  [^ZonedDateTime zdt]
  (.format canonical-instant (.toInstant zdt)))

(defn- json-value->edn
  "Walk a jnats `JsonValue` (the parsed per-endpoint custom stats `:data`) into EDN
   — map keys keywordized, byte-identical with the CLJS leg's `js->clj`-of-`JSON.parse`
   path (ADR 0024). This deliberately does NOT go through the connection codec: the
   stats `:data` is a polyglot JSON blob, not an application payload. Numbers collapse
   to the natural Clojure type the matching enum arm carries; an absent/NULL value is
   nil so the lift can drop the key."
  [^JsonValue jv]
  (when (and jv (not= JsonValue$Type/NULL (.-type jv)))
    (condp = (.-type jv)
      JsonValue$Type/MAP    (reduce-kv (fn [m k v] (assoc m (keyword k) (json-value->edn v)))
                                       {} (into {} (.-map jv)))
      JsonValue$Type/ARRAY  (mapv json-value->edn (.-array jv))
      JsonValue$Type/STRING (.-string jv)
      JsonValue$Type/BOOL   (.-bool jv)
      (.-number jv))))

(defn- assoc-some
  "Assoc `k`→`v` only when `v` is present (non-nil and, for strings/maps, non-empty),
   so the normalized maps stay byte-identical across legs — an absent native field
   (jnats nil, nats.js undefined) drops the key on both rather than surfacing as a
   leg-specific empty value."
  [m k v]
  (cond-> m (and (some? v) (not (and (or (string? v) (map? v)) (empty? v)))) (assoc k v)))

(defn- ping->edn
  "Lift a jnats `PingResponse` to the portable identity map, dropping the wire `type`
   discriminator (ADR 0024)."
  [^PingResponse p]
  (-> {:name (.getName p) :id (.getId p) :version (.getVersion p)}
      (assoc-some :metadata (some->> (.getMetadata p) (into {})))))

(defn- info-endpoint->edn
  [^Endpoint e]
  (-> {:name (.getName e) :subject (.getSubject e)}
      (assoc-some :queue-group (.getQueueGroup e))
      (assoc-some :metadata (some->> (.getMetadata e) (into {})))))

(defn- info->edn
  [^InfoResponse i]
  (-> {:name (.getName i) :id (.getId i) :version (.getVersion i)
       :description (.getDescription i)
       :endpoints (mapv info-endpoint->edn (.getEndpoints i))}
      (assoc-some :metadata (some->> (.getMetadata i) (into {})))))

(defn- stats-endpoint->edn
  [^EndpointStats es]
  (-> {:name                       (.getName es)
       :subject                    (.getSubject es)
       :num-requests               (.getNumRequests es)
       :num-errors                 (.getNumErrors es)
       :processing-time-ns         (.getProcessingTime es)
       :average-processing-time-ns (.getAverageProcessingTime es)}
      (assoc-some :queue-group (.getQueueGroup es))
      (assoc-some :last-error (.getLastError es))
      (assoc-some :data (json-value->edn (.getData es)))))

(defn- stats->edn
  [^StatsResponse s]
  (-> {:name (.getName s) :id (.getId s) :version (.getVersion s)
       :started (->canonical-timestamp (.getStarted s))
       :endpoints (mapv stats-endpoint->edn (.getEndpointStatsList s))}
      (assoc-some :metadata (some->> (.getMetadata s) (into {})))))

(defn- discovery
  "Build a jnats `Discovery` bounded by the portable `opts`, defaulting to jnats'
   own DEFAULT_DISCOVERY_* so an unbounded call still terminates."
  ^Discovery [^Connection client {:keys [max-results timeout-ms]}]
  (Discovery. client
              (long (or timeout-ms Discovery/DEFAULT_DISCOVERY_MAX_TIME_MILLIS))
              (int (or max-results Discovery/DEFAULT_DISCOVERY_MAX_RESULTS))))

(extend-type JvmConnection
  proto/Discovery
  (-ping [{:keys [^Connection client]} {:keys [name id] :as opts}]
    ;; jnats' Discovery has no List-returning 2-arg ping; narrowing to a single
    ;; instance (name + id) returns one response (or null when absent), so wrap it
    ;; into the same VECTOR the broadcast variants drain into (ADR 0024).
    (core/then
     (core/resolved nil)
     (fn [_]
       (let [d (discovery client opts)]
         (mapv ping->edn
               (cond (and name id) (remove nil? [(.ping d ^String name ^String id)])
                     name          (.ping d ^String name)
                     :else         (.ping d)))))))
  (-info [{:keys [^Connection client]} {:keys [name id] :as opts}]
    (core/then
     (core/resolved nil)
     (fn [_]
       (let [d (discovery client opts)]
         (mapv info->edn
               (cond (and name id) (remove nil? [(.info d ^String name ^String id)])
                     name          (.info d ^String name)
                     :else         (.info d)))))))
  (-stats [{:keys [^Connection client]} {:keys [name id] :as opts}]
    (core/then
     (core/resolved nil)
     (fn [_]
       (let [d (discovery client opts)]
         (mapv stats->edn
               (cond (and name id) (remove nil? [(.stats d ^String name ^String id)])
                     name          (.stats d ^String name)
                     :else         (.stats d))))))))
