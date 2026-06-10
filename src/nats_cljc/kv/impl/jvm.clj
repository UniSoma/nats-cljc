(ns ^:no-doc nats-cljc.kv.impl.jvm
  "JVM KV implementation (ADR 0003/0016/0017). jnats KeyValue interop is
   quarantined here (ADR 0005), kept out of the core `nats-cljc.impl.jvm`: this ns
   `extend`s the KV protocol onto that ns's `JvmConnection` record, so `(kv conn)`
   vends a context without the core record itself depending on KV — the structural
   mirror of the CLJS confinement that keeps a core-only bundle KV-free (ADR 0016).

   jnats' `keyValueManagement()` is a cheap local construction that never touches
   the server, so a JS-disabled server would not surface until the first real
   operation. Obtaining the context therefore forces a `$JS.API.INFO` round-trip,
   off-thread, so `:jetstream-not-enabled` surfaces at the handle (ADR 0017) —
   the same forced verify `(jetstream conn)` does, with the same transient/closing
   disambiguation, so the classifiers are shared with the JetStream impl ns."
  (:require [nats-cljc.impl.protocol :as proto]
            [nats-cljc.impl.jvm :as core]
            [nats-cljc.jetstream.impl.jvm :as jet-jvm]
            [nats-cljc.jetstream.impl.error :as jet-err]
            [nats-cljc.kv.impl.bucket :as bucket]
            [nats-cljc.kv.impl.error :as kv-err])
  (:import [nats_cljc.impl.jvm JvmConnection]
           [io.nats.client Connection JetStreamApiException KeyValue KeyValueManagement]
           [io.nats.client.api ServerInfo KeyValueConfiguration StorageType]
           [java.io IOException]
           [java.time Duration]
           [java.util.concurrent CompletableFuture ExecutorService RejectedExecutionException]
           [java.util.function Supplier]))

;; The KV context (ADR 0017's twin): the handle wrapping jnats' management-plane
;; `KeyValueManagement` object every Bucket-lifecycle operation flows through.
;; `codec` is the connection's default (the resolved `Prepared`), captured at entry
;; so each Bucket handle binds it (ADR 0011). `io-executor` is the connection's
;; per-connection IO pool, carried so every off-thread KV op runs there instead of
;; the shared commonPool (the connection owns its lifecycle). `client` is the
;; owning jnats Connection, carried so opening a Bucket can construct its
;; per-Bucket `KeyValue` object.
(defrecord JvmKvContext [^KeyValueManagement kvm codec ^ExecutorService io-executor ^Connection client])

;; The Bucket handle: the per-Bucket jnats `KeyValue` object the entry operations
;; dispatch over, plus the codec the Bucket binds (the context's — i.e. the
;; connection default) and the IO pool its blocking calls run on. `bucket` is the
;; Bucket's name, carried so KV-faced errors can name it (ADR 0023).
(defrecord JvmBucket [^KeyValue kv codec ^ExecutorService io-executor bucket])

(extend-type JvmConnection
  proto/KV
  (-kv [conn]
    ;; The same shape as the JetStream entry point, for the same reasons: `then`
    ;; re-wraps so a rejection surfaces as the BARE ex-info (ADR 0006), and
    ;; `supplyAsync` runs the blocking round-trip off the caller's thread (ADR
    ;; 0002) on the connection's IO pool. keyValueManagement()/jetStreamManagement()
    ;; are cheap LOCAL constructions whose only failure is jnats' ensureNotClosing
    ;; on a closing/closed connection — the OUTER catch, keyed on structured
    ;; connection status. The forced getAccountStatistics is the verify-at-entry
    ;; $JS.API.INFO round-trip (ADR 0017); removing it would defer the JS-disabled
    ;; failure to the first operation.
    (let [^Connection client           (:client conn)
          ^ExecutorService io-executor (:io-executor conn)]
      (core/then
       (CompletableFuture/supplyAsync
        (reify Supplier
          (get [_]
            (try
              (let [kvm (.keyValueManagement client)
                    jsm (.jetStreamManagement client)]
                (try
                  (.getAccountStatistics jsm)
                  (catch JetStreamApiException e
                    ;; A server-issued JetStream API error (e.g. an account-level
                    ;; disable → 10039): normalize via the shared table (ADR 0020).
                    (throw (ex-info (.getMessage e)
                                    {:type (jet-err/api-error-type (.getApiErrorCode e))} e)))
                  (catch IOException e
                    ;; No usable response: the round-trip-free server flag
                    ;; disambiguates a transient blip from a true disable, exactly
                    ;; as the JetStream entry point does (ADR 0017).
                    (let [available? (.isJetStreamAvailable ^ServerInfo (.getServerInfo client))]
                      (throw (ex-info (if available?
                                        "JetStream INFO request timed out"
                                        "JetStream is not enabled on the server or account")
                                      {:type (jet-jvm/verify-io-type available?)} e)))))
                (->JvmKvContext kvm (:codec conn) io-executor client))
              (catch IOException e
                ;; Reached ONLY from the constructions above (ensureNotClosing on a
                ;; non-open connection); keyed on connection status, not message
                ;; text (ADR 0006).
                (throw (ex-info (.getMessage e)
                                {:type (jet-jvm/closing-type (.getStatus client))} e))))))
        io-executor)
       identity))))

(defn- api-ex->ex-info
  "Normalize a jnats JetStreamApiException reaching the KV layer to the portable
   operational ex-info, KV-faced: the err_code routes through the KV table — a
   not-found Bucket is 10059 ⇒ `:bucket-not-found`, never the stream-layer `:type`
   (ADR 0023) — and carries `{:code :description}`."
  [^JetStreamApiException e]
  (ex-info (.getMessage e)
           (kv-err/api-error-data (.getApiErrorCode e) (.getErrorDescription e))
           e))

(defn- off-thread
  "Run the blocking KV thunk `f` off the caller's thread (ADR 0002) on the
   connection's IO `executor` — the KV twin of the JetStream impl's `off-thread`,
   differing only in routing a server-issued JetStreamApiException through the
   KV-faced normalization (ADR 0023). Delivers the BARE ex-info on rejection (ADR
   0006); a submit racing the connection's close surfaces as the same retry-able
   `:connection-closed` a closed connection's round-trip would."
  [^ExecutorService executor f]
  (core/then
   (try
     (CompletableFuture/supplyAsync
      (reify Supplier
        (get [_]
          (try (f)
               (catch JetStreamApiException e (throw (api-ex->ex-info e))))))
      executor)
     (catch RejectedExecutionException _
       (CompletableFuture/failedFuture
        (ex-info "Connection is closed" {:type :connection-closed}))))
   identity))

(defn ->kv-config
  "Build a jnats KeyValueConfiguration from the portable closed kebab `config`
   (already validated by the facade). Only the keys present are set, so an absent
   key takes the server default; `:storage` routes through the shared wire table
   and `:ttl-ms` becomes a Duration (the CLJS leg passes integer ms)."
  ^KeyValueConfiguration [config]
  (let [b (KeyValueConfiguration/builder)]
    (.name b ^String (:bucket config))
    (when-let [description (:description config)]
      (.description b ^String description))
    (when-let [history (:history config)]
      (.maxHistoryPerKey b (int history)))
    (when-let [ttl-ms (:ttl-ms config)]
      (.ttl b (Duration/ofMillis ttl-ms)))
    (when-let [max-value-size (:max-value-size config)]
      (.maxValueSize b (long max-value-size)))
    (when-let [max-bucket-size (:max-bucket-size config)]
      (.maxBucketSize b (long max-bucket-size)))
    (when-let [storage (:storage config)]
      (.storageType b (StorageType/get (bucket/storage->wire storage))))
    (when-let [replicas (:replicas config)]
      (.replicas b (int replicas)))
    (when (some? (:compression? config))
      (.compression b (boolean (:compression? config))))
    (.build b)))

(defn- bucket-handle
  "Construct the Bucket handle for `bucket` on `ctx`. jnats' `keyValue(bucket)` is
   a cheap local construction (the existence check is the caller's — create's
   server round-trip or open's forced getStatus), binding the context's codec."
  [ctx ^String bucket]
  (->JvmBucket (.keyValue ^Connection (:client ctx) bucket)
               (:codec ctx) (:io-executor ctx) bucket))

(extend-type JvmKvContext
  proto/BucketManager
  (-create-bucket [ctx config]
    (off-thread (:io-executor ctx)
                #(do (.create ^KeyValueManagement (:kvm ctx) (->kv-config config))
                     (bucket-handle ctx (:bucket config)))))
  (-open-bucket [ctx bucket]
    ;; getStatus is the forced existence round-trip (the open contract): a missing
    ;; Bucket raises the substrate's not-found 10059, re-faced :bucket-not-found
    ;; (ADR 0023) — never deferred to the first entry operation.
    (off-thread (:io-executor ctx)
                #(do (.getStatus ^KeyValueManagement (:kvm ctx) ^String bucket)
                     (bucket-handle ctx bucket))))
  (-delete-bucket [ctx bucket]
    (off-thread (:io-executor ctx)
                #(do (.delete ^KeyValueManagement (:kvm ctx) ^String bucket) nil))))
