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
           [io.nats.client.api ServerInfo KeyValueConfiguration KeyValueEntry KeyValueStatus KeyValueWatcher KeyValueWatchOption StorageType]
           [io.nats.client.impl NatsKeyValueWatchSubscription]
           [java.io IOException]
           [java.time Duration ZonedDateTime]
           [java.time.format DateTimeFormatter DateTimeFormatterBuilder]
           [java.util.concurrent CompletableFuture CompletionStage ExecutorService RejectedExecutionException]
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

(defn- off-thread*
  "Run the blocking KV thunk `f` off the caller's thread (ADR 0002) on the
   connection's IO `executor`, routing a server-issued JetStreamApiException
   through `api-ex->info` — the per-verb-family normalization seam. Delivers the
   BARE ex-info on rejection (ADR 0006); a submit racing the connection's close
   surfaces as the same retry-able `:connection-closed` a closed connection's
   round-trip would."
  [^ExecutorService executor api-ex->info f]
  (core/then
   (try
     (CompletableFuture/supplyAsync
      (reify Supplier
        (get [_]
          (try (f)
               (catch JetStreamApiException e (throw (api-ex->info e))))))
      executor)
     (catch RejectedExecutionException _
       (CompletableFuture/failedFuture
        (ex-info "Connection is closed" {:type :connection-closed}))))
   identity))

(defn- off-thread
  "The Bucket-verb `off-thread*`: the KV twin of the JetStream impl's
   `off-thread`, differing only in routing a server-issued JetStreamApiException
   through the KV-faced normalization (ADR 0023)."
  [^ExecutorService executor f]
  (off-thread* executor api-ex->ex-info f))

(defn- cas-off-thread
  "The compare-and-set `off-thread*` for a CAS verb over `key`: a lost race (the
   substrate's wrong-last-sequence) is re-faced `:wrong-revision` carrying the
   contested `:key` (ADR 0023); any other API error keeps its Bucket-verb face."
  [^ExecutorService executor key f]
  (off-thread* executor
               (fn [^JetStreamApiException e]
                 (ex-info (.getMessage e)
                          (kv-err/cas-error-data (.getApiErrorCode e) (.getErrorDescription e) key)
                          e))
               f))

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

(defn- status->map
  "Curate a jnats KeyValueStatus into the normalized portable status map: the
   bucket-config keys as the server applied them — `:description` already nil
   when none is set, `:ttl-ms` lifted from jnats' Duration (the CLJS leg reads
   native ms), `:storage` back through the shared wire table — plus the observed
   `:values` / `:bytes` counters. One pinned shape on every leg."
  [^KeyValueStatus s]
  {:bucket          (.getBucketName s)
   :description     (.getDescription s)
   :history         (.getMaxHistoryPerKey s)
   :ttl-ms          (.toMillis (.getTtl s))
   :max-value-size  (.getMaxValueSize s)
   :max-bucket-size (.getMaxBucketSize s)
   :storage         (bucket/wire->storage (str (.getStorageType s)))
   :replicas        (.getReplicas s)
   :compression?    (.isCompressed s)
   :values          (.getEntryCount s)
   :bytes           (.getByteCount s)})

(defn- bucket-handle
  "Construct the Bucket handle for `bucket` on `ctx`. jnats' `keyValue(bucket)` is
   a cheap local construction (the existence check is the caller's — create's
   server round-trip or open's forced getStatus), binding the context's codec."
  [ctx ^String bucket]
  (->JvmBucket (.keyValue ^Connection (:client ctx) bucket)
               (:codec ctx) (:io-executor ctx) bucket))

;; The one canonical timestamp format on this leg (UTC, exactly three fractional
;; digits) — the same formatter the JetStream impl pins, duplicated rather than
;; exported from it so neither impl ns owns the other's date seam.
(def ^:private ^DateTimeFormatter canonical-instant
  (-> (DateTimeFormatterBuilder.) (.appendInstant 3) .toFormatter))

(defn- ->canonical-timestamp
  "Normalize a jnats ZonedDateTime to the canonical portable timestamp string."
  [^ZonedDateTime zdt]
  (.format canonical-instant (.toInstant zdt)))

;; jnats KeyValueOperation enum name → the portable Entry `:operation` keyword
;; (ADR 0023). Only :put is reachable through `-kv-get` (jnats' get already reads
;; tombstones as absent); :delete/:purge surface through `-kv-history`, whose
;; lift reuses this raw shape.
(def ^:private operation->kw
  {"PUT" :put "DELETE" :delete "PURGE" :purge})

(defn- entry->raw
  "Lift a jnats KeyValueEntry to the raw portable entry map the facade decodes:
   `{:bucket :key :bytes :revision :created :operation}`, `:bytes` the undecoded
   wire value and `:created` the canonical timestamp string."
  [^KeyValueEntry e]
  {:bucket    (.getBucket e)
   :key       (.getKey e)
   :bytes     (.getValue e)
   :revision  (.getRevision e)
   :created   (->canonical-timestamp (.getCreated e))
   :operation (operation->kw (.name (.getOperation e)))})

;; The watch handle (the value `-kv-watch` resolves with): the jnats watch
;; subscription to close on stop, plus the `initialized` future the facade's
;; consumers read as `(:initialized handle)`. `stopped?` makes stop idempotent
;; OUR way (ADR 0012 spirit): jnats' close routes through Dispatcher.unsubscribe,
;; which throws IllegalStateException on an already-removed subscription, so the
;; CAS guard (plus the belt-and-braces catch for a close racing the connection's
;; own teardown) turns the second stop into a silent no-op.
(defrecord JvmWatch [^NatsKeyValueWatchSubscription sub ^CompletableFuture initialized stopped?]
  proto/Watch
  (-watch-stop [_]
    (when (compare-and-set! stopped? false true)
      (try (.close sub) (catch Exception _ nil)))
    nil))

;; The portable :deliver mode → jnats KeyValueWatchOption flags. :latest is
;; jnats' optionless default (DeliverPolicy.LastPerSubject).
(def ^:private deliver->watch-options
  {:latest  []
   :history [KeyValueWatchOption/INCLUDE_HISTORY]
   :updates [KeyValueWatchOption/UPDATES_ONLY]})

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
                #(do (.delete ^KeyValueManagement (:kvm ctx) ^String bucket) nil)))
  (-bucket-names [ctx]
    ;; jnats' dedicated names endpoint — the stream-names precedent: never pays
    ;; for full statuses.
    (off-thread (:io-executor ctx)
                #(vec (.getBucketNames ^KeyValueManagement (:kvm ctx)))))
  (-list-buckets [ctx]
    (off-thread (:io-executor ctx)
                #(mapv status->map (.getStatuses ^KeyValueManagement (:kvm ctx)))))
  (-bucket-status [ctx bucket]
    ;; A missing Bucket raises the substrate's not-found 10059 from getStatus,
    ;; re-faced :bucket-not-found by off-thread's normalization (ADR 0023).
    (off-thread (:io-executor ctx)
                #(status->map (.getStatus ^KeyValueManagement (:kvm ctx) ^String bucket)))))

(extend-type JvmBucket
  proto/BucketEntries
  (-kv-put [bucket key bytes]
    ;; jnats' put returns the new revision directly — the bare number the facade
    ;; resolves with.
    (off-thread (:io-executor bucket)
                #(.put ^KeyValue (:kv bucket) ^String key ^bytes bytes)))
  (-kv-get [bucket key]
    ;; jnats' get already reads an absent OR tombstoned key as null (the portable
    ;; absent-is-nil contract, ADR 0023), so the lift is a plain when-let.
    (off-thread (:io-executor bucket)
                #(when-let [e (.get ^KeyValue (:kv bucket) ^String key)]
                   (entry->raw e))))
  (-kv-create [bucket key bytes]
    ;; jnats' create models first-writer-wins as an update expecting revision 0
    ;; (retrying over a tombstone), raising wrong-last-sequence on a live key —
    ;; re-faced :wrong-revision carrying the :key (ADR 0023).
    (cas-off-thread (:io-executor bucket) key
                    #(.create ^KeyValue (:kv bucket) ^String key ^bytes bytes)))
  (-kv-update [bucket key bytes revision]
    ;; jnats' update returns the new revision directly, raising
    ;; wrong-last-sequence when the expected revision is stale — re-faced
    ;; :wrong-revision carrying the :key (ADR 0023).
    (cas-off-thread (:io-executor bucket) key
                    #(.update ^KeyValue (:kv bucket) ^String key ^bytes bytes (long revision))))
  (-kv-delete [bucket key revision]
    ;; jnats' delete is void on both arities; a stale guard raises
    ;; wrong-last-sequence — re-faced :wrong-revision carrying the :key (ADR
    ;; 0023), the same CAS seam the writes route through. The unguarded arity
    ;; cannot lose a race, so the CAS routing is inert there.
    (cas-off-thread (:io-executor bucket) key
                    #(do (if revision
                           (.delete ^KeyValue (:kv bucket) ^String key (long revision))
                           (.delete ^KeyValue (:kv bucket) ^String key))
                         nil)))
  (-kv-purge [bucket key revision]
    ;; jnats' purge mirrors delete: void on both arities, wrong-last-sequence on
    ;; a stale guard — re-faced :wrong-revision carrying the :key (ADR 0023).
    (cas-off-thread (:io-executor bucket) key
                    #(do (if revision
                           (.purge ^KeyValue (:kv bucket) ^String key (long revision))
                           (.purge ^KeyValue (:kv bucket) ^String key))
                         nil)))
  (-kv-history [bucket key]
    ;; jnats' history hands back the full List<KeyValueEntry> oldest-to-newest
    ;; in one call (an absent key is an empty list, never an error); the lift is
    ;; the get raw shape plus :delta, which jnats populates on history entries
    ;; (the distance from the key's newest revision) — verified, not inferred.
    (off-thread (:io-executor bucket)
                #(mapv (fn [^KeyValueEntry e]
                         (assoc (entry->raw e) :delta (.getDelta e)))
                       (.history ^KeyValue (:kv bucket) ^String key)))))

(extend-type JvmBucket
  proto/BucketWatch
  (-kv-watch [bucket deliver raw-handler]
    ;; jnats' watchAll drives the Watch natively: a dedicated dispatcher delivers
    ;; serially to the KeyValueWatcher, whose endOfData IS the initialized signal
    ;; — jnats fires it immediately for UPDATES_ONLY, at subscribe time when
    ;; there is nothing to replay, else after the delta-0 entry. Blocking the
    ;; watch callback on a returned CompletionStage is road 2 again (ADR 0007):
    ;; per-Watch backpressure rides jnats' own dispatcher queue, exactly like the
    ;; core subscription leg. watchAll's construction round-trips the consumer
    ;; subscribe, hence off-thread (ADR 0002).
    (off-thread (:io-executor bucket)
                #(let [initialized (CompletableFuture.)
                       watcher     (reify KeyValueWatcher
                                     (watch [_ e]
                                       (let [r (raw-handler (assoc (entry->raw e) :delta (.getDelta ^KeyValueEntry e)))]
                                         (when (instance? CompletionStage r)
                                           (-> ^CompletionStage r .toCompletableFuture .join))))
                                     (endOfData [_]
                                       (.complete initialized nil)))
                       sub         (.watchAll ^KeyValue (:kv bucket) watcher
                                              ^"[Lio.nats.client.api.KeyValueWatchOption;"
                                              (into-array KeyValueWatchOption (deliver->watch-options deliver)))]
                   (->JvmWatch sub initialized (atom false))))))
