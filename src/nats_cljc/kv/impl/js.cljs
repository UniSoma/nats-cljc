(ns ^:no-doc nats-cljc.kv.impl.js
  "ClojureScript KV implementation (ADR 0003/0016/0017). This is the ONE namespace
   that imports `@nats-io/kv`; it is required only by the KV facade, so a core-only
   consumer who never touches the facade keeps a KV-free browser bundle —
   shadow-cljs's module graph excludes the unreachable npm dep (ADR 0016). It
   `extend`s the KV protocol onto the core `JsConnection` record (defined in
   `nats-cljc.impl.js`), mirroring the JVM confinement.

   Unlike the JetStream entry point, `new Kvm(nc)` is a cheap sync construction
   that never touches the server — so verify-at-entry (ADR 0017) is FORCED here via
   `jetstreamManager(nc)`'s native $JS.API.INFO round-trip, the inverse of the JVM
   leg's relationship to its native client. Same inversion on open: `kvm.open`
   merely binds, so the existence check is a forced status round-trip."
  (:require [nats-cljc.impl.protocol :as proto]
            [nats-cljc.impl.js :as core]
            [nats-cljc.jetstream.impl.js :as jet-js]
            [nats-cljc.kv.impl.bucket :as bucket]
            [nats-cljc.kv.impl.error :as kv-err]
            ["@nats-io/jetstream" :as jetstream]
            ["@nats-io/kv" :as kv]))

;; The KV context (ADR 0017's twin): the handle wrapping @nats-io/kv's
;; management-plane `Kvm` object every Bucket-lifecycle operation flows through.
;; `codec` is the connection's default (the resolved `Prepared`), captured at
;; entry so each Bucket handle binds it (ADR 0011).
(defrecord JsKvContext [kvm codec])

;; The Bucket handle: the per-Bucket @nats-io/kv `KV` object the entry operations
;; dispatch over, plus the codec the Bucket binds (the context's — i.e. the
;; connection default). `bucket` is the Bucket's name, carried so KV-faced errors
;; can name it (ADR 0023).
(defrecord JsBucket [kv codec bucket])

(extend-type core/JsConnection
  proto/KV
  (-kv [conn]
    ;; Kvm(nc) is a cheap sync construction with NO native verify, so the
    ;; $JS.API.INFO round-trip is forced through jetstreamManager(nc) — which
    ;; rejects when JetStream is disabled — before the context resolves (ADR 0017).
    ;; The rejection routes through the same verify normalization as the JetStream
    ;; entry point: only the no-responder is :jetstream-not-enabled.
    (let [client (:client conn)]
      (-> (jetstream/jetstreamManager client)
          (.then (fn [_] (->JsKvContext (kv/Kvm. client) (:codec conn))))
          (.catch (fn [e] (throw (jet-js/verify-error e))))))))

(defn kv-error
  "Normalize a nats.js JetStreamApiError reaching the KV layer to the portable
   operational ex-info, KV-faced: the err_code routes through the KV table — a
   not-found Bucket is 10059 ⇒ `:bucket-not-found`, never the stream-layer `:type`
   the substrate raises (ADR 0023) — and carries `{:code :description}`. Anything
   that is not a JetStreamApiError passes through unchanged for the slice that
   owns it."
  [^js e]
  (if (instance? jetstream/JetStreamApiError e)
    (let [api ^js (.apiError e)]
      (ex-info (.-message e)
               (kv-err/api-error-data (.-err_code api) (.-description api))
               e))
    e))

(defn- with-kv-error
  "Attach the shared Bucket-verb rejection tail: normalize a nats.js error through
   `kv-error` (ADR 0023) before it propagates — the KV twin of the JetStream
   impl's `with-api-error`."
  [p]
  (.catch p (fn [e] (throw (kv-error e)))))

(defn cas-error
  "Normalize a nats.js JetStreamApiError reaching a compare-and-set verb over
   `key`: a lost race (the substrate's wrong-last-sequence) is re-faced
   `:wrong-revision` carrying the contested `:key` (ADR 0023); any other API
   error keeps its Bucket-verb face. Anything that is not a JetStreamApiError
   passes through unchanged for the slice that owns it."
  [^js e key]
  (if (instance? jetstream/JetStreamApiError e)
    (let [api ^js (.apiError e)]
      (ex-info (.-message e)
               (kv-err/cas-error-data (.-err_code api) (.-description api) key)
               e))
    e))

(defn- with-cas-error
  "Attach the compare-and-set rejection tail for a CAS verb over `key`: normalize
   a nats.js error through `cas-error` (ADR 0023) before it propagates."
  [p key]
  (.catch p (fn [e] (throw (cas-error e key)))))

(defn ->kv-opts
  "Build the @nats-io/kv KvOptions object from the portable closed kebab `config`
   (already validated by the facade). Only the keys present are set, so an absent
   key takes the native default; `:storage` routes through the shared wire table,
   `:ttl-ms` is the native `ttl` (already ms on this leg — the JVM uses a
   Duration), `:max-bucket-size` the native `max_bytes`, and `clj->js` produces
   the string-keyed object @nats-io/kv reads — surviving advanced compilation."
  [config]
  (clj->js
   (cond-> {}
     (:description config)             (assoc :description (:description config))
     (:history config)                 (assoc :history (:history config))
     (:ttl-ms config)                  (assoc :ttl (:ttl-ms config))
     (:max-value-size config)          (assoc :maxValueSize (:max-value-size config))
     (:max-bucket-size config)         (assoc :max_bytes (:max-bucket-size config))
     (:storage config)                 (assoc :storage (bucket/storage->wire (:storage config)))
     (:replicas config)                (assoc :replicas (:replicas config))
     (some? (:compression? config))    (assoc :compression (boolean (:compression? config))))))

(defn- status->map
  "Curate a @nats-io/kv KvStatus into the normalized portable status map: the
   bucket-config keys as the server applied them — `:description` normalized to
   nil when none is set (nats.js reads an absent description as \"\"; jnats as
   null), `:ttl-ms` from the native ms `ttl`, `:storage` back through the shared
   wire table — plus the observed `:values` / `:bytes` counters. One pinned
   shape on every leg."
  [^js s]
  {:bucket          (.-bucket s)
   :description     (let [d (.-description s)] (when (seq d) d))
   :history         (.-history s)
   :ttl-ms          (.-ttl s)
   :max-value-size  (.-maxValueSize s)
   :max-bucket-size (.-max_bytes s)
   :storage         (bucket/wire->storage (.-storage s))
   :replicas        (.-replicas s)
   :compression?    (boolean (.-compression s))
   :values          (.-values s)
   :bytes           (.-size s)})

(extend-type JsKvContext
  proto/BucketManager
  (-create-bucket [ctx config]
    (-> (.create ^js (:kvm ctx) (:bucket config) (->kv-opts config))
        (.then (fn [kv] (->JsBucket kv (:codec ctx) (:bucket config))))
        with-kv-error))
  (-open-bucket [ctx bucket]
    ;; kvm.open binds WITHOUT a server round-trip, so the open contract's
    ;; existence check is forced via status() — a stream-info round-trip whose
    ;; not-found rejects re-faced :bucket-not-found (ADR 0023) — matching the JVM
    ;; leg's getStatus, never deferred to the first entry operation.
    (-> (.open ^js (:kvm ctx) bucket)
        (.then (fn [kv] (-> (.status ^js kv)
                            (.then (fn [_] (->JsBucket kv (:codec ctx) bucket))))))
        with-kv-error))
  (-delete-bucket [ctx bucket]
    ;; @nats-io/kv has no delete-by-name on Kvm: bind (no round-trip), then
    ;; destroy(). A missing Bucket rejects from the destroy itself, re-faced
    ;; :bucket-not-found (ADR 0023).
    (-> (.open ^js (:kvm ctx) bucket)
        (.then (fn [kv] (.destroy ^js kv)))
        (.then (fn [_] nil))
        with-kv-error))
  (-bucket-names [ctx]
    ;; Kvm has no dedicated names endpoint (unlike jnats' getBucketNames), so the
    ;; names derive from draining the same status Lister `-list-buckets` rides —
    ;; shape parity, not cadence parity.
    (-> (jet-js/drain-lister (.list ^js (:kvm ctx)) #(.-bucket ^js %) [])
        with-kv-error))
  (-list-buckets [ctx]
    (-> (jet-js/drain-lister (.list ^js (:kvm ctx)) status->map [])
        with-kv-error))
  (-bucket-status [ctx bucket]
    ;; kvm.open binds WITHOUT a server round-trip; status() is the stream-info
    ;; round-trip whose not-found rejects re-faced :bucket-not-found (ADR 0023),
    ;; matching the JVM leg's getStatus.
    (-> (.open ^js (:kvm ctx) bucket)
        (.then (fn [kv] (.status ^js kv)))
        (.then status->map)
        with-kv-error)))

;; @nats-io/kv KvEntry `operation` string → the portable Entry `:operation`
;; keyword (ADR 0023) — note nats.js says "DEL" where jnats says "DELETE".
;; Only :put is reachable through `-kv-get` (tombstones normalize to nil below);
;; :delete/:purge are here for the history/watch lifts that reuse this raw shape.
(def ^:private operation->kw
  {"PUT" :put "DEL" :delete "PURGE" :purge})

(defn- entry->raw
  "Lift a @nats-io/kv KvEntry to the raw portable entry map the facade decodes:
   `{:bucket :key :bytes :revision :created :operation}`, `:bytes` the undecoded
   Uint8Array and `:created` normalized to the canonical UTC-millis timestamp
   string via `Date#toISOString`, byte-identical to the JVM leg."
  [^js e]
  {:bucket    (.-bucket e)
   :key       (.-key e)
   :bytes     (.-value e)
   :revision  (.-revision e)
   :created   (.toISOString (js/Date. (.-created e)))
   :operation (operation->kw (.-operation e))})

(extend-type JsBucket
  proto/BucketEntries
  (-kv-put [bucket key bytes]
    ;; kv.put resolves the new revision directly — the bare number the facade
    ;; resolves with.
    (-> (.put ^js (:kv bucket) key bytes)
        with-kv-error))
  (-kv-get [bucket key]
    ;; Unlike jnats, nats.js' get surfaces a tombstoned key as an entry with a
    ;; DEL/PURGE operation; the portable contract reads it as absent (ADR 0023),
    ;; so anything that is not a live PUT normalizes to nil here.
    (-> (.get ^js (:kv bucket) key)
        (.then (fn [^js e]
                 (when (and e (= "PUT" (.-operation e)))
                   (entry->raw e))))
        with-kv-error))
  (-kv-create [bucket key bytes]
    ;; nats.js' create models first-writer-wins as a put expecting revision 0
    ;; (retrying over a tombstone), rejecting with wrong-last-sequence on a live
    ;; key — re-faced :wrong-revision carrying the :key (ADR 0023).
    (-> (.create ^js (:kv bucket) key bytes)
        (with-cas-error key)))
  (-kv-update [bucket key bytes revision]
    ;; kv.update resolves the new revision directly, rejecting with
    ;; wrong-last-sequence when the expected revision is stale — re-faced
    ;; :wrong-revision carrying the :key (ADR 0023).
    (-> (.update ^js (:kv bucket) key bytes revision)
        (with-cas-error key))))
