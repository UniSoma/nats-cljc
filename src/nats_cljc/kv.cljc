(ns nats-cljc.kv
  "Portable public facade for NATS Key/Value, the last-value registry built on
   JetStream. A thin `.cljc` surface mirroring `nats-cljc.jetstream`: the same
   consumer code compiles and runs on the JVM, the browser, and Node — and it
   speaks KV vocabulary throughout, never the stream substrate (ADR 0023):
   Buckets, not streams; `:bucket-not-found`, not `:stream-not-found`.

   Requiring this namespace loads the per-leg KV impl — and, on CLJS, pulls the
   `@nats-io/kv` bundle bytes — which a core-only consumer who never requires it
   does not pay for (ADR 0016). The impl require is for that load side-effect only
   (it `extend`s the KV protocol onto the platform Connection record); this facade
   calls the record through the protocol.

   `get`, `update`, and `keys` shadow clojure.core — the jetstream `next`
   precedent: the namespace-aliased call is the public name."
  (:refer-clojure :exclude [get update keys])
  (:require [nats-cljc.codec :as codec]
            [nats-cljc.impl.protocol :as proto]
            [nats-cljc.kv.impl.bucket :as bucket]
            #?(:clj  [nats-cljc.impl.jvm :as impl]
               :cljs [nats-cljc.impl.js :as impl])
            #?(:clj  [nats-cljc.kv.impl.jvm]
               :cljs [nats-cljc.kv.impl.js])))

(defn kv
  "Obtain the KV context for `conn`, returning a platform-native promise
   (CompletableFuture on the JVM, js/Promise on CLJS) that resolves to the single
   context every Bucket-lifecycle operation flows through — the KV twin of
   `(jetstream conn)` (ADR 0017).

   KV is JetStream-backed, so obtaining it verifies JetStream is enabled by
   forcing a JS-info round-trip on both legs: the promise rejects with an
   `ex-info` `:type :jetstream-not-enabled` (err 10039) when the server/account
   has JetStream disabled — at the handle, never deferred to the first operation
   (ADR 0017/0020)."
  [conn]
  (proto/-kv conn))

(defn- codec-override
  "Resolve an open/create `:codec` override once, into the `Prepared` the Bucket
   handle binds for every read and write through it (per-Bucket only — a Bucket's
   values are homogeneous, so there is no per-op override; ADR 0011's
   resolve-once, applied per handle instead of per connection). Returns nil when
   `opts` carries no override, leaving the connection default the impl bound; an
   unresolvable reference throws — inside a chain stage, so it rejects pre-flight,
   before any native call."
  [opts]
  (when (contains? opts :codec)
    (codec/prepare (:codec opts))))

(defn- bind-codec
  "Bind `override` (a `Prepared`, or nil for the connection default) onto the
   Bucket `handle` the impl resolved — the one place the per-Bucket codec choice
   lands on the handle, shared by `create-bucket` and `open-bucket`."
  [handle override]
  (cond-> handle override (assoc :codec override)))

(defn create-bucket
  "Create a Bucket on the KV context `ctx` from the portable, CLOSED kebab
   `config` map, returning a platform-native promise that resolves to a Bucket
   handle — the value every entry operation takes, binding the Bucket's one
   Codec: the connection's default, unless `opts` carries a `:codec` override
   (a registry keyword or `ICodec` instance), which then governs all reads and
   writes through this handle (per-Bucket only — no per-op override). Config
   keys: `:bucket` (required, the Bucket's name), `:description`, `:history`
   (revisions kept per key), `:ttl-ms` (integer milliseconds), `:max-value-size`,
   `:max-bucket-size` (bytes), `:storage` (`:file` | `:memory`), `:replicas`, and
   `:compression?`. The map is closed: an unrecognized key rejects the promise
   with a validation `:type :unknown-config-key`, an omitted `:bucket` with
   `:missing-required-key`, and a malformed Bucket name with `:invalid-name`, all
   pre-flight before any native call (ADR 0015). A config the SERVER rejects
   surfaces as an operational `:type :jetstream-api-error` carrying
   `{:code :description}` (ADR 0020)."
  ([ctx config] (create-bucket ctx config {}))
  ([ctx config opts]
   (-> (impl/resolved nil)
       (impl/then (fn [_]
                    (bucket/validate-config config)
                    (codec-override opts)))
       (impl/bind (fn [override]
                    (impl/then (proto/-create-bucket ctx config)
                               #(bind-codec % override)))))))

(defn open-bucket
  "Open the existing Bucket named `bucket` on the KV context `ctx`, returning a
   platform-native promise that resolves to a Bucket handle (see `create-bucket`
   — including the `opts` `:codec` override the handle binds in place of the
   connection default). Opening VERIFIES the Bucket exists, so the promise
   rejects with an operational `:type :bucket-not-found` when it does not (ADR
   0023) — at the handle, never deferred to the first entry operation — and
   pre-flight with a validation `:type :invalid-name` when `bucket` is malformed
   (ADR 0015)."
  ([ctx bucket] (open-bucket ctx bucket {}))
  ([ctx bucket opts]
   (-> (impl/resolved nil)
       (impl/then (fn [_]
                    (bucket/validate-name bucket)
                    (codec-override opts)))
       (impl/bind (fn [override]
                    (impl/then (proto/-open-bucket ctx bucket)
                               #(bind-codec % override)))))))

(defn delete-bucket
  "Delete the Bucket named `bucket` on the KV context `ctx` — decommissioning the
   Bucket and every entry in it — returning a platform-native promise that
   resolves to nil once it is gone. The promise rejects with an operational
   `:type :bucket-not-found` when no such Bucket exists (ADR 0023), and
   pre-flight with a validation `:type :invalid-name` when `bucket` is malformed
   (ADR 0015)."
  [ctx bucket]
  (-> (impl/resolved nil)
      (impl/then (fn [_] (bucket/validate-name bucket)))
      (impl/bind (fn [_] (proto/-delete-bucket ctx bucket)))))

(defn bucket-names
  "Enumerate the Buckets on the KV context `ctx`, returning a platform-native
   promise that resolves to a vector of Bucket name strings — the KV twin of
   `stream-names`, fully realized rather than paged."
  [ctx]
  (proto/-bucket-names ctx))

(defn list-buckets
  "Enumerate the Buckets on the KV context `ctx`, returning a platform-native
   promise that resolves to a vector of normalized status maps (see
   `bucket-status`), one per Bucket — the KV twin of `list-streams`."
  [ctx]
  (proto/-list-buckets ctx))

(defn bucket-status
  "Read the status of the Bucket named `bucket` on the KV context `ctx`,
   returning a platform-native promise that resolves to one normalized status
   map, identical in shape on every leg: the bucket-config keys as the server
   applied them — `:bucket`, `:description` (nil when none is set), `:history`,
   `:ttl-ms` (0 when none), `:max-value-size` / `:max-bucket-size` (-1 when
   unlimited), `:storage`, `:replicas`, and `:compression?` — plus the observed
   counters `:values` (live entry count) and `:bytes` (stored size). The promise
   rejects with an operational `:type :bucket-not-found` when no such Bucket
   exists (ADR 0023), and pre-flight with a validation `:type :invalid-name`
   when `bucket` is malformed (ADR 0015)."
  [ctx bucket]
  (-> (impl/resolved nil)
      (impl/then (fn [_] (bucket/validate-name bucket)))
      (impl/bind (fn [_] (proto/-bucket-status ctx bucket)))))

(defn put
  "Write `value` under `key` in the Bucket `handle`, encoded through the Bucket's
   one Codec, returning a platform-native promise that resolves to the new
   Revision as a bare number — immediately usable as the expected Revision of a
   follow-up compare-and-set. The promise rejects pre-flight with a validation
   `:type :invalid-key` (carrying `:key`) when `key` is malformed, before any
   wire call (ADR 0015), and with `:type :codec-error` when the value does not
   encode (ADR 0006)."
  [handle key value]
  (-> (impl/resolved nil)
      (impl/then (fn [_]
                   (bucket/validate-key key)
                   (codec/encode (:codec handle) value)))
      (impl/bind (fn [bytes] (proto/-kv-put handle key bytes)))))

(defn create
  "Write `value` under `key` in the Bucket `handle` only when the key is ABSENT
   — first-writer-wins, enabling initialization and locks — encoded through the
   Bucket's one Codec, returning a platform-native promise that resolves to the
   new Revision as a bare number. A key that already exists is a lost
   compare-and-set race: the promise rejects with the operational
   `:type :wrong-revision` carrying the contested `:key` (ADR 0023) — KV
   vocabulary, never the stream substrate's. It also rejects pre-flight with a
   validation `:type :invalid-key` (carrying `:key`) when `key` is malformed
   (ADR 0015), and with `:type :codec-error` when the value does not encode
   (ADR 0006)."
  [handle key value]
  (-> (impl/resolved nil)
      (impl/then (fn [_]
                   (bucket/validate-key key)
                   (codec/encode (:codec handle) value)))
      (impl/bind (fn [bytes] (proto/-kv-create handle key bytes)))))

(defn update
  "Write `value` under `key` in the Bucket `handle` only when `revision` is
   still the key's latest Revision — the revision-guarded write, so concurrent
   writers cannot silently clobber each other — encoded through the Bucket's one
   Codec, returning a platform-native promise that resolves to the new Revision
   as a bare number. A stale expected Revision is a lost compare-and-set race:
   the promise rejects with the operational `:type :wrong-revision` carrying the
   contested `:key` (ADR 0023), sharing one canonical face with a lost `create`.
   It also rejects pre-flight with a validation `:type :invalid-key` (carrying
   `:key`) when `key` is malformed (ADR 0015), and with `:type :codec-error`
   when the value does not encode (ADR 0006)."
  [handle key value revision]
  (-> (impl/resolved nil)
      (impl/then (fn [_]
                   (bucket/validate-key key)
                   (codec/encode (:codec handle) value)))
      (impl/bind (fn [bytes] (proto/-kv-update handle key bytes revision)))))

(defn get
  "Read the latest Entry for `key` from the Bucket `handle`, returning a
   platform-native promise that resolves to an Entry — a plain map
   `{:bucket :key :value :revision :created :operation}` whose `:value` is
   decoded through the Bucket's one Codec — or to nil when the key is absent
   (never written, deleted, or purged): absence is a normal domain outcome to
   branch on with if-let, not an Error, and a STORED nil stays distinguishable as
   `{:value nil ...}` (ADR 0023).

   `opts` may carry `:revision`, pinning the read to that exact past Revision —
   cheap archaeology on the Bucket. A pinned read does NOT hide markers: pinned
   to a delete/purge marker Revision it delivers the marker Entry, its
   `:operation` (`:delete` / `:purge`) visible and `:value` nil undecoded
   (identical on both legs — normalized, since the natives diverge here); a
   Revision the Bucket never assigned, or one belonging to another key, resolves
   to nil.

   The promise rejects pre-flight with a validation `:type :invalid-key`
   (carrying `:key`) when `key` is malformed, before any wire call (ADR 0015),
   and with `:type :codec-error` when the stored bytes do not decode (ADR 0006)."
  ([handle key] (get handle key {}))
  ([handle key opts]
   (-> (impl/resolved nil)
       (impl/then (fn [_] (bucket/validate-key key)))
       (impl/bind (fn [_] (proto/-kv-get handle key (:revision opts))))
       (impl/then (fn [raw]
                    (when raw
                      {:bucket    (:bucket raw)
                       :key       (:key raw)
                       :value     (when (= :put (:operation raw))
                                    (codec/decode (:codec handle) (:bytes raw)))
                       :revision  (:revision raw)
                       :created   (:created raw)
                       :operation (:operation raw)}))))))

(defn delete
  "Write a Tombstone for `key` in the Bucket `handle`, returning a
   platform-native promise that resolves to nil: the key subsequently reads as
   absent via `get`, while `history` retains the Tombstone — so deletion stays
   observable to history readers and watchers (ADR 0023). `opts` may carry the
   optional `:revision` guard: when present the Tombstone lands only if it is
   still the key's latest Revision, and a stale guard rejects with the
   operational `:type :wrong-revision` carrying the contested `:key` — operators
   only remove what they believe they are removing. The promise also rejects
   pre-flight with a validation `:type :invalid-key` (carrying `:key`) when
   `key` is malformed (ADR 0015)."
  ([handle key] (delete handle key {}))
  ([handle key opts]
   (-> (impl/resolved nil)
       (impl/then (fn [_] (bucket/validate-key key)))
       (impl/bind (fn [_] (proto/-kv-delete handle key (:revision opts)))))))

(defn purge
  "Erase the history of `key` in the Bucket `handle` down to a single purge
   marker — reclaiming space for good, where `delete` keeps history readable —
   returning a platform-native promise that resolves to nil (ADR 0023). `opts`
   may carry the optional `:revision` guard, exactly as on `delete`: a stale
   guard rejects with the operational `:type :wrong-revision` carrying the
   contested `:key`. The promise also rejects pre-flight with a validation
   `:type :invalid-key` (carrying `:key`) when `key` is malformed (ADR 0015)."
  ([handle key] (purge handle key {}))
  ([handle key opts]
   (-> (impl/resolved nil)
       (impl/then (fn [_] (bucket/validate-key key)))
       (impl/bind (fn [_] (proto/-kv-purge handle key (:revision opts)))))))

(defn- watch-entry
  "Lift a raw watch delivery into the portable Entry the Handler receives: the
   `history` shape — a live Entry's `:value` decoded through the Bucket's one
   Codec, a Tombstone/purge marker carrying `:value` nil undecoded, `:delta` the
   Entry's distance from the newest matching delivery (0 once caught up; both
   natives populate it on watch deliveries — verified, not inferred)."
  [handle raw]
  {:bucket    (:bucket raw)
   :key       (:key raw)
   :value     (when (= :put (:operation raw))
                (codec/decode (:codec handle) (:bytes raw)))
   :revision  (:revision raw)
   :created   (:created raw)
   :operation (:operation raw)
   :delta     (:delta raw)})

(defn watch
  "Watch the Bucket `handle` for changes — the open-ended observation that feels
   exactly like a core subscription: each matching Entry (the `history` shape,
   `:value` decoded through the Bucket's one Codec, Tombstones and purge markers
   included with `:value` nil) is pushed to `handler`, the library's many-times
   currency (ADR 0007: deliveries are serial within one Watch, a handler must
   never block, and a returned promise suspends the next delivery until it
   settles). Returns a platform-native promise that resolves to a watch handle
   whose `:initialized` key holds a Promise resolving when the initial replay
   completes — the \"cache is warm\" signal, so cache builders populate first and
   serve reads after — and which `stop` ends, idempotently.

   `opts` may carry the one CLOSED `:deliver` mode replacing the natives' flag
   set: `:latest` (the default) replays each key's current value then streams
   updates, `:history` replays the full retained history first, `:updates`
   streams only new changes (its `:initialized` resolves immediately — nothing
   replays). Any other value rejects pre-flight with a validation
   `:type :invalid-deliver` carrying the offending `:deliver`, before any native
   call (ADR 0015)."
  ([handle handler] (watch handle handler {}))
  ([handle handler opts]
   (-> (impl/resolved nil)
       (impl/then (fn [_] (bucket/validate-deliver (:deliver opts :latest))))
       (impl/bind (fn [deliver]
                    (proto/-kv-watch handle deliver
                                     (fn [raw] (handler (watch-entry handle raw)))))))))

(defn stop
  "End the Watch behind `watch-handle` (the value `watch` resolved to), ceasing
   delivery to its Handler. Fire-and-forget — returns nil synchronously — and
   idempotent: stopping an already-ended Watch is a safe no-op (ADR 0012
   spirit), so teardown never needs racy liveness guards."
  [watch-handle]
  (proto/-watch-stop watch-handle))

(defn history
  "Read the retained history of `key` from the Bucket `handle`, returning a
   platform-native promise that resolves to a fully-realized vector of Entries
   oldest-to-newest — INCLUDING Tombstones and purge markers, each marker's
   `:operation` (`:delete` / `:purge`) visible (ADR 0023). Each Entry is the
   `get` map plus `:delta`, the Entry's distance from the key's newest revision
   (0 for the newest — both natives populate it on history reads); a live
   Entry's `:value` is decoded through the Bucket's one Codec, while a marker
   carries `:value` nil undecoded. An absent key resolves to [] (the server
   bounds retained history per the Bucket's `:history`, 64 max per key). The
   promise rejects pre-flight with a validation `:type :invalid-key` (carrying
   `:key`) when `key` is malformed (ADR 0015), and with `:type :codec-error`
   when a retained value does not decode (ADR 0006)."
  [handle key]
  (-> (impl/resolved nil)
      (impl/then (fn [_] (bucket/validate-key key)))
      (impl/bind (fn [_] (proto/-kv-history handle key)))
      (impl/then (fn [raws]
                   (mapv (fn [raw]
                           {:bucket    (:bucket raw)
                            :key       (:key raw)
                            :value     (when (= :put (:operation raw))
                                         (codec/decode (:codec handle) (:bytes raw)))
                            :revision  (:revision raw)
                            :created   (:created raw)
                            :operation (:operation raw)
                            :delta     (:delta raw)})
                         raws)))))

(defn keys
  "Enumerate the LIVE keys in the Bucket `handle`, returning a platform-native
   promise that resolves to a fully-realized vector of key strings — deleted and
   purged keys excluded, so a Bucket's live contents are enumerable (the
   stream-names / consumer-names precedent; ADR 0023). `filter` is an optional
   subject-style filter restricting the result (e.g. `\"user.>\"`); without it
   every live key enumerates, and a filter matching nothing resolves to []."
  ([handle] (keys handle nil))
  ([handle filter]
   (proto/-kv-keys handle filter)))
