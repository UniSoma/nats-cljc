(ns nats-cljc.jetstream
  "Portable public facade for JetStream (aliased `jet` — CLJS reserves `js` for
   host interop), the durable half of NATS. A thin `.cljc` surface mirroring
   `nats-cljc.core`: the same consumer code compiles and runs on the JVM, the
   browser, and Node.

   Requiring this namespace loads the per-leg JetStream impl — and, on CLJS, pulls
   the `@nats-io/jetstream` bundle bytes — which a core-only consumer who never
   requires it does not pay for (ADR 0016). The impl require is for that load
   side-effect only (it `extend`s the JetStream protocol onto the platform
   Connection record); this facade calls the record through the protocol."
  ;; `next` is the pull poll-one verb (ADR 0018); the alias `jet/next` is the public
  ;; name, so clojure.core/next is excluded rather than shadowed in-ns.
  (:refer-clojure :exclude [next])
  (:require #?(:clj  [nats-cljc.impl.jvm :as impl]
               :cljs [nats-cljc.impl.js :as impl])
    #?(:clj  [nats-cljc.jetstream.impl.jvm]
       :cljs [nats-cljc.jetstream.impl.js])
    [nats-cljc.codec :as codec]
    [nats-cljc.impl.msg :as msg]
    [nats-cljc.impl.protocol :as proto]
    [nats-cljc.jetstream.impl.acks :as acks]
    [nats-cljc.jetstream.impl.consumer :as consumer]
    [nats-cljc.jetstream.impl.pub :as pub]
    [nats-cljc.jetstream.impl.pull :as pull]
    [nats-cljc.jetstream.impl.refill :as refill]
    [nats-cljc.jetstream.impl.stream :as stream]))

(defn jetstream
  "Obtain the JetStream context for `conn`, returning a platform-native promise
   (CompletableFuture on the JVM, js/Promise on CLJS) that resolves to a single
   context holding both the data plane (publish, pull) and the management plane
   (stream/consumer admin) — every JetStream operation flows through it (ADR 0017).

   Obtaining it verifies JetStream is enabled by forcing a JS-info round-trip on
   both legs, so the promise rejects with an `ex-info` `:type :jetstream-not-enabled`
   (err 10039) when the server/account has JetStream disabled — at the handle,
   never deferred to the first operation (ADR 0017/0020)."
  [conn]
  (proto/-jetstream conn))

(defn create-stream
  "Create a Stream on the JetStream context `ctx` from the portable, CLOSED kebab
   `config` map.

   Returns a platform-native promise that resolves to the normalized StreamInfo map
   (`{:config :created :state}`, `:created` an ISO-8601 string).

   `config` keys (`:name` required, the rest optional):

   - `:name` — string; the Stream's name.
   - `:subjects` — vector of subject strings the Stream captures.
   - `:storage` — `:file` or `:memory`; where the Stream persists.
   - `:retention` — `:limits`, `:interest`, or `:work-queue`; the retention policy
     governing when messages are removed.
   - `:max-age-ms` — integer milliseconds; max age before a message ages out.

   The map is closed: an unrecognized key rejects the promise with a validation
   `:type :unknown-config-key`, and a malformed `:name` with `:invalid-name`, both
   pre-flight before any native call (ADR 0015). A config the SERVER rejects (e.g. a
   subject overlap) rejects with an operational `:type :jetstream-api-error` carrying
   `{:code :description}` — detected after the native call, so it is operational, not
   validation (ADR 0020).

   ```clojure
   @(create-stream ctx {:name \"ORDERS\" :subjects [\"orders.>\"] :storage :file})
   ```"
  [ctx config]
  (-> (impl/resolved nil)
    (impl/then (fn [_] (stream/validate-config config)))
    (impl/bind (fn [_] (proto/-create-stream ctx config)))))

(defn stream-info
  "Look up the Stream named `name` on the JetStream context `ctx`, returning a
   platform-native promise that resolves to the normalized StreamInfo map (see
   [[create-stream]]). The promise rejects with an operational `:type
   :stream-not-found` when no such Stream exists, and pre-flight with a validation
   `:type :invalid-name` when `name` is malformed (ADR 0015/0020)."
  [ctx name]
  (-> (impl/resolved nil)
    (impl/then (fn [_] (stream/validate-name name)))
    (impl/bind (fn [_] (proto/-stream-info ctx name)))))

(defn update-stream
  "Update an existing Stream's configuration on the JetStream context `ctx` from the
   portable, CLOSED kebab `config` map (same keys as [[create-stream]], `:name`
   naming the Stream to change).

   Returns a platform-native promise that resolves to the normalized StreamInfo map
   carrying the new active config. The keys present are MERGED over the Stream's
   current config — an absent key keeps its current value rather than reverting to a
   server default — so a retention or limit can change without restating the whole
   config (ADR 0020).

   The map is closed: an unrecognized key rejects pre-flight with a validation `:type
   :unknown-config-key`, and a malformed `:name` with `:invalid-name` (ADR 0015). The
   promise rejects with an operational `:type :stream-not-found` when no such Stream
   exists, and `:jetstream-api-error` carrying `{:code :description}` when the server
   rejects the change (e.g. an immutable field) (ADR 0020)."
  [ctx config]
  (-> (impl/resolved nil)
    (impl/then (fn [_] (stream/validate-config config)))
    (impl/bind (fn [_] (proto/-update-stream ctx config)))))

(defn purge-stream
  "Purge every message from the Stream named `name` on the JetStream context `ctx`,
   keeping the Stream definition itself, returning a platform-native promise that
   resolves to `{:purged <count>}` — the number of messages the server dropped. The
   promise rejects with an operational `:type :stream-not-found` when no such
   Stream exists, and pre-flight with a validation `:type :invalid-name` when
   `name` is malformed (ADR 0015/0020)."
  [ctx name]
  (-> (impl/resolved nil)
    (impl/then (fn [_] (stream/validate-name name)))
    (impl/bind (fn [_] (proto/-purge-stream ctx name)))))

(defn delete-stream
  "Delete the Stream named `name` on the JetStream context `ctx`, returning a
   platform-native promise that resolves to nil once it is gone. The promise
   rejects with an operational `:type :stream-not-found` when no such Stream
   exists, and pre-flight with a validation `:type :invalid-name` when `name` is
   malformed (ADR 0015/0020)."
  [ctx name]
  (-> (impl/resolved nil)
    (impl/then (fn [_] (stream/validate-name name)))
    (impl/bind (fn [_] (proto/-delete-stream ctx name)))))

(defn list-streams
  "Enumerate Streams through JetStream context `ctx`.

   `ctx` is a context from [[jetstream]]. Returns a platform-native promise
   resolving to a vector of normalized StreamInfo maps (see [[create-stream]]), one
   per Stream. Takes no options. The promise rejects with operational `:type
   :jetstream-api-error` carrying `{:code :description}` when the server-side
   listing fails (ADR 0020)."
  [ctx]
  (proto/-list-streams ctx))

(defn stream-names
  "Enumerate Stream names through JetStream context `ctx`.

   `ctx` is a context from [[jetstream]]. Returns a platform-native promise
   resolving to a vector of Stream name strings. Takes no options. Unlike
   [[consumer-names]], this rides each leg's dedicated names endpoint rather than
   deriving from [[list-streams]], so it never pays for full infos. The promise
   rejects with operational `:type :jetstream-api-error` carrying `{:code
   :description}` when the server-side listing fails (ADR 0020)."
  [ctx]
  (proto/-stream-names ctx))

(defn create-consumer
  "Create a Consumer on the Stream named `stream` through the JetStream context
   `ctx`, from the portable, CLOSED kebab `config` map, returning a platform-native
   promise that resolves to the normalized ConsumerInfo map (`{:stream :name :config
   :created :delivered :ack-floor :pending}`, `:created` an ISO-8601 string,
   `:delivered`/`:ack-floor` each a `{:consumer-seq :stream-seq}` pair). `config`
   keys, all optional except as noted:

   | key                | type                                                  | default | effect                                                                                   |
   |--------------------|-------------------------------------------------------|---------|------------------------------------------------------------------------------------------|
   | `:name`            | string                                                | none    | The consumer name; REQUIRED for a durable (the durable name), optional for an ephemeral. |
   | `:durable?`        | boolean                                               | `true`  | `true` creates a durable (server-persisted) consumer, `false` an ephemeral (ADR 0021).   |
   | `:ack-policy`      | `:none` \\| `:all` \\| `:explicit`                    | server  | When and how deliveries are acknowledged.                                                |
   | `:deliver-policy`  | `:all` \\| `:last` \\| `:new` \\| `:last-per-subject` | server  | Where the consumer starts delivering from.                                               |
   | `:ack-wait-ms`     | integer ms                                            | server  | Redelivery window: how long the server waits for an ack before redelivering.             |
   | `:max-deliver`     | integer                                               | server  | Max delivery attempts per message before it is dropped.                                  |
   | `:filter-subjects` | vector of subject strings                             | server  | Restrict the consumer to these subjects.                                                 |
  
   The map is closed: an unrecognized key rejects the promise with a validation
   `:type :unknown-config-key` (carrying the offending `:keys`); a durable consumer
   with no `:name` rejects with `:type :missing-required-key` (carrying `:key
   :name`); a malformed `:name` or `stream` rejects with `:type :invalid-name` — all
   pre-flight before any native call (ADR 0015). A config the SERVER rejects rejects
   with an operational `:type :jetstream-api-error` carrying `{:code :description}`
   (ADR 0020).

   ```clojure
   @(create-consumer ctx \"ORDERS\" {:name \"workers\" :ack-policy :explicit :max-deliver 5})
   ```"
  [ctx stream config]
  (-> (impl/resolved nil)
    (impl/then (fn [_] (stream/validate-name stream)))
    (impl/then (fn [_] (consumer/validate-config config)))
    (impl/bind (fn [_] (proto/-create-consumer ctx stream config)))))

(defn update-consumer
  "Update an existing Consumer's configuration on the Stream named `stream` through
   the JetStream context `ctx`, from the portable, CLOSED kebab `config` map (same
   keys as [[create-consumer]], `:name` naming the Consumer to change), returning a
   platform-native promise that resolves to the normalized ConsumerInfo map carrying
   the new active config. The keys present are MERGED over the Consumer's current
   config — an absent key keeps its current value rather than reverting to a server
   default — so an ack wait or delivery cap can change without restating the whole
   config (ADR 0020). Updates are deliberate and separate from [[create-consumer]],
   which stays create-only (ADR 0021), mirroring the [[create-stream]]/[[update-stream]]
   split. The map is closed: an unrecognized key rejects pre-flight with a validation
   `:type :unknown-config-key` (carrying the offending `:keys`); a `config` whose
   `:durable?` is absent or `true` with no `:name` rejects with `:type
   :missing-required-key` (carrying `:key :name`); and a malformed `:name` or
   `stream` rejects with `:type :invalid-name` (ADR 0015). The promise rejects with
   an operational `:type :consumer-not-found` when no such Consumer exists, and
   `:jetstream-api-error` carrying `{:code :description}` when the server rejects the
   change (e.g. an immutable field) (ADR 0020)."
  [ctx stream config]
  (-> (impl/resolved nil)
    (impl/then (fn [_] (stream/validate-name stream)))
    (impl/then (fn [_] (consumer/validate-config config)))
    (impl/bind (fn [_] (proto/-update-consumer ctx stream config)))))

(defn consumer-info
  "Look up the Consumer named `name` on the Stream `stream` through the JetStream
   context `ctx`, returning a platform-native promise that resolves to the normalized
   ConsumerInfo map (see [[create-consumer]]). The promise rejects with an operational
   `:type :consumer-not-found` when no such Consumer exists, and pre-flight with a
   validation `:type :invalid-name` when `stream` or `name` is malformed (ADR 0015/0020)."
  [ctx stream name]
  (-> (impl/resolved nil)
    (impl/then (fn [_] (stream/validate-name stream)))
    (impl/then (fn [_] (consumer/validate-name name)))
    (impl/bind (fn [_] (proto/-consumer-info ctx stream name)))))

(defn delete-consumer
  "Delete the Consumer named `name` on the Stream `stream` through the JetStream
   context `ctx`, returning a platform-native promise that resolves to nil once it is
   gone. The promise rejects with an operational `:type :consumer-not-found` when no
   such Consumer exists, and pre-flight with a validation `:type :invalid-name` when
   `stream` or `name` is malformed (ADR 0015/0020)."
  [ctx stream name]
  (-> (impl/resolved nil)
    (impl/then (fn [_] (stream/validate-name stream)))
    (impl/then (fn [_] (consumer/validate-name name)))
    (impl/bind (fn [_] (proto/-delete-consumer ctx stream name)))))

(defn list-consumers
  "Enumerate the Consumers on the Stream `stream` through the JetStream context `ctx`,
   returning a platform-native promise that resolves to a vector of normalized
   ConsumerInfo maps (see [[create-consumer]]), one per Consumer. The promise rejects
   pre-flight with a validation `:type :invalid-name` when `stream` is malformed
   (ADR 0015/0020)."
  [ctx stream]
  (-> (impl/resolved nil)
    (impl/then (fn [_] (stream/validate-name stream)))
    (impl/bind (fn [_] (proto/-list-consumers ctx stream)))))

(defn consumer-names
  "Enumerate the durable names of the Consumers on the Stream `stream` through the
   JetStream context `ctx`, returning a platform-native promise that resolves to a
   vector of name strings — the name projection of [[list-consumers]], from which it is
   derived (nats.js exposes no names endpoint). The promise rejects pre-flight with a
   validation `:type :invalid-name` when `stream` is malformed (ADR 0015/0020)."
  [ctx stream]
  (impl/then (list-consumers ctx stream) (fn [infos] (mapv :name infos))))

(defn publish
  "Acked publish of `data` to `subject` through the JetStream context `ctx`, encoding
   `data` with the context's default codec, returning a platform-native promise that
   resolves to the normalized PubAck map `{:stream :seq :duplicate :domain}` (`:seq`
   the assigned stream sequence, `:duplicate` true when the server deduped the
   message, `:domain` nil when none is configured).

   `opts` (optional map), every key optional:

   | key           | type                | default      | effect |
   |---------------|---------------------|--------------|--------|
   | `:headers`    | map                 | none         | Case-sensitive string names to string or vector-of-string values. |
   | `:msg-id`     | string              | none         | Dedup id; duplicate publish resolves with `:duplicate true`. |
   | `:expect`     | map                 | none         | Optimistic-concurrency assertions `{:last-seq :last-msg-id :stream :last-subject-seq}`. |
   | `:timeout-ms` | integer ms          | native/client default | Ack deadline; an absent PubAck rejects rather than hangs. |
   | `:codec`      | keyword or `ICodec` | ctx's codec  | Per-call codec for encoding `data`; headers are not codec'd. |

   `:msg-id`/`:expect` are the sanctioned way to set reserved `Nats-*` headers, so a
   reserved key set directly in `:headers` rejects pre-flight with a validation `:type
   :reserved-header` (carrying `:keys`), and a non-map `:headers` with `:invalid-header`
   — both before any native call (ADR 0015). Beyond `:wrong-last-sequence`, a non-API
   publish failure (e.g. no responders) rejects with the operational catch-all `:type
   :publish-failed` (ADR 0006/0020). A `:timeout-ms` deadline elapsing diverges by
   platform: the JVM rejects with the core `:type :timeout`, ClojureScript with
   `:type :publish-failed`.

   ```clojure
   @(publish ctx \"orders.new\" {:id 42}
             {:msg-id \"42\" :expect {:last-seq 7} :timeout-ms 2000})
   ```"
  ([ctx subject data] (publish ctx subject data {}))
  ([ctx subject data {:keys [headers] :as opts}]
    (-> (impl/resolved nil)
      (impl/then (fn [_] (pub/validate-headers headers)))
      (impl/then (fn [hs]
                   {:headers (msg/normalize-headers hs)
                    :bytes   (codec/encode (msg/effective-codec ctx opts) data)}))
      (impl/bind (fn [{:keys [headers bytes]}]
                   (proto/-js-publish ctx subject headers bytes
                     (select-keys opts [:msg-id :expect :timeout-ms])))))))

(defn get-message
  "One-shot direct read of a stored message from the Stream `stream` on the
   JetStream context `ctx` — a read from the stream's storage, not a consumer
   delivery — returning a platform-native promise that resolves to ONE pure-data
   stored message `{:subject :data :seq :timestamp}` (plus `:headers` when the
   message carries some): `:data` decoded with the context codec, `:seq` the
   message's stream sequence, `:timestamp` the canonical ISO-8601 receive time.
   There is no `:js` consumer metadata and no ack-subject — nothing was delivered,
   so there is nothing to ack.

   `query` is the portable, CLOSED kebab map selecting the message by EXACTLY one of:

   | key                | type         | default | effect |
   |--------------------|--------------|---------|--------|
   | `:seq`             | positive int | none    | Select this stream sequence. |
   | `:last-by-subject` | string       | none    | Select the newest stored message for this subject. |

   `opts` (optional map): `:codec` is a keyword or `ICodec`, default ctx's codec,
   used to decode the stored payload.

   The promise rejects with an operational `:type :no-message-found` (err 10037,
   carrying `{:code :description}`) when nothing matches — a sequence the stream
   never reached or has purged, or a subject with no stored message — and
   `:stream-not-found` when no such Stream exists (ADR 0020). Pre-flight, a
   malformed `stream` rejects with a validation `:type :invalid-name`, an
   unrecognized query key with `:unknown-config-key`, and a query that does not
   select by exactly one well-formed selector with `:invalid-query` (ADR 0015)."
  ([ctx stream query] (get-message ctx stream query {}))
  ([ctx stream query opts]
    (-> (impl/resolved nil)
      (impl/then (fn [_] (stream/validate-name stream)))
      (impl/then (fn [_] (stream/validate-get-query query)))
      (impl/bind (fn [_] (proto/-js-get-message ctx stream query)))
      (impl/then (fn [{:keys [subject bytes headers] :as raw}]
                   (cond-> {:subject   subject
                            :data      (codec/decode (msg/effective-codec ctx opts) bytes)
                            :seq       (:seq raw)
                            :timestamp (:timestamp raw)}
                     (seq headers) (assoc :headers (msg/trim-headers headers))))))))

(defn- decode-js-msg
  "Decode a raw pull map `{:subject :bytes :headers :js}` (the per-leg `js-msg->raw`
   lift) into the public pure-data JetStream message `{:subject :data :headers :js}`,
   decoding `:bytes` with `codec` and carrying the `:js` metadata through untouched.
   The JetStream counterpart to core's `decode-msg`: same header contract via the
   shared `msg/trim-headers` (surrounding whitespace stripped, an empty map dropped
   so `:headers` stays absent), but the ack address lives under `:js :ack-subject`,
   never as a top-level `:reply` (ADR 0019)."
  [codec {:keys [subject bytes headers js]}]
  (cond-> {:subject subject
           :data    (codec/decode codec bytes)
           :js      js}
    (seq headers) (assoc :headers (msg/trim-headers headers))))

(defn ordered-consumer
  "Create an Ordered consumer over the Stream `stream` through the JetStream context
   `ctx` — a specialized Ephemeral consumer for single-client, gap-free replay:
   server-managed, taking NO acknowledgements (ack policy none), and automatically
   recreated by the client should a sequence gap appear — returning a platform-native
   promise that resolves to an ordered pull handle. The handle plugs into the same
   pull triad as a named Consumer, via the handle-first arities: `(next handle opts)`,
   `(fetch handle opts)`, `(consume handle handler opts)` — see [[next]], [[fetch]],
   [[consume]] — same pure-data messages, same opts, same drainable consume handle
   (ADR 0018/0019). It leaves no durable
   Consumer behind: the underlying ephemeral is reclaimed by the server after its
   inactivity window.

   `opts` is the portable, CLOSED kebab map tuning what the handle replays:

   | key                | type                                      | default | effect |
   |--------------------|-------------------------------------------|---------|--------|
   | `:filter-subjects` | vector of subject strings                 | all     | Restrict replay to these subjects. |
   | `:deliver-policy`  | `:all`/`:last`/`:new`/`:last-per-subject` | server  | Where replay starts. |

   Everything else — name, ack policy, recreation — is the library's business. The
   promise rejects pre-flight with a validation `:type :invalid-name` when `stream`
   is malformed and `:unknown-config-key` for an unrecognized opts key (ADR 0015),
   and operationally with `:stream-not-found` when no such Stream exists — both legs
   round-trip stream info at creation (ADR 0020)."
  ([ctx stream] (ordered-consumer ctx stream {}))
  ([ctx stream opts]
    (-> (impl/resolved nil)
      (impl/then (fn [_] (stream/validate-name stream)))
      (impl/then (fn [_] (consumer/validate-ordered-config opts)))
      (impl/bind (fn [_] (proto/-js-ordered-consumer ctx stream opts))))))

(defn fetch
  "Fetch a bounded batch from the durable Consumer `consumer` on Stream `stream`
   through the JetStream context `ctx`.

   Returns a platform-native promise that resolves to a vector of up to `:batch`
   PURE-DATA messages `{:subject :data :js}` (plus `:headers` when the message carried
   some), each `:data` decoded with the context codec, in stream order (ADR 0018). A
   delivered message is plain data — no native object — with its JetStream metadata
   under `:js` `{:stream :consumer :stream-seq :delivery-seq :delivered :pending
   :redelivered :timestamp :domain :ack-subject}`, `:timestamp` an ISO-8601 string and
   `:redelivered` true once `:delivered` exceeds 1 (ADR 0019).

   `opts` (optional map):

   | key           | type                | default | effect |
   |---------------|---------------------|---------|--------|
   | `:batch`      | positive integer    | `100`   | Maximum messages to fetch. |
   | `:expires-ms` | integer ms          | native/client default | Window after which a short batch settles with what it has. |
   | `:codec`      | keyword or `ICodec` | ctx or handle codec | Per-call decode override. |

   The promise rejects pre-flight with a validation `:type :invalid-name` when `stream`
   or `consumer` is malformed, and `:invalid-expires` when a supplied `:expires-ms` is
   below the 1000ms floor both clients enforce or is not a whole number (ADR 0015).

   The handle-first arities fetch from an [[ordered-consumer]] handle instead — same
   message shape, opts, and `:invalid-expires` guard; the handle already names its
   Stream and the library manages the ephemeral, so there is nothing to validate
   by name."
  ([ordered] (fetch ordered {}))
  ([ordered opts]
    (-> (impl/resolved nil)
      (impl/then (fn [_] (pull/validate-expires opts)))
      (impl/bind (fn [_] (proto/-oc-fetch ordered opts)))
      (impl/then (fn [raws]
                   (let [codec (msg/effective-codec ordered opts)]
                     (mapv #(decode-js-msg codec %) raws))))))
  ([ctx stream consumer] (fetch ctx stream consumer {}))
  ([ctx stream consumer opts]
    (-> (impl/resolved nil)
      (impl/then (fn [_] (stream/validate-name stream)))
      (impl/then (fn [_] (consumer/validate-name consumer)))
      (impl/then (fn [_] (pull/validate-expires opts)))
      (impl/bind (fn [_] (proto/-js-fetch ctx stream consumer opts)))
      (impl/then (fn [raws]
                   (let [codec (msg/effective-codec ctx opts)]
                     (mapv #(decode-js-msg codec %) raws)))))))

(defn next
  "Poll a single message from the durable Consumer `consumer` on Stream `stream`
   through the JetStream context `ctx`.

   Returns a platform-native promise that resolves to ONE PURE-DATA message
   `{:subject :data :js}` (plus `:headers` when present, shape as in [[fetch]]), or nil
   when no message arrives within the poll window — an empty consumer (ADR 0018).

   `opts` (optional map):

   | key           | type                | default | effect |
   |---------------|---------------------|---------|--------|
   | `:expires-ms` | integer ms          | native/client default | How long to wait for a message before resolving nil. |
   | `:codec`      | keyword or `ICodec` | ctx or handle codec | Per-call decode override. |

   The promise rejects pre-flight with a validation `:type :invalid-name` when `stream`
   or `consumer` is malformed, and `:invalid-expires` when a supplied `:expires-ms` is
   below the 1000ms floor both clients enforce or is not a whole number (ADR 0015).

   The handle-first arities poll an [[ordered-consumer]] handle instead — same message
   shape, opts, and `:invalid-expires` guard; successive polls continue from the
   handle's tracked position, in stream order.

   Named `next`, this var excludes (does not shadow in-ns) `clojure.core/next` in
   this namespace: call the core sequence fn fully qualified as `clojure.core/next`
   here, while `jet/next` is this poll-one verb at the alias (ADR 0018)."
  ([ordered] (next ordered {}))
  ([ordered opts]
    (-> (impl/resolved nil)
      (impl/then (fn [_] (pull/validate-expires opts)))
      (impl/bind (fn [_] (proto/-oc-next ordered opts)))
      (impl/then (fn [raw] (when raw (decode-js-msg (msg/effective-codec ordered opts) raw))))))
  ([ctx stream consumer] (next ctx stream consumer {}))
  ([ctx stream consumer opts]
    (-> (impl/resolved nil)
      (impl/then (fn [_] (stream/validate-name stream)))
      (impl/then (fn [_] (consumer/validate-name consumer)))
      (impl/then (fn [_] (pull/validate-expires opts)))
      (impl/bind (fn [_] (proto/-js-next ctx stream consumer opts)))
      (impl/then (fn [raw] (when raw (decode-js-msg (msg/effective-codec ctx opts) raw)))))))

(defn consume
  "Continuously deliver from the durable Consumer `consumer` on Stream `stream`
   through the JetStream context `ctx`, invoking `handler` with one PURE-DATA
   message `{:subject :data :js}` at a time (plus `:headers` when present, shape as
   in [[fetch]]), and returning a platform-native promise that resolves to a handle —
   drainable and unsubscribable exactly like a core Subscription, via
   [[nats-cljc.core/drain]] and [[nats-cljc.core/unsubscribe]] (ADR 0018).

   `handler` is the core handler contract (ADR 0007): it may return a promise, and
   the runtime waits for that promise to settle before delivering the next message
   AND refilling — per-message backpressure with no async dependency, the client's
   read rate gating its own pull rate. There is NO `:max-pending`/`:slow-consumer`
   in pull: unrequested messages simply wait on the server (ADR 0018).

   `opts` are the refill knobs (ADR 0018):

   | key                  | type                | default        | effect |
   |----------------------|---------------------|----------------|--------|
   | `:batch`             | positive integer    | `100`          | Max messages per pull window. |
   | `:threshold`         | positive integer    | 75% of `:batch`| Refill once buffered count drops to it. |
   | `:expires-ms`        | integer ms          | native/client default | Pull window and drain wind-down bound. |
   | `:idle-heartbeat-ms` | integer ms          | none           | Server liveness pulses while idle; cadence may differ on JVM. |
   | `:max-bytes`         | integer             | none           | Byte window per pull, mutually exclusive with `:batch`/`:threshold`. |
   | `:codec`             | keyword or `ICodec` | ctx or handle codec | Per-call decode override. |
   | `:on-error`          | 1-arg fn            | none           | Named consumes only: per-consume error sink. |

   The promise rejects pre-flight with a validation `:type :invalid-name` when
   `stream` or `consumer` is malformed, `:invalid-batch` when `:batch` is present but
   not a positive integer, `:invalid-threshold` when `:threshold` is not a positive
   integer no greater than `:batch`, `:exclusive-window` when `:max-bytes` is combined
   with `:batch`/`:threshold`, `:invalid-expires` when a supplied `:expires-ms` is
   below the 1000ms floor or not a whole number (ADR 0015) — and operationally with
   `:consumer-not-found` when no such Consumer exists (ADR 0020).

   `:on-error` (named consumes) is the per-consume error sink (ADR 0020): the
   consume-time side-band conditions — `:heartbeats-missed`, `:consumer-deleted`,
   `:exceeded-limits`, with a backing-stream loss reusing `:stream-not-found` and
   (CLJS) a vanished Consumer surfacing as `:consumer-not-found` — reach it as bare
   ex-infos, exactly like core's per-subscription `:slow-consumer` row: this sink
   ONLY, dropped when unset, never the connection `:on-status`. A handler throw or
   decode failure reaches the same sink, and delivery continues. Terminal conditions
   (the Consumer or its backing Stream is gone — `:consumer-deleted`,
   `:stream-not-found`, and on CLJS `:consumer-not-found`) additionally END the
   consume — the handle goes inactive — whether or not `:on-error` is set. On the
   JVM a gone Consumer surfaces as the 409 `:consumer-deleted`; CLJS (nats.js) can
   instead emit `:consumer-not-found`.

   On the handle, [[nats-cljc.core/drain]] stops new pulls and settles once the
   consume winds down (on the JVM buffered messages deliver first; on CLJS the buffer
   is discarded — un-acked, so the server redelivers them); [[nats-cljc.core/unsubscribe]]
   ends it abruptly and idempotently, and takes no `max` (a consume has no
   auto-unsubscribe count — passing one throws `:type :invalid-max`).

   The handle-first arities consume from an [[ordered-consumer]] handle instead — same
   handler contract, refill knobs, and drainable handle; deliveries arrive in stream
   order with no acknowledgements to take."
  ([ordered handler] (consume ordered handler {}))
  ([ordered handler opts]
    (-> (impl/resolved nil)
      (impl/then (fn [_] (refill/validate-opts opts)))
      (impl/bind (fn [_]
                   (let [codec (msg/effective-codec ordered opts)]
                     (proto/-oc-consume ordered
                       (select-keys opts [:batch :threshold :expires-ms :idle-heartbeat-ms :max-bytes])
                       (fn [raw] (handler (decode-js-msg codec raw)))))))))
  ([ctx stream consumer handler] (consume ctx stream consumer handler {}))
  ([ctx stream consumer handler opts]
    (-> (impl/resolved nil)
      (impl/then (fn [_] (stream/validate-name stream)))
      (impl/then (fn [_] (consumer/validate-name consumer)))
      (impl/then (fn [_] (refill/validate-opts opts)))
      (impl/bind (fn [_]
                   (let [codec (msg/effective-codec ctx opts)]
                     (proto/-js-consume ctx stream consumer
                       (select-keys opts [:batch :threshold :expires-ms :idle-heartbeat-ms :max-bytes :on-error])
                       (fn [raw] (handler (decode-js-msg codec raw))))))))))

(defn- ack-publish!
  "The one ack code path (ADR 0019): publish the `verb` protocol payload to `msg`'s
   ack subject on `conn`, fire-and-forget — `reply`'s shape, byte-identical on both
   legs. Synchronous, returns nil."
  [conn msg verb opts]
  (proto/-publish conn (acks/ack-subject msg) nil (acks/payload verb opts))
  nil)

(defn ack
  "Acknowledge delivered JetStream message `msg` as processed on `conn`.

   `conn` is a Connection. `msg` is a delivered JetStream message carrying `:js
   :ack-subject`. Returns nil synchronously. Takes no options. A redundant ack of
   an already-acked message is a harmless publish the server ignores.

   Throws synchronously with `:type :no-ack-subject` when `msg` carries no ack
   subject. Other publish failures can throw the same synchronous failures as
   `nats-cljc.core/publish`, including `:connection-closed`."
  [conn msg]
  (ack-publish! conn msg :ack nil))

(defn nak
  "Negatively acknowledge delivered JetStream message `msg` on `conn`.

   Signals that the message was not processed and asks the server to redeliver it.
   Returns nil synchronously.

   `opts` (optional map):

   | key         | type       | default | effect |
   |-------------|------------|---------|--------|
   | `:delay-ms` | integer ms | none    | Ask the server to hold redelivery for this long. |

   Throws synchronously with `:type :no-ack-subject` when `msg` carries no ack
   subject. Other publish failures can throw the same synchronous failures as
   `nats-cljc.core/publish`, including `:connection-closed`."
  ([conn msg] (nak conn msg {}))
  ([conn msg opts]
    (ack-publish! conn msg :nak opts)))

(defn term
  "Terminate delivered JetStream message `msg` on `conn`.

   Gives up on the message so the server never redelivers it. `conn` is a
   Connection; `msg` must carry `:js :ack-subject`. Returns nil synchronously and
   takes no options.

   Throws synchronously with `:type :no-ack-subject` when `msg` carries no ack
   subject. Other publish failures can throw the same synchronous failures as
   `nats-cljc.core/publish`, including `:connection-closed`."
  [conn msg]
  (ack-publish! conn msg :term nil))

(defn working
  "Signal that delivered JetStream message `msg` is still being processed.

   `conn` is a Connection; `msg` must carry `:js :ack-subject`. The signal
   postpones the consumer's ack-wait timer. Returns nil synchronously and takes no
   options. It is repeatable; send it again whenever the deadline nears.

   Throws synchronously with `:type :no-ack-subject` when `msg` carries no ack
   subject. Other publish failures can throw the same synchronous failures as
   `nats-cljc.core/publish`, including `:connection-closed`."
  [conn msg]
  (ack-publish! conn msg :working nil))

(defn double-ack
  "Acknowledge the delivered JetStream message `msg` on `conn` AND await the
   server's confirmation, returning a platform-native promise that resolves to
   true once the ack is known processed — where [[ack]] is fire-and-forget. Sugar
   over [[nats-cljc.core/request]] of the `+ACK` protocol payload to the message's ack subject; the
   server's (empty) reply is the confirmation (ADR 0019). Named double-ack (the
   NATS community term), not jnats' ack-sync — ours is asynchronous. Idempotent:
   the server confirms a redundant ack too, so double-acking the same message
   again resolves true, never throws.

   `opts` (optional map):

   | key           | type       | default | effect |
   |---------------|------------|---------|--------|
   | `:timeout-ms` | integer ms | `5000`  | How long to wait for server confirmation. |

   A confirmation that never arrives rejects with core `:type :timeout`; a message
   without an ack subject rejects with `:type :no-ack-subject`; closed/draining
   connection and request failures reject with the same canonical types as
   `nats-cljc.core/request` (ADR 0006)."
  ([conn msg] (double-ack conn msg {}))
  ([conn msg opts]
    (-> (impl/resolved nil)
      (impl/then (fn [_] (acks/ack-subject msg)))
      (impl/bind (fn [subject] (proto/-request conn subject (acks/payload :ack nil) (:timeout-ms opts 5000))))
      (impl/then (fn [_] true)))))
