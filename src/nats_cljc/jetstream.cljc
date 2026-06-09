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
  (:require [nats-cljc.core :as core]
            [nats-cljc.codec :as codec]
            [nats-cljc.protocol :as proto]
            [nats-cljc.jetstream.stream :as stream]
            [nats-cljc.jetstream.consumer :as consumer]
            [nats-cljc.jetstream.pub :as pub]
            [nats-cljc.jetstream.pull :as pull]
            [nats-cljc.jetstream.refill :as refill]
            [nats-cljc.jetstream.acks :as acks]
            #?(:clj  [nats-cljc.impl.jvm :as impl]
               :cljs [nats-cljc.impl.js :as impl])
            #?(:clj  [nats-cljc.jetstream.impl.jvm]
               :cljs [nats-cljc.jetstream.impl.js])))

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
   `config` map, returning a platform-native promise that resolves to the
   normalized StreamInfo map (`{:config :created :state}`, `:created` an ISO-8601
   string). Config keys: `:name` (required), `:subjects`, `:storage` (`:file` |
   `:memory`), `:retention` (`:limits` | `:interest` | `:work-queue`), and
   `:max-age-ms` (integer milliseconds). The map is closed: an unrecognized key
   rejects the promise with a validation `:type :unknown-config-key`, and a
   malformed `:name` with `:invalid-name`, both pre-flight before any native call
   (ADR 0015). A config the SERVER rejects (e.g. a subject overlap) rejects with an
   operational `:type :jetstream-api-error` carrying `{:code :description}` — it is
   detected after the native call, so it is operational, not validation (ADR 0020)."
  [ctx config]
  (-> (impl/resolved nil)
      (impl/then (fn [_] (stream/validate-config config)))
      (impl/bind (fn [_] (proto/-create-stream ctx config)))))

(defn stream-info
  "Look up the Stream named `name` on the JetStream context `ctx`, returning a
   platform-native promise that resolves to the normalized StreamInfo map (see
   `create-stream`). The promise rejects with an operational `:type
   :stream-not-found` when no such Stream exists, and pre-flight with a validation
   `:type :invalid-name` when `name` is malformed (ADR 0015/0020)."
  [ctx name]
  (-> (impl/resolved nil)
      (impl/then (fn [_] (stream/validate-name name)))
      (impl/bind (fn [_] (proto/-stream-info ctx name)))))

(defn update-stream
  "Update an existing Stream's configuration on the JetStream context `ctx` from the
   portable, CLOSED kebab `config` map (same keys as `create-stream`, `:name`
   naming the Stream to change), returning a platform-native promise that resolves
   to the normalized StreamInfo map carrying the new active config. The keys
   present are MERGED over the Stream's current config — an absent key keeps its
   current value rather than reverting to a server default — so a retention or
   limit can change without restating the whole config (ADR 0020). The map is
   closed: an unrecognized key rejects pre-flight with a validation `:type
   :unknown-config-key`, and a malformed `:name` with `:invalid-name` (ADR 0015).
   The promise rejects with an operational `:type :stream-not-found` when no such
   Stream exists, and `:jetstream-api-error` carrying `{:code :description}` when
   the server rejects the change (e.g. an immutable field) (ADR 0020)."
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
  "Enumerate the Streams on the server through the JetStream context `ctx`,
   returning a platform-native promise that resolves to a vector of normalized
   StreamInfo maps (see `create-stream`), one per Stream."
  [ctx]
  (proto/-list-streams ctx))

(defn stream-names
  "Enumerate the Stream names on the server through the JetStream context `ctx`,
   returning a platform-native promise that resolves to a vector of name strings.
   Unlike `consumer-names`, this rides each leg's dedicated names endpoint rather
   than deriving from `list-streams`, so it never pays for full infos."
  [ctx]
  (proto/-stream-names ctx))

(defn create-consumer
  "Create a durable Consumer on the Stream named `stream` through the JetStream context
   `ctx`, from the portable, CLOSED kebab `config` map, returning a platform-native
   promise that resolves to the normalized ConsumerInfo map (`{:stream :name :config
   :created :delivered :ack-floor :pending}`, `:created` an ISO-8601 string). Config
   keys: `:name` (required — the durable), `:ack-policy` (`:none` | `:all` | `:explicit`),
   `:deliver-policy` (`:all` | `:last` | `:new` | `:last-per-subject`), `:ack-wait-ms`
   (integer milliseconds), `:max-deliver` (integer), and `:filter-subjects` (a vector of
   subject strings). The map is closed: an unrecognized key rejects the promise with a
   validation `:type :unknown-config-key`, and a malformed `:name` or `stream` with
   `:invalid-name`, both pre-flight before any native call (ADR 0015). A config the
   SERVER rejects rejects with an operational `:type :jetstream-api-error` carrying
   `{:code :description}` (ADR 0020)."
  [ctx stream config]
  (-> (impl/resolved nil)
      (impl/then (fn [_] (stream/validate-name stream)))
      (impl/then (fn [_] (consumer/validate-config config)))
      (impl/bind (fn [_] (proto/-create-consumer ctx stream config)))))

(defn update-consumer
  "Update an existing durable Consumer's configuration on the Stream named `stream`
   through the JetStream context `ctx`, from the portable, CLOSED kebab `config` map
   (same keys as `create-consumer`, `:name` naming the durable to change), returning
   a platform-native promise that resolves to the normalized ConsumerInfo map
   carrying the new active config. The keys present are MERGED over the Consumer's
   current config — an absent key keeps its current value rather than reverting to a
   server default — so an ack wait or delivery cap can change without restating the
   whole config (ADR 0020). Updates are deliberate and separate from `create-consumer`,
   which stays create-only (ADR 0021), mirroring the `create-stream`/`update-stream`
   split. The map is closed: an unrecognized key rejects pre-flight with a validation
   `:type :unknown-config-key`, and a malformed `:name` or `stream` with
   `:invalid-name` (ADR 0015). The promise rejects with an operational `:type
   :consumer-not-found` when no such Consumer exists, and `:jetstream-api-error`
   carrying `{:code :description}` when the server rejects the change (e.g. an
   immutable field) (ADR 0020)."
  [ctx stream config]
  (-> (impl/resolved nil)
      (impl/then (fn [_] (stream/validate-name stream)))
      (impl/then (fn [_] (consumer/validate-config config)))
      (impl/bind (fn [_] (proto/-update-consumer ctx stream config)))))

(defn consumer-info
  "Look up the Consumer named `name` on the Stream `stream` through the JetStream
   context `ctx`, returning a platform-native promise that resolves to the normalized
   ConsumerInfo map (see `create-consumer`). The promise rejects with an operational
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
   ConsumerInfo maps (see `create-consumer`), one per durable. The promise rejects
   pre-flight with a validation `:type :invalid-name` when `stream` is malformed
   (ADR 0015/0020)."
  [ctx stream]
  (-> (impl/resolved nil)
      (impl/then (fn [_] (stream/validate-name stream)))
      (impl/bind (fn [_] (proto/-list-consumers ctx stream)))))

(defn consumer-names
  "Enumerate the durable names of the Consumers on the Stream `stream` through the
   JetStream context `ctx`, returning a platform-native promise that resolves to a
   vector of name strings — the name projection of `list-consumers`, from which it is
   derived (nats.js exposes no names endpoint)."
  [ctx stream]
  (impl/then (list-consumers ctx stream) (fn [infos] (mapv :name infos))))

(defn publish
  "Acked publish of `data` to `subject` through the JetStream context `ctx`, encoding
   `data` with the context's default codec, returning a platform-native promise that
   resolves to the normalized PubAck map `{:stream :seq :duplicate :domain}`. `opts`:
   `:headers` (a map of case-sensitive string names to one or more string values, a
   scalar normalized to a one-element vector); `:msg-id` (server-side dedup within the
   stream's dedup window — the PubAck `:duplicate` is true on a retry); `:expect`
   (`{:last-seq :last-msg-id :stream :last-subject-seq}`, optimistic-concurrency
   assertions whose mismatch rejects with an operational `:type :wrong-last-sequence`);
   `:timeout-ms` (a missing PubAck rejects rather than hangs); and `:codec` (a per-call
   codec override — only `:data` is codec'd). `:msg-id`/`:expect` are the sanctioned
   way to set reserved `Nats-*` headers, so a reserved key set directly in `:headers`
   rejects pre-flight with a validation `:type :reserved-header`, and a malformed
   header with `:invalid-header` — both before any native call (ADR 0015/0020)."
  ([ctx subject data] (publish ctx subject data {}))
  ([ctx subject data {:keys [headers] :as opts}]
   (-> (impl/resolved nil)
       (impl/then (fn [_] (pub/validate-headers headers)))
       (impl/then (fn [hs]
                    {:headers (core/normalize-headers hs)
                     :bytes   (codec/encode (core/effective-codec ctx opts) data)}))
       (impl/bind (fn [{:keys [headers bytes]}]
                    (proto/-js-publish ctx subject headers bytes
                                       (select-keys opts [:msg-id :expect :timeout-ms])))))))

(defn- decode-js-msg
  "Decode a raw pull map `{:subject :bytes :headers :js}` (the per-leg `js-msg->raw`
   lift) into the public pure-data JetStream message `{:subject :data :headers :js}`,
   decoding `:bytes` with `codec` and carrying the `:js` metadata through untouched.
   The JetStream counterpart to core's `decode-msg`: same header contract via the
   shared `core/trim-headers` (surrounding whitespace stripped, an empty map dropped
   so `:headers` stays absent), but the ack address lives under `:js :ack-subject`,
   never as a top-level `:reply` (ADR 0019)."
  [codec {:keys [subject bytes headers js]}]
  (cond-> {:subject subject
           :data    (codec/decode codec bytes)
           :js      js}
    (seq headers) (assoc :headers (core/trim-headers headers))))

(defn fetch
  "Fetch a bounded batch from the durable Consumer `consumer` on Stream `stream` through
   the JetStream context `ctx`, returning a platform-native promise that resolves to a
   vector of up to `:batch` PURE-DATA messages `{:subject :data :js}` (plus `:headers`
   when the message carried some), each `:data` decoded with the context codec, in stream
   order (ADR 0018). A delivered message is plain data — no native object — with its
   JetStream metadata under `:js` `{:stream :consumer :stream-seq :delivery-seq :delivered
   :pending :redelivered :timestamp :domain :ack-subject}`, `:timestamp` an ISO-8601 string
   and `:redelivered` true once `:delivered` exceeds 1 (ADR 0019). `opts`: `:batch` (max
   messages, default 100), `:expires-ms` (the window after which a batch shorter than
   `:batch` settles with what it has; omitted, the window is the 30000ms default both
   clients independently apply — jnats' FetchConsumeOptions and nats.js' fetch — so the
   legs agree on the wait when a caller omits it), and `:codec` (a per-call decode
   override). The
   promise rejects pre-flight with a validation `:type :invalid-name` when `stream` or
   `consumer` is malformed, and `:invalid-expires` when a supplied `:expires-ms` is below
   the 1000ms floor both clients enforce or is not a whole number (ADR 0015)."
  ([ctx stream consumer] (fetch ctx stream consumer {}))
  ([ctx stream consumer opts]
   (-> (impl/resolved nil)
       (impl/then (fn [_] (stream/validate-name stream)))
       (impl/then (fn [_] (consumer/validate-name consumer)))
       (impl/then (fn [_] (pull/validate-expires opts)))
       (impl/bind (fn [_] (proto/-js-fetch ctx stream consumer opts)))
       (impl/then (fn [raws]
                    (let [codec (core/effective-codec ctx opts)]
                      (mapv #(decode-js-msg codec %) raws)))))))

(defn next
  "Poll a single message from the durable Consumer `consumer` on Stream `stream` through
   the JetStream context `ctx`, returning a platform-native promise that resolves to ONE
   PURE-DATA message `{:subject :data :js}` (plus `:headers` when present, shape as in
   `fetch`), or nil when no message arrives within the poll window — an empty consumer
   (ADR 0018). `opts`: `:expires-ms` (how long to wait for a message before resolving nil;
   omitted, it waits the 30000ms default both clients independently apply — jnats'
   no-arg next() and nats.js' next — so the legs agree on the wait) and `:codec`
   (a per-call decode override). The promise
   rejects pre-flight with a validation `:type :invalid-name` when `stream` or `consumer`
   is malformed, and `:invalid-expires` when a supplied `:expires-ms` is below the 1000ms
   floor both clients enforce or is not a whole number (ADR 0015)."
  ([ctx stream consumer] (next ctx stream consumer {}))
  ([ctx stream consumer opts]
   (-> (impl/resolved nil)
       (impl/then (fn [_] (stream/validate-name stream)))
       (impl/then (fn [_] (consumer/validate-name consumer)))
       (impl/then (fn [_] (pull/validate-expires opts)))
       (impl/bind (fn [_] (proto/-js-next ctx stream consumer opts)))
       (impl/then (fn [raw] (when raw (decode-js-msg (core/effective-codec ctx opts) raw)))))))

(defn consume
  "Continuously deliver from the durable Consumer `consumer` on Stream `stream`
   through the JetStream context `ctx`, invoking `handler` with one PURE-DATA
   message `{:subject :data :js}` at a time (plus `:headers` when present, shape as
   in `fetch`), and returning a platform-native promise that resolves to a handle —
   drainable and unsubscribable exactly like a core Subscription, via `core/drain`
   and `core/unsubscribe` (ADR 0018).

   `handler` is the core handler contract (ADR 0007): it may return a promise, and
   the runtime waits for that promise to settle before delivering the next message
   AND refilling — per-message backpressure with no async dependency, the client's
   read rate gating its own pull rate. There is NO `:max-pending`/`:slow-consumer`
   in pull: unrequested messages simply wait on the server (ADR 0018).

   `opts` are the refill knobs (ADR 0018): `:batch` (max messages per pull window,
   default 100), `:threshold` (refill once the buffered count drops to it; a COUNT
   portably — the JVM converts count->percent; each client's default is 75% of
   `:batch`), `:expires-ms` (the pull window — also how long a `drain` may take to
   wind down the open pull), `:idle-heartbeat-ms` (server liveness pulses while
   idle; accepted on both legs, but the JVM client derives its own cadence from
   `:expires-ms` — shape, not cadence, ADR 0006), `:max-bytes` (a BYTE window per
   pull, mutually exclusive with the message-count window — nats.js forbids setting
   both, so `:max-bytes` with `:batch` or `:threshold` is rejected), and `:codec` (a
   per-call decode override). The promise rejects pre-flight with a validation
   `:type :invalid-name` when `stream` or `consumer` is malformed,
   `:invalid-threshold` when `:threshold` is not a positive integer no greater than
   `:batch`, `:exclusive-window` when `:max-bytes` is combined with `:batch`/
   `:threshold`, `:invalid-expires` when a supplied `:expires-ms` is below the
   1000ms floor or not a whole number (ADR 0015) — and operationally with
   `:consumer-not-found` when no such Consumer exists (ADR 0020).

   On the handle, `core/drain` stops new pulls and settles once the consume winds
   down (on the JVM buffered messages deliver first; on CLJS the buffer is
   discarded — un-acked, so the server redelivers them); `core/unsubscribe` ends it
   abruptly and idempotently, and takes no `max` (a consume has no auto-unsubscribe
   count — passing one throws `:type :invalid-max`)."
  ([ctx stream consumer handler] (consume ctx stream consumer handler {}))
  ([ctx stream consumer handler opts]
   (-> (impl/resolved nil)
       (impl/then (fn [_] (stream/validate-name stream)))
       (impl/then (fn [_] (consumer/validate-name consumer)))
       (impl/then (fn [_] (refill/validate-opts opts)))
       (impl/bind (fn [_]
                    (let [codec (core/effective-codec ctx opts)]
                      (proto/-js-consume ctx stream consumer
                                         (select-keys opts [:batch :threshold :expires-ms :idle-heartbeat-ms :max-bytes])
                                         (fn [raw] (handler (decode-js-msg codec raw))))))))))

(defn- ack-publish!
  "The one ack code path (ADR 0019): publish the `verb` protocol payload to `msg`'s
   ack subject on `conn`, fire-and-forget — `reply`'s shape, byte-identical on both
   legs. Synchronous, returns nil."
  [conn msg verb opts]
  (proto/-publish conn (acks/ack-subject msg) nil (acks/payload verb opts))
  nil)

(defn ack
  "Acknowledge the delivered JetStream message `msg` as processed, on `conn` (the
   connection, explicit as in core `reply`), stopping its redelivery. Sugar over
   publish of the `+ACK` protocol payload to the message's ack subject (under `:js
   :ack-subject`) — never a native `.ack()`, so the wire bytes are identical on
   both legs (ADR 0019). Synchronous, returns nil, and idempotent: a redundant ack
   of an already-acked message is a harmless publish the server ignores, never a
   throw. Throws an ex-info `:type :no-ack-subject` when `msg` carries no ack
   subject (it is not a delivered JetStream message), rather than publishing to a
   nil subject — the `reply` `:no-reply-subject` precedent."
  [conn msg]
  (ack-publish! conn msg :ack nil))

(defn nak
  "Negatively acknowledge the delivered JetStream message `msg` on `conn`: signal
   it was NOT processed, asking the server to redeliver it (immediately, or after
   `:delay-ms` milliseconds when set in `opts`). Sugar over publish of the `-NAK`
   protocol payload — `-NAK {\"delay\":ns}` with a delay — to the message's ack
   subject (ADR 0019), shaped exactly as `ack`: synchronous, returns nil,
   idempotent, and throws `:type :no-ack-subject` on a message without an ack
   subject."
  ([conn msg] (nak conn msg {}))
  ([conn msg opts]
   (ack-publish! conn msg :nak opts)))

(defn term
  "Terminate the delivered JetStream message `msg` on `conn`: give up on it, so the
   server never redelivers it — `ack`'s terminal sibling for a message that was NOT
   processed and never will be. Sugar over publish of the `+TERM` protocol payload
   to the message's ack subject (ADR 0019), shaped exactly as `ack`: synchronous,
   returns nil, idempotent, and throws `:type :no-ack-subject` on a message without
   an ack subject."
  [conn msg]
  (ack-publish! conn msg :term nil))

(defn working
  "Signal the delivered JetStream message `msg` is still being processed, on
   `conn`, postponing the consumer's ack-wait timer so the server holds off
   redelivering while work continues. Sugar over publish of the `+WPI` protocol
   payload to the message's ack subject (ADR 0019), shaped exactly as `ack`:
   synchronous, returns nil, and throws `:type :no-ack-subject` on a message
   without an ack subject. Unlike its terminal siblings it is a REPEATABLE
   progress signal — send it again whenever the deadline nears."
  [conn msg]
  (ack-publish! conn msg :working nil))

(defn double-ack
  "Acknowledge the delivered JetStream message `msg` on `conn` AND await the
   server's confirmation, returning a platform-native promise that resolves to
   true once the ack is known processed — where `ack` is fire-and-forget. Sugar
   over `request` of the `+ACK` protocol payload to the message's ack subject; the
   server's (empty) reply is the confirmation (ADR 0019). Named double-ack (the
   NATS community term), not jnats' ack-sync — ours is asynchronous. Idempotent:
   the server confirms a redundant ack too, so double-acking the same message
   again resolves true, never throws. `opts` may set `:timeout-ms` (default 5000);
   a confirmation that never arrives rejects with the core `:timeout`, and a
   message without an ack subject rejects with `:type :no-ack-subject` — on the
   returned promise, never a synchronous throw (ADR 0006)."
  ([conn msg] (double-ack conn msg {}))
  ([conn msg opts]
   (-> (impl/resolved nil)
       (impl/then (fn [_] (acks/ack-subject msg)))
       (impl/bind (fn [subject] (proto/-request conn subject (acks/payload :ack nil) (:timeout-ms opts 5000))))
       (impl/then (fn [_] true)))))
