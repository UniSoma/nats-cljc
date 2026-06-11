(ns ^:no-doc nats-cljc.impl.protocol
  "Internal protocols (ADR 0005) the platform records implement; the public facade
   in `nats-cljc.core` sits on top and owns codec encode/decode, so everything
   here deals in raw wire bytes.

   - `Conn` — the primitive operations a platform Connection implements.
   - `Drainable` — draining, supported by a connection AND a single subscription,
     so the facade's `drain` dispatches uniformly over either.
   - `Sub` — a subscription's own lifecycle predicate.
   - `JetStream` — vends the JetStream context (ADR 0017). Unlike the others, the
     platform Connection records do NOT implement it inline; the JetStream impl
     namespaces `extend` it onto them, so the `@nats-io/jetstream` import stays out
     of a core-only CLJS bundle (ADR 0016).

   The lifecycle slice added -flush/-close/-drain; request/reply added -request;
   the queue-groups slice gave `-subscribe` a queue arg, returned a Subscription
   record (not the native handle) carrying `Drainable`/`Sub`, and split -drain out
   of `Conn` into `Drainable`; the unsubscribe slice added `-unsubscribe` to `Sub`;
   later slices grow it further (status)."
  ;; -flush would otherwise shadow cljs.core/-flush (IWriter); no clojure.core
  ;; var by that name exists, so the exclude is a no-op on the JVM.
  (:refer-clojure :exclude [-flush]))

(defprotocol Conn
  "The primitive operations every platform Connection record implements."
  (-publish [conn subject headers bytes]
    "Publish raw `bytes` to `subject`. Fire-and-forget; return value unused.
     `headers` is the canonical portable header map `{name -> vector-of-strings}`
     (case-sensitive string names) the facade normalizes to, or nil for none.")
  (-subscribe [conn subject queue opts handler]
    "Subscribe to `subject`, returning a Subscription record synchronously. When
     `queue` is a non-nil group name the subscription joins that queue group and
     the server load-balances each matching message to exactly one member; nil is
     a plain subscription that receives every matching message. `handler` is the
     low-level handler, invoked per message with a raw map
     `{:subject <string> :bytes <platform-bytes> :reply <string-or-nil>}`,
     where `:reply` is the message's reply-to subject (nil when absent).
     `opts` is `{:on-error <fn-or-nil> :max-pending <int-or-nil>}` (ADR 0006/0007):
     `:on-error` is a 1-arg sink for this subscription's async failures (a thrown
     handler value, a decode failure, or `:slow-consumer`); when absent, handler
     and decode failures fall back to the connection's `:on-status` `:error` event
     and `:slow-consumer` is dropped. `:max-pending` is a message-count threshold
     above which `:slow-consumer` fires.")
  (-request [conn subject bytes timeout-ms]
    "Send a request: publish raw `bytes` to `subject` with a managed reply inbox,
     returning a native promise of the raw reply map
     `{:subject <string> :bytes <platform-bytes> :reply <string-or-nil>}`. The
     promise rejects with an ex-info carrying `:type :no-responders` (nobody
     subscribes `subject`) or `:type :timeout` (responders exist but none answer
     within `timeout-ms`) (ADR 0002/0006).")
  (-flush [conn]
    "Flush pending writes, returning a native promise that settles once the
     server has processed everything buffered on the connection.")
  (-close [conn]
    "Close the connection, returning a native promise that settles once it is
     fully closed. Ends all of the connection's subscriptions."))

(defprotocol Drainable
  "Draining, supported by both a connection and a single subscription, so the
   facade's `drain` dispatches uniformly over either."
  (-drain [x]
    "Drain `x`, returning a native promise that settles once draining completes.
     A connection stops its subscriptions after their pending messages are
     delivered, flushes, then closes; a subscription ends just itself, leaving
     the connection open."))

(defprotocol Sub
  "A single subscription's lifecycle, beyond draining."
  (-active? [sub]
    "True while the subscription is still delivering — stays true through a
     drain's wind-down, flipping false only once the drain settles, the sub is
     unsubscribed, or the connection ends it (ADR 0022).")
  (-unsubscribe [sub max]
    "End this subscription abruptly, the lower-level sibling of `-drain`: tell the
     server to stop and drop any not-yet-delivered messages, returning nil
     synchronously. `max` nil unsubscribes now; a positive int auto-unsubscribes
     once the subscription has received that many messages over its lifetime
     (counted from subscription start). Idempotent: unsubscribing an
     already-ended subscription is a silent no-op (ADR 0012). The facade owns the
     arities and validates `max`."))

(defprotocol JetStream
  "Vends the JetStream context (ADR 0017), the single async handle every JetStream
   operation flows through. Defined here so the pure protocol lives in the
   core-reachable namespace, but EXTENDED onto each platform's Connection record
   from the JetStream impl namespaces (`nats-cljc.jetstream.impl.*`), never
   implemented inline on the record — so the `@nats-io/jetstream` import stays out
   of a core-only CLJS bundle (ADR 0016)."
  (-jetstream [conn]
    "Return a native promise of a JetStream context — a platform record holding
     both the data plane (publish, pull) and the management plane (stream/consumer
     admin), which the native clients split but the portable surface collapses into
     one. Obtaining it verifies JetStream is enabled by forcing a JS-info
     round-trip on both legs (native on CLJS, added inside the off-thread wrap on
     the JVM), so the promise rejects with `:type :jetstream-not-enabled` (err
     10039) at the handle when it is not — never deferred to the first operation
     (ADR 0017/0020)."))

(defprotocol StreamManager
  "JetStream stream management (ADR 0017), the management-plane verbs — EXTENDED
   onto each platform's JetStream context record from the impl namespaces, never
   implemented inline, so the `@nats-io/jetstream` import stays confined (ADR 0016).
   The facade owns the public arglists and the pre-flight validation; these deal in
   the portable closed kebab config map and the normalized info map, translating
   to/from each leg's native config inside the impl (ADR 0020)."
  (-create-stream [ctx config]
    "Create a Stream from the portable closed kebab `config` (already validated by
     the facade), returning a native promise of the normalized StreamInfo map. The
     promise rejects with an operational `:jetstream-api-error` (carrying
     `{:code :description}`) when the server rejects the config (ADR 0020).")
  (-update-stream [ctx config]
    "Update an existing Stream's configuration from the portable closed kebab
     `config` (already validated by the facade), MERGING the keys present over the
     Stream's current config — an absent key keeps its current value, never reverts
     to a server default — and returning a native promise of the normalized
     StreamInfo map. nats.js merges natively (its `update` reads the current config
     first); the JVM leg reproduces that read-merge-write so the semantics are
     identical (ADR 0020). Rejects with `:type :stream-not-found` when the Stream
     does not exist, or an operational `:jetstream-api-error` when the server
     rejects the change.")
  (-stream-info [ctx name]
    "Return a native promise of the normalized StreamInfo map for the Stream
     `name`, rejecting with `:type :stream-not-found` when it does not exist.")
  (-purge-stream [ctx name]
    "Purge every message from the Stream `name` while keeping its definition,
     returning a native promise of `{:purged <count>}` and rejecting with `:type
     :stream-not-found` when it does not exist.")
  (-delete-stream [ctx name]
    "Delete the Stream `name`, returning a native promise that resolves to nil once
     it is gone and rejects with `:type :stream-not-found` when it does not exist.")
  (-list-streams [ctx]
    "Return a native promise of a vector of the normalized StreamInfo maps for
     every Stream on the server.")
  (-stream-names [ctx]
    "Return a native promise of a vector of every Stream name on the server — the
     dedicated names endpoint both native clients expose (unlike consumer names,
     which the facade derives), so the listing never pays for full infos."))

(defprotocol ConsumerManager
  "JetStream consumer management (ADR 0017), the management-plane verbs for durable
   Consumers — EXTENDED onto each platform's JetStream context record from the impl
   namespaces, never implemented inline, so the `@nats-io/jetstream` import stays
   confined (ADR 0016). The facade owns the public arglists and the pre-flight
   validation; these deal in the portable closed kebab config map and the normalized
   info map, translating to/from each leg's native config inside the impl (ADR 0020).
   `consumer-names` has no method here — the facade derives it from `-list-consumers`,
   since nats.js exposes no names endpoint."
  (-create-consumer [ctx stream config]
    "Create a durable Consumer on the Stream `stream` from the portable closed kebab
     `config` (already validated by the facade), returning a native promise of the
     normalized ConsumerInfo map. The promise rejects with an operational
     `:jetstream-api-error` (carrying `{:code :description}`) when the server rejects
     the config (ADR 0020).")
  (-update-consumer [ctx stream config]
    "Update an existing durable Consumer's configuration on the Stream `stream` from
     the portable closed kebab `config` (already validated by the facade), MERGING
     the keys present over the Consumer's current config — an absent key keeps its
     current value, never reverts to a server default — and returning a native
     promise of the normalized ConsumerInfo map. nats.js merges natively (its
     `consumers.update` reads the current config first); the JVM leg reproduces that
     read-merge-write so the semantics are identical (ADR 0020). Rejects with `:type
     :consumer-not-found` when the Consumer does not exist, or an operational
     `:jetstream-api-error` when the server rejects the change (e.g. an immutable
     field).")
  (-consumer-info [ctx stream name]
    "Return a native promise of the normalized ConsumerInfo map for the Consumer
     `name` on Stream `stream`, rejecting with `:type :consumer-not-found` when it
     does not exist.")
  (-delete-consumer [ctx stream name]
    "Delete the Consumer `name` on Stream `stream`, returning a native promise that
     resolves to nil once it is gone and rejects with `:type :consumer-not-found`
     when it does not exist.")
  (-list-consumers [ctx stream]
    "Return a native promise of a vector of the normalized ConsumerInfo maps for
     every Consumer on Stream `stream`."))

(defprotocol JetStreamData
  "JetStream data-plane operations (ADR 0017) — the publish/pull verbs, the sibling
   of the management-plane `StreamManager` — EXTENDED onto each platform's JetStream
   context record from the impl namespaces, never implemented inline, so the
   `@nats-io/jetstream` import stays confined (ADR 0016). The facade owns the public
   arglists, the pre-flight validation, and codec encode/decode; these deal in raw
   wire bytes, the canonical portable header map, and the normalized PubAck."
  (-js-publish [ctx subject headers bytes opts]
    "Acked publish of raw `bytes` to `subject` through the JetStream context `ctx`,
     returning a native promise of the normalized PubAck map
     `{:stream :seq :duplicate :domain}`. `headers` is the canonical portable header
     map `{name -> vector-of-strings}` (already guarded + normalized by the facade) or
     nil; `opts` is `{:msg-id :expect :timeout-ms}`, which the impl translates to the
     native publish options (`:msg-id`/`:expect` become the sanctioned reserved
     headers). The promise rejects with an operational `:type :wrong-last-sequence`
     when an `:expect` assertion fails, and the catch-all `:jetstream-api-error`
     for any other server rejection (ADR 0020).")
  (-js-next [ctx stream consumer opts]
    "Poll a single message from the `consumer` on Stream `stream` through `ctx`,
     returning a native promise of ONE raw JetStream message map
     `{:subject :bytes :headers :js {...}}` (`:headers` is nil when the message carries
     none) — the per-leg `js-msg->raw` lift, which reads native metadata, captures the
     ack-subject string, then discards the native object (ADR 0019) — or nil when no
     message arrives within the poll's `:expires-ms` wait (an empty consumer). `:js` carries
     `{:stream :consumer :stream-seq :delivery-seq :delivered :pending :redelivered
     :timestamp :domain :ack-subject}`, `:timestamp` ISO-8601 and `:redelivered` =
     (delivered > 1). The facade decodes `:bytes`, and adds trimmed `:headers` only when
     present, dropping it when absent (ADR 0005).")
  (-js-fetch [ctx stream consumer opts]
    "Fetch a bounded batch from the `consumer` on Stream `stream` through `ctx`,
     returning a native promise of a VECTOR of up to `:batch` raw JetStream message
     maps (the same `js-msg->raw` lift as `-js-next`), in stream order. The batch
     settles when `:batch` messages have arrived or the `:expires-ms` window elapses,
     so a consumer with fewer than `:batch` pending yields a shorter vector (ADR 0018).")
  (-js-consume [ctx stream consumer opts handler]
    "Continuously deliver from the `consumer` on Stream `stream` through `ctx`,
     returning a native promise of a handle carrying `Drainable`/`Sub` — the
     JetStream counterpart of `-subscribe`'s Subscription record (ADR 0018).
     `handler` is the low-level handler, invoked per message with ONE raw JetStream
     message map (the per-leg `js-msg->raw` lift, as `-js-next`); a returned
     promise suspends the next delivery until it settles, gating the impl's own
     pull rate (per-message backpressure, ADR 0007 road 2 — each leg drives its
     native consume machinery, so the refill engages natively). `opts` is the
     validated refill-knob map `{:batch :threshold :expires-ms :idle-heartbeat-ms
     :max-bytes}` the impl translates to native consume options (`:threshold`
     count->percent on the JVM). There is no `:slow-consumer` in pull: a slow
     handler slows the pull, nothing overflows (ADR 0018).")
  (-js-get-message [ctx stream query]
    "One-shot direct read of a stored message from the Stream `stream` through
     `ctx`, selected by `{:seq n}` (a stream sequence) or `{:last-by-subject s}`
     (the newest message stored on a subject) — already validated by the facade.
     Returns a native promise of ONE raw stored-message map
     `{:subject :bytes :headers :seq :timestamp}` (`:headers` nil when the message
     carries none; `:timestamp` the canonical ISO-8601 receive time). A read from
     the stream's storage, not a consumer delivery, so there is no `:js` consumer
     metadata and no ack-subject. Rejects with `:type :no-message-found` (err
     10037, carrying `{:code :description}`) when nothing matches — jnats raises
     the 10037, nats.js absorbs it to null and the impl re-raises, so the legs
     agree — and `:stream-not-found` when the Stream does not exist (ADR 0020).")
  (-js-ordered-consumer [ctx stream opts]
    "Create an Ordered consumer over the Stream `stream` through `ctx`, returning a
     native promise of the per-leg ordered pull handle (an `OrderedPull` record,
     carrying the context codec) the facade's pull triad then dispatches over. The
     opts map is the validated ordered config `{:filter-subjects :deliver-policy}`,
     translated to each leg's native ordered-consumer options. Both legs round-trip
     stream info at creation, so the promise rejects with `:type :stream-not-found`
     when no such Stream exists (ADR 0020). The consumer is a server-managed
     EPHEMERAL with ack policy none; the native client recreates it on a sequence
     gap, which is what makes the replay gap-free."))

(defprotocol KV
  "Vends the KV context (ADR 0017's twin for the KV facade) — defined here so the
   pure protocol lives in the core-reachable namespace, but EXTENDED onto each
   platform's Connection record from the KV impl namespaces
   (`nats-cljc.kv.impl.*`), never implemented inline on the record — so the
   `@nats-io/kv` import stays out of a core-only CLJS bundle (ADR 0016)."
  (-kv [conn]
    "Return a native promise of a KV context — a platform record wrapping the
     native KV management client every Bucket-lifecycle operation flows through
     (ADR 0003: jnats KeyValueManagement on the JVM, @nats-io/kv's Kvm on CLJS).
     Obtaining it verifies JetStream is enabled by forcing a JS-info round-trip on
     both legs (KV is JetStream-backed), so the promise rejects with `:type
     :jetstream-not-enabled` at the handle when it is not — never deferred to the
     first operation, exactly like `-jetstream` (ADR 0017)."))

(defprotocol BucketManager
  "KV Bucket lifecycle (ADR 0017/0023) — EXTENDED onto each platform's KV context
   record from the impl namespaces, never implemented inline, so the `@nats-io/kv`
   import stays confined (ADR 0016). The facade owns the public arglists and the
   pre-flight validation; these deal in the portable closed kebab config map and
   speak KV vocabulary on rejection — `:bucket-not-found`, never the stream-layer
   `:type` the native clients raise from the substrate (ADR 0023)."
  (-create-bucket [ctx config]
    "Create a Bucket from the portable closed kebab `config` (already validated by
     the facade), returning a native promise of a Bucket handle — the platform
     record carrying the native KV client bound to the Bucket plus the context's
     codec, which the entry operations dispatch over. The promise rejects with an
     operational `:jetstream-api-error` (carrying `{:code :description}`) when the
     server rejects the config (ADR 0020).")
  (-open-bucket [ctx bucket]
    "Open the existing Bucket named `bucket`, returning a native promise of a
     Bucket handle (as `-create-bucket`). Opening VERIFIES the Bucket exists —
     forcing a status round-trip on the leg whose native open merely binds — so the
     promise rejects with `:type :bucket-not-found` at the handle when it does not
     (ADR 0023), never deferred to the first entry operation.")
  (-delete-bucket [ctx bucket]
    "Delete the Bucket named `bucket`, returning a native promise that resolves to
     nil once it is gone and rejects with `:type :bucket-not-found` when no such
     Bucket exists (ADR 0023).")
  (-bucket-names [ctx]
    "Enumerate the Buckets on the server, returning a native promise of a vector
     of Bucket name strings.")
  (-list-buckets [ctx]
    "Enumerate the Buckets on the server, returning a native promise of a vector
     of normalized status maps — one per Bucket, the same shape `-bucket-status`
     resolves with.")
  (-bucket-status [ctx bucket]
    "Read the status of the Bucket named `bucket`, returning a native promise of
     the normalized portable status map — the bucket-config keys as the server
     applied them, plus the observed `:values` / `:bytes` counters — identical in
     shape on every leg. Rejects with `:type :bucket-not-found` when no such
     Bucket exists (ADR 0023)."))

(defprotocol BucketEntries
  "KV entry operations over a Bucket handle (ADR 0023) — EXTENDED onto each
   platform's Bucket record from the impl namespaces, never implemented inline, so
   the `@nats-io/kv` import stays confined (ADR 0016). The facade owns the public
   arglists, the pre-flight key validation, and the codec seam: these deal in WIRE
   BYTES and raw entry maps, never decoded values, exactly like the JetStream pull
   verbs."
  (-kv-put [bucket key bytes]
    "Write `bytes` under `key` in the Bucket, returning a native promise that
     resolves to the new Revision as a bare number.")
  (-kv-create [bucket key bytes]
    "First-writer-wins write: store `bytes` under `key` only when the key is
     absent, returning a native promise that resolves to the new Revision as a
     bare number and rejects with `:type :wrong-revision` carrying the contested
     `:key` when the key already exists (ADR 0023) — never the substrate's
     `:wrong-last-sequence`, though the wire condition is the same.")
  (-kv-update [bucket key bytes revision]
    "Revision-guarded write: store `bytes` under `key` only when `revision` is
     still the key's latest Revision, returning a native promise that resolves
     to the new Revision as a bare number and rejects with
     `:type :wrong-revision` carrying the contested `:key` when the expected
     Revision is stale (ADR 0023).")
  (-kv-get [bucket key revision]
    "Read an entry for `key`, returning a native promise of a RAW entry map
     `{:bucket :key :bytes :revision :created :operation}` — `:bytes` the
     undecoded wire value, `:created` the canonical ISO-8601 timestamp string,
     `:operation` the keyword form of the entry's KV operation. `revision` nil is
     the latest read: it resolves nil when the key reads as absent — never
     written, deleted, or purged (ADR 0023); the leg whose native get surfaces
     tombstones normalizes them to nil here, so absence is one portable contract.
     A non-nil `revision` is the pinned read at that exact Revision: a
     delete/purge marker DELIVERS as a raw entry with its `:operation` visible —
     the leg whose native hides markers on a pinned read reconstructs them from
     the stream substrate here — while a Revision the Bucket never assigned, or
     one belonging to another key, resolves nil.")
  (-kv-keys [bucket filter]
    "Enumerate the LIVE keys in the Bucket — deleted and purged keys excluded —
     returning a native promise of a fully-realized vector of key strings (the
     stream-names precedent). `filter` is an optional subject-style filter
     string restricting the result (nil for every key); a filter matching
     nothing resolves to [].")
  (-kv-delete [bucket key revision]
    "Write a Tombstone for `key` — the key subsequently reads as absent while its
     history is retained — returning a native promise that resolves to nil.
     `revision` is the optional guard (nil for unguarded): when non-nil the
     Tombstone lands only if it is still the key's latest Revision, rejecting
     with `:type :wrong-revision` carrying the contested `:key` when stale
     (ADR 0023).")
  (-kv-purge [bucket key revision]
    "Erase the history of `key` down to a single purge marker, returning a
     native promise that resolves to nil. `revision` is the optional guard (nil
     for unguarded), rejecting with `:type :wrong-revision` carrying the
     contested `:key` when stale (ADR 0023).")
  (-kv-purge-deletes [bucket]
    "Remove every Tombstoned key's retained history from the Bucket — marker
     included, regardless of the marker's age (the natives' default 30-minute
     grace is overridden to none on both legs) — returning a native promise
     that resolves to nil. Live keys keep their Entries untouched; a Bucket
     with no Tombstones is a safe no-op.")
  (-kv-history [bucket key]
    "Read the retained history of `key`, returning a native promise of a
     fully-realized vector of RAW entry maps oldest-to-newest —
     `{:bucket :key :bytes :revision :created :operation :delta}`, the `-kv-get`
     shape plus `:delta` (the entry's distance from the key's newest revision,
     populated by both natives) — INCLUDING Tombstones and purge markers, whose
     `:operation` stays visible (ADR 0023). An absent key resolves to []."))

(defprotocol BucketWatch
  "Watching a Bucket handle (ADR 0023) — EXTENDED onto each platform's Bucket
   record from the impl namespaces, never implemented inline, so the
   `@nats-io/kv` import stays confined (ADR 0016). The facade owns the public
   arglists, the `:deliver` pre-flight validation, and the codec seam: the
   raw-handler it passes down receives RAW entry maps, never decoded values."
  (-kv-watch [bucket opts raw-handler]
    "Open a Watch, returning a native promise of a watch handle — a platform
     record satisfying `Watch`, whose `:initialized` key holds a native promise
     that resolves when the initial replay completes (immediately when there is
     nothing to replay). `opts` is the facade-normalized map
     `{:deliver :keys :ignore-deletes? :on-error}`: `:deliver` is the validated
     mode — `:latest` replays each key's current value then streams updates,
     `:history` replays the full retained history first, `:updates` streams only
     new changes (its `:initialized` resolves at once); `:keys` is nil (every
     key) or a non-empty vector of subject-style key patterns restricting the
     Watch to their union; `:ignore-deletes?` true suppresses Tombstone and
     purge-marker deliveries; `:on-error` is the per-Watch async-failure sink —
     a raw-handler throw (the facade's decode seam) or a rejecting returned
     promise routes there when set, else to the connection's `:on-status` as an
     `:error` event, never both (ADR 0006/0007). `raw-handler` is invoked per
     matching entry — including Tombstones and purge markers, unless ignored —
     with one raw entry map (the `-kv-history` shape: `{:bucket :key :bytes
     :revision :created :operation :delta}`); deliveries are serial within one
     Watch, and a returned promise suspends the next delivery until it settles
     (the ADR 0007 contract)."))

(defprotocol Watch
  "A single Watch's lifecycle — implemented by the per-leg watch handle records
   `-kv-watch` resolves with."
  (-watch-stop [watch]
    "End the Watch's delivery, returning nil synchronously — fire-and-forget,
     the watch sibling of `-unsubscribe`. Idempotent: stopping an already-ended
     Watch is a silent no-op (ADR 0012 spirit)."))

(defprotocol Service
  "Hosting a Service (ADR 0024) — EXTENDED onto each platform's Connection record
   from the service impl namespaces (`nats-cljc.service.impl.*`), never implemented
   inline on the record, so the `@nats-io/services` import stays out of a core-only
   CLJS bundle (ADR 0016/0026). Unlike `-kv`/`-jetstream` there is no context and
   nothing is verified at entry: `-create-service` hangs directly off the
   Connection (ADR 0024). The facade owns the public arglists, the codec seam (the
   handler's raw bytes are decoded there, the respond value encoded there), and the
   ADR-0007 Handler delivery; these deal in raw wire bytes and the per-leg native
   service message."
  (-create-service [conn config]
    "Create and start a Service from the portable `config`
     `{:name :version :description :metadata :endpoints}`, returning a native
     promise of a running Service handle — a platform record satisfying
     `ServiceLifecycle`. Each endpoint `{:name :subject :handler :queue-group
     :metadata}` (already defaulted by the facade — `:subject` is non-nil) binds a
     queue-subscribed native handler; `handler` is the LOW-LEVEL handler, invoked
     per request with a raw map `{:subject :bytes :reply :headers ::native}` where
     `::native` is the per-leg native service message `-respond` routes a reply
     through so native per-endpoint stats stay correct. The handler is an ADR-0007
     push Handler (serial per endpoint, may return a native promise for
     backpressure).")
  (-respond [conn native bytes]
    "Reply to the request whose native service message is `native` with raw
     `bytes`, routing through that native message (not a bare publish to the reply
     subject) so the owning endpoint's native stats stay correct — the service
     analog of `-publish` to a reply subject, threading `conn` as jnats'
     `ServiceMessage.respond(conn, bytes)` requires. Returns nil.")
  (-respond-error [conn native code description bytes]
    "Reply to the request whose native service message is `native` with a service
     error (ADR 0025): integer `code`, string `description`, and optional raw
     `bytes` as the body (nil for an empty body), routed through that native
     message so the owning endpoint's native ERROR stats stay correct. Sets the
     `Nats-Service-Error` / `Nats-Service-Error-Code` headers the facade's `error`
     reads back; `conn` is threaded as in `-respond`. Returns nil."))

(defprotocol ServiceLifecycle
  "A running Service's lifecycle — implemented by the per-leg Service handle record
   `-create-service` resolves with."
  (-stop-service [svc]
    "Stop the Service and tear it down, returning a native promise that settles
     once it has stopped — enough for test teardown (full drain semantics and the
     `:stopped` promise are the lifecycle slice's job)."))

(defprotocol OrderedPull
  "The pull triad over an Ordered consumer handle (the value `-js-ordered-consumer`
   resolves) — EXTENDED onto each platform's ordered record from the impl
   namespaces, the ordered sibling of `JetStreamData`'s named-consumer pull verbs.
   Same raw-map contract: each leg drives its native ordered consumer (jnats'
   OrderedConsumerContext, nats.js' ordered pull Consumer), which tracks the
   delivery sequence client-side and recreates the ephemeral on a gap, so the
   lifted messages arrive in stream order with no acknowledgements taken."
  (-oc-next [oc opts]
    "Poll a single message from the ordered handle `oc`, returning a native promise
     of ONE raw JetStream message map (the per-leg `js-msg->raw` lift, as
     `-js-next`) or nil when no message arrives within the poll's `:expires-ms`
     wait.")
  (-oc-fetch [oc opts]
    "Fetch a bounded batch from the ordered handle `oc`, returning a native promise
     of a VECTOR of up to `:batch` raw JetStream message maps in stream order, as
     `-js-fetch`.")
  (-oc-consume [oc opts handler]
    "Continuously deliver from the ordered handle `oc`, returning a native promise
     of a drainable/unsubscribable consume handle, as `-js-consume` — same handler
     contract (one raw map per call, a returned promise gates the next delivery)
     and the same refill-knob `opts`."))
