# nats-cljc

A Clojure/ClojureScript library that exposes [NATS](https://nats.io) messaging under a single, portable `.cljc` API. The same consumer code is intended to compile and run unchanged across three platforms — the JVM, the browser, and Node.

## Language

### Platforms & transport

**Platform**:
One of the three execution environments nats-cljc targets: JVM, browser, or Node. Used to say *where* portable code runs and which transport applies.
_Avoid_: runtime, target, host, environment

**Transport**:
The wire binding a connection uses to reach a NATS server: TCP on the JVM, WebSocket on the browser and Node. WebSocket is mandatory for any ClojureScript platform, so a reachable server must have its websocket listener enabled.
_Avoid_: protocol, link, channel

### Async surface

**Promise**:
The value returned by every one-shot operation (connect, request, publish-with-ack, …): a single eventual result-or-error that the caller awaits. The portable currency for "this finishes once."
_Avoid_: future, deferred

**Handler**:
The function the caller supplies to receive delivered messages, one call per message, for as long as a subscription is active. The portable currency for "this happens many times."
_Avoid_: callback, listener, subscriber, consumer (*consumer* is reserved for its JetStream meaning)

### Connection

**Connection**:
The value `connect` returns: a native client wrapped together with a default codec and options, and the thing every publish, subscribe, and request flows through. Draining or closing it ends all of its subscriptions.
_Avoid_: client, session

**Status event**:
A normalized connection-lifecycle notification delivered to an `:on-status` handler, identical in shape on every platform. Canonical `:type`s: `:connected`, `:disconnected`, `:reconnecting`, `:reconnected`, `:closed`, `:error`, `:lame-duck`, `:servers-changed`. (`:slow-consumer` is *not* here: it is inherently per-subscription, so it is an *Error* `:type` routed to the subscription's `:on-error`, keeping every *lifecycle* `:on-status` event a bare connection-level `{:type ...}`.)
The contract normalizes *shape*, not *cadence*: each delivered lifecycle event is a bare `{:type ...}` map drawn from the canonical set, but the count, ordering, and trigger conditions follow each underlying client's native strategy and may differ per platform (see ADR 0006). The `:error` event is the lone exception to bareness: it carries the offending Error under an `:error` key (`{:type :error :error <ex-info>}`), so a consumer dispatches uniformly on `(:type ev)` and, for `:error`, reads the canonical error `:type` with `(:type (ex-data (:error ev)))`. Known divergences: a single connection loss yields one `:reconnecting` on the JVM (synthesized) but one per dial attempt on Node/browser (nats.js' native signal); `:servers-changed` fires only when genuinely new servers are gossiped on the JVM, but on essentially every server INFO on Node/browser. Portable consumers should treat each `:type` as an edge to react to, not a counter.
_Avoid_: connection listener, notification

**Reconnect**:
The client's automatic re-establishment of a dropped connection, configured with the `:reconnect {:max :wait-ms :jitter-ms}` connect-option. `:max` is a non-negative attempt count with two sentinels shared by both underlying clients: `0` disables reconnection, `-1` is unlimited; `:wait-ms`/`:jitter-ms` set the per-attempt delay and its random spread. Any absent key leaves the native client's own default in place — and those defaults differ (JVM 60 attempts, Node/browser 10), so omitting `:max` does not give identical retry behavior across platforms.
_Avoid_: retry, redial

**Error**:
A failure surfaced as an `ex-info` carrying a canonical `:type` and structured data, identical in shape on every platform (so portable code reads `(:type (ex-data e))` rather than branching on host exception types). Canonical `:type`s: `:timeout`, `:no-responders`, `:connect-failed`, `:connection-closed`, `:permissions-violation`, `:codec-error`, `:max-payload-exceeded`, `:protocol-error`, `:drained`, `:slow-consumer`, `:auth-invalid`. JetStream extends this set with operational `:type`s — `:jetstream-not-enabled`, `:stream-not-found`, `:consumer-not-found`, `:wrong-last-sequence`, `:no-message-found` (a direct `get-message` that matched nothing — err 10037), `:jetstream-api-error` (one-shot rejections), KV adds `:bucket-not-found` (an `open-bucket` on a Bucket that does not exist — the KV face of `:stream-not-found`) and `:wrong-revision` (a compare-and-set write losing its race: an `update` whose expected Revision is stale, or a `create` on a key that already exists — the KV face of the server's wrong-last-sequence condition, carrying the `:key`), plus the inherently-per-consume `:heartbeats-missed`, `:consumer-deleted`, `:exceeded-limits` (delivered to a consume's `:on-error` only, dropped if unset, exactly like `:slow-consumer`) — see ADR 0020. `:auth-invalid` is client-side credential validation failing *before* any dial (e.g. an nkey that does not match its seed), distinct from `:connect-failed` (the server-side connect attempt failed); it rejects the `connect` promise. One-shot operations reject their promise with it. Async failures reach a sink instead: a throwing handler or a decode failure reaches the subscription's `:on-error` if one is set, else the connection's `:on-status` as an `:error` event; `:slow-consumer` — being per-subscription — reaches the subscription's `:on-error` only (and is silently dropped when none is set); while connection-level `:permissions-violation` and `:protocol-error` reach `:on-status` as an `:error` event only, never a per-subscription override. The override is strict: when a subscription sets `:on-error`, only it fires — never both it and `:on-status`. `:on-error` receives the bare ex-info, so portable code reads `(:type (ex-data e))` exactly as on the one-shot reject path; the connection-level `:error` event wraps that ex-info under `:error` (see Status event). A thrown handler value is passed through unchanged and carries no canonical `:type` — it is the consumer's own exception, not a normalized NATS failure.
_Avoid_: failure, fault (a bare host *exception* is what we normalize *into* an Error)

**Validation error**:
A caller-misuse failure — a malformed argument caught *before* any native NATS call — surfaced as an `ex-info` carrying a `:type` from a separate set, distinct from the canonical *Error* set and sharing only its shape. Current `:type`s: `:invalid-header` (a header name or value that is not a printable-ASCII token), `:invalid-max` (a `max` argument outside the integer range its operation accepts — the `2147483647` JVM `int` cap on both legs for `unsubscribe`/blocking `subscribe`, with `:reconnect` additionally allowing the `0`/`-1` sentinels), `:invalid-max-pending` (a non-positive `subscribe` `:max-pending`), `:no-reply-subject` (`reply` to a message that has no `:reply`), and `:invalid-capacity` (a non-positive blocking-`subscribe` capacity). JetStream adds `:invalid-name` (a malformed stream/consumer name), `:unknown-config-key` (an unrecognized stream/consumer config key), `:missing-required-key` (a required config key omitted — e.g. a durable consumer's `:name`, carrying the offending `:key`), `:reserved-header` (a reserved `Nats-*` header set directly in a publish's `:headers` rather than via `:msg-id`/`:expect`), `:invalid-expires` (a pull `fetch`/`next`/`consume` `:expires-ms` below the 1000ms floor both clients enforce, or not a whole number of milliseconds, carrying the offending `:expires-ms`), `:invalid-batch` (a `consume` `:batch` that is not a positive integer, carrying the offending `:batch`), `:invalid-threshold` (a `consume` `:threshold` that is not a positive integer no greater than `:batch`, carrying the offending `:threshold` and the effective `:batch`), `:exclusive-window` (a `consume` `:max-bytes` byte window combined with the message-count `:batch`/`:threshold` — a pull window is bounded by count or by bytes, never both — carrying `:max-bytes`, `:batch`, and `:threshold`), `:no-ack-subject` (an ack verb — `ack`/`nak`/`term`/`working`/`double-ack` — called on a message carrying no `:js :ack-subject`, one that never came off a JetStream pull, carrying the offending `:subject`), and `:invalid-query` (a `get-message` query that does not select by exactly one well-formed selector — `:seq` a positive integer or `:last-by-subject` a non-empty string — carrying the offending `:query`; an unrecognized query key reuses `:unknown-config-key`, the map being closed). KV adds `:invalid-key` (a key whose syntax both native clients reject before any wire call — outside the `[-/_=.a-zA-Z0-9]` charset, a leading/trailing `.`, or a wildcard — carrying the offending `:key`; malformed *Bucket* names reuse `:invalid-name`, buckets being infrastructure names like streams), `:invalid-deliver` (a Watch `:deliver` mode outside the closed `:latest` / `:history` / `:updates` set, carrying the offending `:deliver`), and `:invalid-keys` (a Watch `:keys` that is an empty sequence of patterns — the union of zero patterns matches nothing, so silently widening it to every key would invert the caller's intent — carrying the offending `:keys`). Service adds `:invalid-version` (a `service/create` `:version` that is not valid semver, carrying the offending `:version`) and `:duplicate-endpoint` (two endpoints in a `service/create` config declaring the same `:name` — the name is the per-endpoint stats key and must be unique, carrying the offending `:name`); malformed service/endpoint *names* reuse `:invalid-name` (they are subject-token infrastructure names like buckets/streams), and a missing `:name`/`:version` reuses `:missing-required-key`. Raised through the operation's *own* channel — a synchronous operation **throws** it, the one promise-returning operation that validates (`connect`) **rejects its promise** — and it **never** reaches an async sink (`:on-error`/`:on-status`), which is exclusively the *Error* model's channel (ADR 0006, ADR 0015). Where the canonical *Error* `:type`s are operational conditions a consumer dispatches on in production, a validation `:type` is a programmer error to *fix*: the set is diagnostic-first and **open** — new guards add new `:type`s — so consumers should not assume it exhaustive.
_Avoid_: error (reserve *Error* for a normalized NATS failure); argument exception, bad-input fault

### Messaging

**Message**:
The unit published to and delivered from NATS: a subject, its data, optional headers, and an optional reply subject. Delivered and published as a plain-keyword map `{:subject :data :headers :reply}`, where `:data` is the decoded value.
_Avoid_: event, packet

**Subject**:
A dot-delimited token string naming where a message is published and what a subscription listens to; supports the wildcards `*` (exactly one token) and `>` (one or more trailing tokens). The string is canonical; a builder helper composes one from parts.
_Avoid_: topic, address, queue (a *queue group* is a different thing)

**Headers**:
Optional named string values carried by a message, HTTP-style: case-sensitive string names, each mapping to one or more string values. Delivered as `name → vector-of-strings`; a scalar value is accepted on publish and normalized to a one-element vector. Surrounding whitespace is insignificant and stripped on delivery.
_Avoid_: properties, attributes, metadata

**Data**:
The decoded value a message carries — ordinary Clojure data, once a codec has been applied. The raw wire form is "bytes" and is never called data.
_Avoid_: payload, body, content

**Codec**:
The pluggable component that converts between Clojure values and the bytes on the wire. A connection has a default codec; any publish/subscribe/request/reply may override it.
_Avoid_: serializer, serde, marshaller (encoder/decoder are the two directions *within* a codec, not synonyms for it)

**Subscription**:
The active interest in a subject that `subscribe` returns synchronously, delivering each matching message to its handler until unsubscribed or drained. May belong to a queue group.
_Avoid_: subscriber, listener

**Pull subscription**:
The interest a JVM-only blocking `subscribe` returns: a handle with no handler, drained one message at a time by `take-message` (which blocks up to a timeout) rather than pushed to. Backed by a bounded buffer that backpressures the producer. The synchronous counterpart to the portable push *Handler* model, and the reason the blocking layer exists.
_Avoid_: poll loop, pull consumer (*consumer* is reserved for its JetStream meaning), queue

**Unsubscribe**:
Ending a subscription *abruptly*: the server is told to stop and any not-yet-delivered messages are dropped, returning `nil` synchronously. Contrast *draining* a subscription, which delivers the already-buffered messages first and is awaitable — both end a subscription, but unsubscribe discards the backlog where drain flushes it. An optional `max` makes the subscription auto-unsubscribe once it has received that many messages over its lifetime (counted from subscription start; messages already delivered past the limit are never recalled).
_Avoid_: cancel, stop, remove (a *Subscription* is unsubscribed; a *Connection* is closed)

**Queue group**:
A named set of subscriptions to the same subject among which the server load-balances, so each message reaches exactly one member. Selected with the `:queue` option on `subscribe`.
_Avoid_: consumer group, worker pool

### JetStream

**JetStream**:
NATS's durable persistence layer over core pub/sub: messages are retained in server-side storage and replayed to clients on demand, rather than delivered once and forgotten. The portable surface lives in the `nats-cljc.jetstream` namespace.
_Avoid_: JS (collides with JavaScript), durable messaging, stream store

**Stream**:
A server-side, durable store of messages captured from one or more Subjects — the unit JetStream retains and replays.
_Avoid_: log, topic, queue, channel

**Consumer**:
A server-side delivery cursor over a Stream that tracks position and outstanding acknowledgements, and the thing a client pulls messages from — the JetStream counterpart to a core Subscription. The library exposes only *pull* Consumers: the client pulls, the server never pushes, so the client's read rate bounds delivery. Every Consumer is either Durable or Ephemeral.
_Avoid_: subscriber, reader, subscription (a *Subscription* is the core pub/sub term; a *Pull subscription* is the JVM blocking-core handle, unrelated)

**Durable consumer**:
A Consumer the server persists by name, retaining its position and ack state across client disconnects and restarts until explicitly deleted. The default kind `create-consumer` builds (`:durable? true`); its `:name` is required.
_Avoid_: named consumer, persistent consumer

**Ephemeral consumer**:
A Consumer the server does not persist: it carries no durable name and the server reclaims it after a window of client inactivity. Requested with `:durable? false`; its `:name` is optional (the server assigns one when omitted). The Ordered consumer is a specialized Ephemeral consumer.
_Avoid_: transient consumer

**Ordered consumer**:
A specialized Ephemeral consumer for single-client, gap-free replay: server-managed, automatically recreated if a sequence gap appears, and taking no acknowledgements.
_Avoid_: replay consumer

**JetStream context**:
The handle `(jetstream conn)` returns — a single value holding both the data-plane (publish, pull) and management (stream/consumer admin) surfaces, through which every JetStream operation flows. The JetStream counterpart to a Connection.
_Avoid_: JS context (collides with JavaScript), JetStream client, handle

**Acked publish**:
A publish into a Stream that waits for the server to acknowledge durable storage and returns a PubAck — in contrast to core's fire-and-forget publish, which returns nothing.
_Avoid_: durable publish, confirmed publish

**PubAck**:
The server's receipt for an Acked publish: `{:stream :seq :duplicate :domain}` — the Stream, the assigned stream sequence, whether the message was a dedup duplicate, and the JetStream domain.
_Avoid_: ack (a consumer's *Ack* acknowledges a delivered message; a *PubAck* is the publisher's receipt)

**Ack**:
A consumer's acknowledgement of a delivered JetStream Message, sent by publishing a tiny control message to the Message's ack subject. The family: `ack` (processed — stop redelivery), `nak` (redeliver, optionally after a delay), `term` (give up — never redeliver), `working` (still processing — postpone redelivery). All are fire-and-forget (return nil) and idempotent; `working` is exempt from terminality.
_Avoid_: PubAck (the publisher's receipt), acknowledgement, nack (spell it *nak*)

**Double-ack**:
An Ack that waits for the server to confirm receipt, returning a Promise — for when losing an ack to a network blip is unacceptable. The one acknowledgement that is awaitable; the rest are fire-and-forget.
_Avoid_: ack-sync (ours is asynchronous), confirmed ack, ackAck

### KV

**Bucket**:
A named key-value store hosted on JetStream — the KV unit of creation, configuration, and deletion. The portable surface lives in the `nats-cljc.kv` namespace.
_Avoid_: store, table, map, stream (its JetStream substrate is an implementation detail)

**KV context**:
The handle `(kv conn)` returns — the value through which every Bucket management operation (create, open, delete, list, status) flows, verified at entry like the JetStream context. The KV counterpart to a JetStream context.
_Avoid_: KV manager, Kvm

**Entry**:
The unit a Bucket stores and KV reads deliver: a plain map `{:bucket :key :value :revision :created :operation}`, where `:value` is the decoded Clojure value (through the Bucket's Codec — the KV counterpart to a Message's `:data`, named `:value` because key/value is the KV domain's own language). A `get` on an absent key (never written, deleted, or purged) resolves to `nil` — absence is a normal domain outcome to branch on, not an Error; an Entry whose stored value is nil is distinguishable as `{:value nil ...}`. `history` reads and Watch deliveries carry one extra key, `:delta` — the Entry's distance from the newest matching delivery (0 for the newest / once caught up; both natives populate it there) — which a one-shot `get` Entry does not carry.
_Avoid_: record, row, message (an Entry rides on a JetStream message, but that is substrate), :data (reserved for what a Message carries)

**Revision**:
The monotonically increasing sequence number a Bucket assigns an Entry on every write — the currency of compare-and-set: `update` succeeds only when the caller's expected Revision is still the Entry's latest.
_Avoid_: version, sequence (its stream-sequence substrate is an implementation detail)

**Watch**:
The open-ended observation of a Bucket's changes: each matching Entry is pushed to a Handler (the library's many-times currency), optionally preceded by a replay of current state (`:deliver :latest`, the default), full retained history (`:deliver :history`), or nothing (`:deliver :updates`). The watch handle carries an `:initialized` Promise that resolves when the replay completes — the "cache is warm" signal — and is ended with `stop`, idempotently. Deletions arrive as Tombstone Entries unless ignored.
_Avoid_: watcher (the consumer-side Handler is just a Handler), subscription (reserved for core pub/sub), feed

**Tombstone**:
The marker Entry a KV `delete` writes: the key reads as absent (`get` → `nil`) but its history is retained, and the deletion itself is observable to history and watches as an Entry with a delete `:operation`. Contrast *purge*, which erases the key's history and leaves a single purge marker in its place.
_Avoid_: soft delete, delete marker

**Bucket handle**:
The value `open-bucket` (or `create-bucket`) resolves to: a single Bucket bound for entry operations (get, put, delete, watch, …). Opening verifies the Bucket exists, rejecting early rather than surprising per-op. The handle binds the Bucket's one Codec (the connection's default unless overridden at open): a Bucket's values are homogeneous, never per-operation choices.
_Avoid_: KV instance, bucket reference

### Service

**Service**:
The value `(service/create conn config)` resolves to: a running, queue-subscribed set of request-reply handlers plus the auto-responders the framework registers on the `$SRV.PING|INFO|STATS.*` control subjects. A Service is a pure convenience over core request-reply — no new wire protocol and no server feature — so creating one verifies nothing at entry (unlike a Bucket or JetStream context). The portable surface lives in the `nats-cljc.service` namespace. There is **no** "service context": the server side (`create`) and the client side (Discovery) are independent entry points off a Connection, because the feature has nothing to group-and-verify the way KV/JetStream management does.
_Avoid_: micro, microservice (the framework is "services"/"micro"; the noun a consumer holds is one Service), server

**Endpoint**:
A named, subject-addressed request-reply handler within a Service — the unit a client actually calls. Declared as data in the create config: `{:name :subject :handler :queue-group :metadata}`, where `:subject` defaults to `:name` when omitted. Each Endpoint carries its own request/error counters and processing-time stats, reported through the Service's INFO/STATS responders. The library exposes no native "Group" subject-prefix namespace: a consumer composes a grouped subject directly (the Subject builder helps), because a Group puts nothing distinct on the wire — it is pure construction-side sugar.
_Avoid_: route, method, operation, group

**Discovery**:
The client-side query of running Services over the `$SRV.PING|INFO|STATS` control subjects — the counterpart to hosting a Service. Exposed as three functions on a Connection — `(service/ping conn opts?)`, `(service/info conn opts?)`, `(service/stats conn opts?)` — each a bounded fan-out that gathers responses (until `:max-results` or `:timeout-ms`) and resolves a **Promise of a vector** of normalized maps; `opts` narrows by `:name`/`:id`. There is no separate local introspection of a Service you host: asking your own Service is the same wire request, narrowed by its `:name`/`:id`. There is no Discovery handle — the query is stateless.
_Avoid_: client, registry, lookup (the act is discovering Services; a "service client" is the native object we don't surface)
