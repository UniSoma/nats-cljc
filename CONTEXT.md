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
A failure surfaced as an `ex-info` carrying a canonical `:type` and structured data, identical in shape on every platform (so portable code reads `(:type (ex-data e))` rather than branching on host exception types). Canonical `:type`s: `:timeout`, `:no-responders`, `:connect-failed`, `:connection-closed`, `:permissions-violation`, `:codec-error`, `:max-payload-exceeded`, `:protocol-error`, `:drained`, `:slow-consumer`, `:auth-invalid`. `:auth-invalid` is client-side credential validation failing *before* any dial (e.g. an nkey that does not match its seed), distinct from `:connect-failed` (the server-side connect attempt failed); it rejects the `connect` promise. One-shot operations reject their promise with it. Async failures reach a sink instead: a throwing handler or a decode failure reaches the subscription's `:on-error` if one is set, else the connection's `:on-status` as an `:error` event; `:slow-consumer` — being per-subscription — reaches the subscription's `:on-error` only (and is silently dropped when none is set); while connection-level `:permissions-violation` and `:protocol-error` reach `:on-status` as an `:error` event only, never a per-subscription override. The override is strict: when a subscription sets `:on-error`, only it fires — never both it and `:on-status`. `:on-error` receives the bare ex-info, so portable code reads `(:type (ex-data e))` exactly as on the one-shot reject path; the connection-level `:error` event wraps that ex-info under `:error` (see Status event). A thrown handler value is passed through unchanged and carries no canonical `:type` — it is the consumer's own exception, not a normalized NATS failure.
_Avoid_: failure, fault (a bare host *exception* is what we normalize *into* an Error)

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

**Queue group**:
A named set of subscriptions to the same subject among which the server load-balances, so each message reaches exactly one member. Selected with the `:queue` option on `subscribe`.
_Avoid_: consumer group, worker pool
