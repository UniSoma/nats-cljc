# JetStream pull delivery reuses the promise-return handler, not a channel/missionary adapter

JetStream pull consumers deliver messages through the **same handler contract as core subscriptions** (ADR 0007): a `(fn [msg] …)` that may return a promise, where the runtime waits for that promise to settle before delivering the next message. That gives per-message backpressure with **no async dependency** — the client's own read rate gates how fast it pulls from the server. The continuous `consume` verb takes this handler and returns a drainable/unsubscribable handle; the bounded `fetch` returns a `Promise<vector>` of messages; the single-shot `next` returns a `Promise<msg-or-nil>`.

This **amends a consequence of ADR 0002**, which predated ADR 0007 and specified that pull consumers would need a channel/missionary adapter "because a bare callback cannot carry backpressure." ADR 0007 then discovered that a *promise-returning* handler carries backpressure perfectly well, and chose it for core subscriptions as "a cheap, portable win available immediately." Phase 2 simply extends that same primitive to JetStream pull. The core.async and missionary adapters remain exactly what ADR 0002 always said they should be — **opt-in sugar layered on top, Phase 3** — never the Phase-2 delivery mechanism.

Forcing the Phase-2 primitive to be a channel/missionary adapter would also contradict the dependency floor: core.async and missionary are non-NATS dependencies, the precise kind the library refuses to force (ADR 0002/0004).

## No slow-consumer in pull

Core push subscriptions can overrun a buffer, so they surface `:slow-consumer` at `:max-pending` (ADR 0007). Pull is structurally different: the client *requests* messages, so a slow handler simply slows the pull — unrequested messages wait on the server, nothing overflows. The refill knobs bound the client-side buffer and the promise-return gates consumption, so `:max-pending` / `:slow-consumer` do not carry over to pull. Flow control is the knobs plus the handler's promise, not a drop-and-signal threshold.

## Refill knobs and the threshold divergence

`consume` / `fetch` take `:batch` (max messages per pull window), `:threshold` (refill when the buffered count drops below it), `:expires-ms`, `:idle-heartbeat-ms`, and `:max-bytes`. The legs disagree on the threshold's unit — the JVM expresses it as a *percent*, nats.js as a *message count*. The portable `:threshold` is a **count** (nats.js-native; the JVM converts count→percent), because "refill when fewer than N remain" is more intuitive and composes with `:batch` directly.

## Consume-time errors route to a per-consume `:on-error`

On CLJS, consume-time runtime conditions (`heartbeats_missed`, `consumer_deleted`, `stream_not_found`, `exceeded_limits`) are emitted on a *separate* `status()` async-iterable — not thrown and not in the message stream — whereas the JVM surfaces them via exceptions / status on the poll. The adapter normalizes both into canonical operational `:type`s and routes them to a **per-consume `:on-error`**, mirroring the per-subscription `:on-error` of core (ADR 0006/0007). The exact `:type` set, and whether these also reach the connection `:on-status`, are settled with the rest of the error model.

## Considered options

- **A channel/missionary adapter as the Phase-2 primitive** (the original ADR 0002 plan). Rejected: it forces a non-NATS async dependency, contradicting the dependency floor, and ADR 0007 already proved a promise-return handler delivers the same backpressure for free. The adapters keep their place as Phase-3 opt-in sugar over the handler.
- **Fire-and-forget handler, all backpressure deferred to the Phase-3 adapters.** Rejected for the same reason ADR 0007 rejected it for core: promise-return backpressure is immediate, portable, and the natural answer to "don't block — return a promise."
- **`fetch` via a handler too**, for symmetry with `consume`. Rejected: `fetch` is a bounded one-shot batch the caller sizes, so a `Promise<vector>` is the honest shape; continuous or large draining is what `consume` is for.

## Consequences

- JetStream pull adds no async dependency; `consume` is the core handler contract with acks and refill knobs layered on.
- ADR 0002's pull consequence is amended in place (with a pointer here); the README roadmap's Phase-2 line changes from "channel/missionary adapter" to the promise-return handler.
- `:max-pending` / `:slow-consumer` are core-push concepts and do not appear in the pull surface.
- The core.async / missionary adapters (Phase 3) are layered on the same handle and handler, not a parallel delivery path.
