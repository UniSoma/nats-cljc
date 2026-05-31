# Handler delivery semantics

A `.cljc` handler runs on two very different execution models — `jnats` delivers on dispatcher threads, `nats.js` delivers by looping an async iterable on the single-threaded event loop. To keep write-once-run-both honest, the delivery contract is the strongest guarantee true on both:

- **Within one subscription:** handler invocations are **serial** — each completes before the next begins. Message *order* is per-publisher: messages from any single publisher arrive in that publisher's publish order (core NATS guarantees this; it does **not** guarantee order across different publishers). So with one publisher a subscription sees a single ordered stream; with several concurrent publishers to the same subject, each publisher's messages keep their relative order but the streams may interleave arbitrarily.
- **Across subscriptions:** **no** ordering or concurrency guarantee. On the JVM, different subscriptions may run in parallel; on ClojureScript they interleave cooperatively on the event loop.
- **Handlers must never block synchronously.** That is fatal on ClojureScript (it freezes the event loop) and on the JVM stalls the subscription's dispatcher and trips `:slow-consumer`.
- **A handler may return a promise.** If it does, the subscription waits for that promise to settle before delivering the next message — per-subscription backpressure with no core.async. Returning a non-promise delivers the next message immediately.

Overflow surfaces as a `:slow-consumer` status event when undelivered messages for a subscription cross an optional `:max-pending` threshold. What `:max-pending` guarantees portably is the **signal**, not a hard heap cap: on the JVM jnats additionally **drops** over-limit messages from its dispatcher queue (and is bounded by default at 512K msgs / 64 MB even with `:max-pending` absent); on ClojureScript nats-core's buffer is unbounded and `:max-pending` only sets the notification threshold (`slow?`) — it does not auto-drop, so the consumer reacts to `:slow-consumer` (drain, unsubscribe, or slow the source). This is an accepted "shape, not cadence" divergence (ADR 0006): the *event* is portable, the *drop* is native.

## Considered options

- **Promise cross-subscription parallelism** — rejected: ClojureScript's single thread cannot honor it, so it would be a portability lie.
- **Strictly fire-and-forget handlers, all backpressure deferred to the future core.async/missionary adapters** — rejected: promise-return backpressure is a cheap, portable win available immediately, and it is the natural answer to "don't block — return a promise instead."

## Consequences

- Portable processing loops can rely on per-subscription ordering, but must not assume any cross-subscription ordering.
- Consumers express async per-message work as a returned promise, never as a blocking call.
