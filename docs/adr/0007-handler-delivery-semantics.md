# Handler delivery semantics

A `.cljc` handler runs on two very different execution models — `jnats` delivers on dispatcher threads, `nats.js` delivers by looping an async iterable on the single-threaded event loop. To keep write-once-run-both honest, the delivery contract is the strongest guarantee true on both:

- **Within one subscription:** messages are delivered **in order and serially** — each handler invocation completes before the next begins.
- **Across subscriptions:** **no** ordering or concurrency guarantee. On the JVM, different subscriptions may run in parallel; on ClojureScript they interleave cooperatively on the event loop.
- **Handlers must never block synchronously.** That is fatal on ClojureScript (it freezes the event loop) and on the JVM stalls the subscription's dispatcher and trips `:slow-consumer`.
- **A handler may return a promise.** If it does, the subscription waits for that promise to settle before delivering the next message — per-subscription backpressure with no core.async. Returning a non-promise delivers the next message immediately.

Overflow surfaces as a `:slow-consumer` status event; an optional `:max-pending` subscribe option bounds buffering.

## Considered options

- **Promise cross-subscription parallelism** — rejected: ClojureScript's single thread cannot honor it, so it would be a portability lie.
- **Strictly fire-and-forget handlers, all backpressure deferred to the future core.async/missionary adapters** — rejected: promise-return backpressure is a cheap, portable win available immediately, and it is the natural answer to "don't block — return a promise instead."

## Consequences

- Portable processing loops can rely on per-subscription ordering, but must not assume any cross-subscription ordering.
- Consumers express async per-message work as a returned promise, never as a blocking call.
