# JVM-only blocking convenience layer

The portable core is non-blocking on every platform (ADR 0001, ADR 0002). For JVM callers who want synchronous ergonomics, a **JVM-only (`.clj`) blocking layer** sits on top, mirroring the async namespace tree as a **parallel subtree**: `nats-cljc.blocking.core` now, and `nats-cljc.blocking.jetstream` / `nats-cljc.blocking.kv` / `nats-cljc.blocking.object` alongside their async twins later. Each blocking namespace uses the **same verb names** as its async counterpart, so a caller switches semantics by swapping a single require (`nats-cljc.core` → `nats-cljc.blocking.core`).

- **One-shots** deref the underlying promise, unwrapping so they throw the canonical `ex-info` directly (not a wrapping `ExecutionException`). Already-synchronous ops (`publish`, `unsubscribe`) are re-exported unchanged.
- **Subscriptions** get a pull model the portable core structurally cannot offer: `subscribe` returns a handle, and `(take-message handle timeout?)` **blocks the calling thread** for the next message (or a timeout/closed sentinel). It is backed by a bounded `BlockingQueue` fed by the async handler; the bound supplies backpressure via the promise-returning-handler mechanism (ADR 0007). An optional `messages` reducible/seq supports `doseq`/`reduce`.

It **ships just after the core** — the core's protocol already supports it, and the non-blocking core is the priority.

## Considered options

- **Flat `nats-cljc.blocking` namespace** — rejected: Phase-2/3 verbs collide (core `publish` vs JetStream `publish`; KV/Object `get`/`put`), exactly the collision the async side avoids by splitting namespaces. A parallel subtree is required.
- **Callback subscriptions in the blocking layer** — rejected: then "blocking" would buy subscriptions nothing; the synchronous pull loop is the whole point.

## Consequences

- The pull API is JVM-only by construction and has no ClojureScript counterpart — intended, and the reason this layer exists.
- **Overflow is backpressure, never drop.** A full queue blocks the feeding dispatcher thread on `BlockingQueue.put`. Under Road 2 (ADR 0007) that thread is already blocked on the handler's `CompletionStage`, so this adds no thread-pinning the core doesn't already have — and backlog builds in jnats' own dispatcher queue, keeping its native slow-consumer ceiling (512K msgs / 64 MB) as the ultimate floor. Capacity defaults to **1024**, overridable per-subscription via `:capacity` (a non-positive value throws `:invalid-capacity`); there is no unbounded escape hatch.
- **`take-message` returns `nil` for both timeout and end-of-stream**, disambiguated by `active?` (false once the sub has ended *and* the buffer is drained). A poison-pill enqueued on teardown wakes any blocked `take-message`. The ergonomic path is `messages`, an `IReduceInit` that terminates on its own.
- **Abrupt vs. graceful teardown mirrors the core's `unsubscribe`/`drain` split.** `unsubscribe` (and connection `close`) clears and poisons the queue — which also unblocks any producer parked in `.put`, so teardown always makes progress and leaks no dispatcher thread. Graceful `drain` of a pull sub flushes the buffer and therefore requires concurrent consumption.
