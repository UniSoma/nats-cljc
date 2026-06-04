# Graceful auto-unsubscribe evicts the oldest message on a full buffer

When a pull subscription reaches its armed `max` (ADR 0012's `[sub max]` arity), the JVM-only blocking layer ends it *gracefully* — the N already-buffered messages must still reach the `take-message` consumer, so it appends the end-of-stream sentinel at the tail rather than clearing the buffer the way abrupt `unsubscribe` does. But seating that sentinel in a **bounded buffer that is full with no consumer draining it** is impossible to do while *also* (a) never blocking and (b) never dropping: into a full queue, one of the three has to give. ADR 0008 makes `unsubscribe` return `nil` synchronously and ADR 0012 reinforces that teardown completes *now*, so blocking is off the table; we **drop instead**. The graceful auto-end seats the sentinel with the same non-blocking evict-offer loop abrupt `poison!` uses (`while (not (.offer poison)) (.poll)`) **minus the leading `.clear`**: when the buffer has room — the normal case — the sentinel lands at the tail after the N and the consumer sees **exactly N, in order**; when the buffer is full it evicts the **oldest** message (the head) to make room, guaranteeing the call returns and the jnats dispatcher thread is freed rather than pinned.

The relaxation is fenced by a precondition the caller controls:

> **`capacity > max` ⟹ exactly-N is delivered, unconditionally** — even with zero consumption, because the buffer can then never be full at end-time.

Capacity defaults to **1024**, so this holds automatically for any realistic `max` (request-reply `max=1`, bounded sampling, small scatter-gather counts). You fall into best-effort — the most-recent `≤N` delivered, in order, then end-of-stream — only by *deliberately* sizing `:capacity` at or below your own `max` **and** starving the consumer, which contradicts the feature's purpose (you armed `max` because you want those messages, so you read them). The server-side stop-at-N (the native auto-unsubscribe) is unaffected and always honored regardless of buffer state.

## Considered options

- **Derive end-of-stream from `ended ∧ empty`** (offer the sentinel non-blocking; if it doesn't fit, don't block *or* drop — let the consumer terminate once it drains the buffer and finds the sub ended). Rejected: it preserves exactly-N with no precondition, but it **bifurcates the end-of-stream model** — today there is one notion (the sentinel sits in the queue), and this adds a second (ended-and-empty) that must agree with it forever — and it pushes a recheck loop into `take-message`'s blocking hot path. Too much surface and a standing two-invariant burden for an acceptance criterion that only requires *no hang*.
- **Reserve one physical slot** (allocate the queue at `capacity + 1` and backpressure the producer on logical size, so the sentinel always fits). Rejected: it would upgrade the guarantee from `capacity > max` to `capacity ≥ max` with no loss, but it moves the producer's backpressure check from "`.offer` returned false" to a size snapshot, introducing a TOCTOU race — a bigger rethink than the bug warranted.

## Consequences

- **Abrupt vs. graceful teardown still split as ADR 0008 describes.** Abrupt `unsubscribe`/`close` clear and poison (drop the whole backlog by design); graceful `drain` of a pull sub still blocks on `.put` (documented: it requires a concurrent consumer). Only graceful **auto-unsubscribe** takes this evict-to-seat path — it is the one teardown that must be both graceful *and* synchronous.
- Under a tight producer race the loop may evict at most ~1 beyond the head before the sentinel seats, since the producer stops once `ended` is set (the same convergence argument as `poison!`).
- The `capacity > max` precondition is documented on `subscribe` (`:capacity`) and `unsubscribe` (`max`); the CONTEXT glossary is unchanged (this is a delivery contract, not a term definition).
