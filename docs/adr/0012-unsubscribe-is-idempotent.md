# Unsubscribe is idempotent

`unsubscribe` ends a single subscription **abruptly** — it tells the server to stop and drops any not-yet-delivered messages, returning `nil` synchronously — as the lower-level sibling to `drain` (which delivers buffered messages first and is awaitable; ADR 0002). Both clients name and split the pair this way; what they *don't* agree on is calling it twice or after the subscription is already gone.

We normalize that to an **idempotent no-op**: unsubscribing an already-ended subscription — whether ended by a prior `unsubscribe`, a `drain`, or the connection closing — returns `nil` and does nothing. nats-core already behaves this way (a closed subscription's `unsubscribe()` is a no-op); jnats throws `IllegalStateException` (its `Dispatcher.unsubscribe` rejects a closed dispatcher or an already-removed subscription), which the JVM impl swallows. Same "normalize shape, not cadence" stance as ADR 0006.

**Consequence — deliberately less strict than `publish`.** A `publish` after close rejects with `:connection-closed`, but an `unsubscribe` after close is a silent no-op. The asymmetry is intentional: `publish` is an action whose outcome the caller depends on, whereas `unsubscribe` is teardown, where "already stopped" *is* success — and making it strict would force racy `(when (active? sub) …)` guards at every call site. The `max` arity is unaffected: reaching N lifetime messages is the normal, non-error path to the same ended state.

## Considered options

- **Surface a typed error** (`:type :inactive-subscription` / `:connection-closed`) instead of swallowing. Rejected: it makes teardown fail on the exact "already torn down" case where the caller's intent is already satisfied, and pushes defensive liveness checks (themselves racy) to every call site.
