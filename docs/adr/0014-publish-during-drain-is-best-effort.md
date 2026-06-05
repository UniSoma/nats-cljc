# Publish during drain is best-effort

A `publish` issued on a **draining** connection is allowed and best-effort — it returns `nil` like any other fire-and-forget publish — and a `publish` on a **closed** connection (whether closed by `drain` completing or by `close`) throws the retry-able `:connection-closed`; `publish` **never** yields `:drained`. The two legs reached this divergently and we converged on jnats' behavior: jnats keeps `getStatus` `CONNECTED` through the drain window and exposes **no draining signal at publish time**, so it simply lets the publish through (returning `nil`) and only throws `IllegalStateException("…Closed")` once the connection is actually closed. nats.js is noisier — a publish lands natively (no throw) in the synchronous tick after `drain()`, throws `DrainingConnectionError` in the microtask window after `noMorePublishing` flips, and `ClosedConnectionError` once closed — and its `isDraining()` latches `true` forever (even after close), so it is **useless as a discriminator**. We therefore key the JS decision on the *error type* (≈ "is the connection actually closed"), not on `isDraining`: `DrainingConnectionError → nil`, `ClosedConnectionError → :connection-closed`. That mirrors what the JVM already does, so the contract holds on both legs with **zero JVM production change**:

| connection state | JVM publish | JS publish | agree? |
|---|---|---|---|
| open | `nil` | `nil` | ✓ |
| drain in flight, not closed | `nil` | `nil` | ✓ |
| closed (drain done **or** `close`) | `:connection-closed` | `:connection-closed` | ✓ |

This contrasts deliberately with **`request`**, which keeps `:drained` on a draining/drained connection (it has a promise to reject, and jnats *does* throw `"Draining"` for a request). So a `publish` and a `request` on the *same* drained-then-closed connection diverge — `:connection-closed` vs `:drained` — a within-leg asymmetry that is **identical on both legs** (which is what portability requires). Forcing publish to also yield `:drained` would require the JVM to carry its own mutable "was-drained" state, since jnats reports only `"Closed"` after drain completes; we rejected that as more machinery than the parity goal needs.

The fix also normalizes the raw-native-error leak both paths had: a publish or request in the nats.js `DrainingConnectionError` microtask window previously fell through to a re-throw of the bare native error with **no canonical `:type`**, violating ADR 0006's "no native exception escapes." Publish now maps that window to `nil`, request to `:drained`. Both microtask branches are **not deterministically testable** (the gap between `noMorePublishing = true` and `close()`, and the suite uses real servers, no mocks), so they are defensive normalization that upholds the ADR 0006 invariant rather than branches a test exercises; the deterministic coverage asserts the *contract* (a publish issued while drain is in flight does not throw; a publish once closed throws `:connection-closed`).

## Considered options

- **Reject a draining publish on both legs (match nats.js, surface `:drained`).** Rejected: `publish` is fire-and-forget (ADR 0002) with no promise to carry a rejection, so it would have to throw *synchronously*; and jnats exposes no draining signal at publish time (`getStatus` stays `CONNECTED`, the publish just succeeds), so the reject direction is not even implementable on the JVM mid-window. The only convergence both native clients can support is allow/best-effort.
- **Key the swallow on `isDraining` — "once drained, every later publish is a silent `nil`" (Design A).** Rejected: because nats.js' `isDraining()` latches `true` after close, this makes a JS publish *after drain completes* return `nil` while the JVM throws `:connection-closed` — re-introducing a cross-leg divergence. Closing that would require adding mutable drained-state to the JVM connection so it also returns `nil`. Keying on the actual close state (this ADR) achieves full parity with neither the divergence nor the extra state.

## Consequences

- Full publish parity across the JVM, browser, and Node as a **JS-only change**; no new state on either leg.
- `publish` is removed from the JS `op-state-error` helper's callers — that helper is now **request-only** (its `isDraining → :drained` branch is exactly the request contract). `publish` builds `:connection-closed` directly.
- A publish that races shutdown can be **silently dropped** with no error once the connection is draining — already true on jnats, now true on every leg. Consumers who need delivery guarantees await `drain`/`flush` before relying on a publish landing.
- ADR 0006 carries a one-line cross-reference noting `publish` never yields `:drained`; the CONTEXT glossary is unchanged (this is a per-operation delivery contract, not a term definition).
