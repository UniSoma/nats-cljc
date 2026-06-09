# `-active?` stays true through the drain window

`Sub`'s `-active?` reports **true from drain-initiation until the drain settles**, on
every implementation — core subscriptions and JetStream consume handles, both legs.
"Still delivering" is read literally: a draining sub is alive until it reaches its
terminal state, and mid-drain the JVM consume handle really is still delivering its
buffered messages after `stop()`. This is also what both native clients naturally
expose for core subscriptions (jnats `isActive`, nats.js `isClosed`), so the consume
handles align to the semantics core gets for free rather than the reverse.

Consequence — a gave-up drain leaves `active?` true. The JVM consume drain is
bounded (ADR 0018's wind-down deadline): when a parked handler keeps the consumer
from ever finishing, `-drain` settles `false` at the deadline and `-active?` stays
`true` indefinitely. That pairing is deliberate: the consumer genuinely never wound
down, the settled-`false` drain already tells the caller "didn't finish within
budget", and escalation (`unsubscribe`, which closes the consumer and flips it
inactive) is the caller's call. The predicate reports the consumer's actual state;
it does not pretend the wind-down succeeded.

## Considered options

- **Inactive at drain-initiation** ("no longer accepting new interest") — what the
  JVM consume handle did via `isStopped`. Rejected: it reports `false` while
  buffered messages are still being delivered to the handler — a lie about "still
  delivering" — and it would force changing core's `Subscription` on both legs (or
  deliberately diverging from it), fighting the liveness semantics the natives
  provide, the same normalization ADR 0006 declined for `:permissions-violation`.
- **Force-flip `active?` when the bounded drain gives up** (e.g. `close()` at the
  deadline). Rejected: it silently converts "bounded give-up" into forced abrupt
  teardown, dropping buffered messages — a materially different action than `drain`
  promised, smuggled in as a side effect.
