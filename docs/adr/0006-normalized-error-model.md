# Normalized error model

Failures surface as an **`ex-info`** carrying a canonical **`:type`** keyword plus structured `ex-data`, identical in shape on the JVM and ClojureScript. Portable code inspects `(:type (ex-data e))` instead of branching on `Throwable` vs `js/Error`; native exceptions are normalized into this representation. There are two channels:

- **One-shot operations** (`connect`, `request`, `flush`, `drain`, `close`) **reject their promise** with such an `ex-info`.
- **Async failures with no call to reject** reach a sink, routed by origin:

  | Failure | Channel | normalized `:type` |
  |---|---|---|
  | Throwing handler (caught, so it never kills the dispatch loop) | sub `:on-error` if set, else `:on-status` `:error` | *none* — the raw thrown value, passed through unchanged |
  | Decode failure (the handler is never called with garbage) | sub `:on-error` if set, else `:on-status` `:error` | `:codec-error` |
  | `:slow-consumer` | sub `:on-error` **only** (dropped if unset) | `:slow-consumer` |
  | `:permissions-violation`, `:protocol-error` | `:on-status` `:error` **only** | as named |

  The per-sub override is **strict**: when a subscription sets `:on-error`, only it fires for that sub's handler/decode failures — never both it and `:on-status`. `:slow-consumer` is inherently a property of one subscription, so it never reaches the connection-level `:on-status`. `:permissions-violation` and `:protocol-error` are connection-level: jnats' `ErrorListener` hands us no subscription identity and nats.js' `permissionContext` does not map cleanly back to our subscription record, so they stay at `:on-status` and carry no per-sub override. A thrown handler value carries no canonical `:type` because it is the consumer's own exception, not a normalized NATS failure.

  **Subscription liveness after a `:permissions-violation` diverges, and is accepted.** nats.js treats a subscription permission error as *terminal for that subscription*: it stops the subscription's async iterable, so our `consume!` loop ends and `(active? sub)` becomes false. jnats only fires the connection-level `errorOccurred` string — `NatsConnection.processError` never touches the subscription set — so the `Subscription` stays live and `(active? sub)` stays true (it silently receives nothing). Both legs still surface the error at `:on-status` as `:permissions-violation`, so only `active?` polling differs. Normalizing it would require parsing the subject out of jnats' `"Permissions Violation for Subscription to '…'"` string and matching it against our own subscription registry — brittle, and fighting the native consumption machinery (ADR 0007) for marginal value; jnats exposes no subscription identity on this path (nor does nats.go, which behaves identically), so there is nothing upstream to lean on.

  **Shape of the two sinks.** `:on-error` receives the **bare ex-info** (portable code reads `(:type (ex-data e))` exactly as on the one-shot reject path). `:on-status` receives a map keyed by a status-vocabulary `:type`; lifecycle events are bare `{:type ...}`, and the `:error` event is the lone exception, wrapping the offending ex-info under `:error` (`{:type :error :error <ex-info>}`) so dispatch on `(:type ev)` stays uniform.

`request` distinguishes **`:timeout`** (responders exist, none answered in time) from **`:no-responders`** (NATS 503 — nobody subscribed); both reject rather than resolving to `nil`.

Canonical `:type`s: `:timeout`, `:no-responders`, `:connect-failed`, `:connection-closed`, `:permissions-violation`, `:codec-error`, `:max-payload-exceeded`, `:protocol-error`, `:drained`, `:slow-consumer`, `:auth-invalid`.

This set is the **normalized NATS error model** — operational failures, each normalized from a native exception. **Caller-misuse validation errors** (`:invalid-header`, `:invalid-max`, `:invalid-max-pending`, `:no-reply-subject`, `:invalid-capacity`) are deliberately **not** here: they are a separate **Validation error** category — programmer errors raised on the operation's own channel before any native call and never to a sink — documented in ADR 0015. `:auth-invalid` stays here, not there, because it is operational (bad credentials are a runtime condition) and validates against the NATS security model rather than API argument well-formedness.

`:drained` is yielded by one-shot operations (`request`, `flush`, `drain`, `close`) on a draining/drained connection; **`publish` never yields it** — a publish during drain is best-effort (`nil`) and a publish once closed is `:connection-closed` (ADR 0014).

`:auth-invalid` names **client-side credential validation** failing before any dial — an nkey/seed mismatch today, and the home for future creds/jwt pre-flight checks — as opposed to `:connect-failed`, which is the server-side connect attempt failing. Validation runs while building connect options, so it surfaces by **rejecting the `connect` promise** (the one-shot channel), not via an async sink.

## Status events: shape, not cadence

The same normalization applies to connection-lifecycle **status events** delivered to `:on-status` (canonical `:type`s in CONTEXT.md). What "identical in shape" guarantees there is deliberately narrower than for errors: each delivered lifecycle event is a bare `{:type ...}` map drawn from the canonical set (the `:error` event excepted — it carries the offending ex-info under `:error`, per the routing table above), but the **count, ordering, and trigger conditions are not normalized** — they follow each underlying client's native reconnect/gossip strategy. We normalize the vocabulary, not the cadence, because the alternative (collapsing or synthesizing events to make the streams byte-identical) means permanently re-implementing two clients' internal loops, and the events carry no payload a consumer could reconcile anyway (they are bare `{:type ...}`).

Known divergences, accepted under this decision:

- **`:reconnecting` count.** A single connection loss yields exactly one `:reconnecting` on the JVM (jnats fires no native reconnecting event, so the listener synthesizes one after `DISCONNECTED`), but one per dial attempt on Node/browser (nats.js dispatches `reconnecting` inside its dial loop).
- **`:servers-changed` conditions.** jnats fires `DISCOVERED_SERVERS` only when genuinely new servers are gossiped; nats.js fires `Events.Update` on essentially every server INFO, including unchanged membership.
- **Default retry count.** With `:reconnect` `:max` absent, each client keeps its own default (JVM 60, Node/browser 10).

Portable consumers treat each `:type` as an edge to react to (dedup if needed), not a counter to compare across platforms.

## Considered options

- **Pass native exceptions through unchanged** — rejected: `Throwable` vs `js/Error` forces host-specific branching in consumer code, defeating write-once-run-both.
- **Return `nil` for no-responders / timeout** — rejected: conflates two distinct failures and discards the `:type`.

## Consequences

- Consumers write a single error-handling path across platforms.
- The canonical `:type` set is part of the public contract and must be maintained as the underlying clients evolve their own error reporting.
