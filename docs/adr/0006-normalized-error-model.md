# Normalized error model

Failures surface as an **`ex-info`** carrying a canonical **`:type`** keyword plus structured `ex-data`, identical in shape on the JVM and ClojureScript. Portable code inspects `(:type (ex-data e))` instead of branching on `Throwable` vs `js/Error`; native exceptions are normalized into this representation. There are two channels:

- **One-shot operations** (`connect`, `request`, `flush`, `drain`, `close`) **reject their promise** with such an `ex-info`.
- **Async failures with no call to reject** — a throwing handler (caught, so it never kills the dispatch loop), a **decode failure** (the handler is not called with garbage), a protocol error — reach the connection's **`:on-status` `:error`** sink, with an optional per-subscription **`:on-error`** override. **`:slow-consumer`** is the exception: it is inherently a property of one subscription, so it is delivered **only** to that subscription's `:on-error` (never the connection-level `:on-status`), keeping every `:on-status` event a bare connection-level `{:type ...}`.

`request` distinguishes **`:timeout`** (responders exist, none answered in time) from **`:no-responders`** (NATS 503 — nobody subscribed); both reject rather than resolving to `nil`.

Canonical `:type`s: `:timeout`, `:no-responders`, `:connect-failed`, `:connection-closed`, `:permissions-violation`, `:codec-error`, `:max-payload-exceeded`, `:protocol-error`, `:drained`, `:slow-consumer`, `:auth-invalid`.

`:auth-invalid` names **client-side credential validation** failing before any dial — an nkey/seed mismatch today, and the home for future creds/jwt pre-flight checks — as opposed to `:connect-failed`, which is the server-side connect attempt failing. Validation runs while building connect options, so it surfaces by **rejecting the `connect` promise** (the one-shot channel), not via an async sink.

## Status events: shape, not cadence

The same normalization applies to connection-lifecycle **status events** delivered to `:on-status` (canonical `:type`s in CONTEXT.md). What "identical in shape" guarantees there is deliberately narrower than for errors: each delivered event is a bare `{:type ...}` map drawn from the canonical set, but the **count, ordering, and trigger conditions are not normalized** — they follow each underlying client's native reconnect/gossip strategy. We normalize the vocabulary, not the cadence, because the alternative (collapsing or synthesizing events to make the streams byte-identical) means permanently re-implementing two clients' internal loops, and the events carry no payload a consumer could reconcile anyway (they are bare `{:type ...}`).

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
