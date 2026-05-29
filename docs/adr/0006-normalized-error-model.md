# Normalized error model

Failures surface as an **`ex-info`** carrying a canonical **`:type`** keyword plus structured `ex-data`, identical in shape on the JVM and ClojureScript. Portable code inspects `(:type (ex-data e))` instead of branching on `Throwable` vs `js/Error`; native exceptions are normalized into this representation. There are two channels:

- **One-shot operations** (`connect`, `request`, `flush`, `drain`, `close`) **reject their promise** with such an `ex-info`.
- **Async failures with no call to reject** — a throwing handler (caught, so it never kills the dispatch loop), a **decode failure** (the handler is not called with garbage), a slow consumer, a protocol error — reach the connection's **`:on-status` `:error`** sink, with an optional per-subscription **`:on-error`** override.

`request` distinguishes **`:timeout`** (responders exist, none answered in time) from **`:no-responders`** (NATS 503 — nobody subscribed); both reject rather than resolving to `nil`.

Canonical `:type`s: `:timeout`, `:no-responders`, `:connect-failed`, `:connection-closed`, `:permissions-violation`, `:codec-error`, `:max-payload-exceeded`, `:protocol-error`, `:drained`.

## Considered options

- **Pass native exceptions through unchanged** — rejected: `Throwable` vs `js/Error` forces host-specific branching in consumer code, defeating write-once-run-both.
- **Return `nil` for no-responders / timeout** — rejected: conflates two distinct failures and discards the `:type`.

## Consequences

- Consumers write a single error-handling path across platforms.
- The canonical `:type` set is part of the public contract and must be maintained as the underlying clients evolve their own error reporting.
