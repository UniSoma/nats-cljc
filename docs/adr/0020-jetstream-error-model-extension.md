# JetStream error model: server-rejected configs are operational; side-band conditions route like slow-consumer

JetStream extends the normalized error model (ADR 0006) and the validation category (ADR 0015) with JetStream-specific `:type`s, normalized from the JetStream API error codes — which are identical numbers on both legs, so normalization is a shared lookup table, not per-leg branching. Two judgment calls in that extension are worth recording, because each is a place a future reader would expect the opposite.

## Server-rejected configs are operational, not validation

A malformed JetStream config the server rejects (err_code 10003 / HTTP 400 — e.g. an illegal subject overlap or an invalid retention combination) is caller misuse, which would seem to belong with the Validation errors of ADR 0015. It does not. ADR 0015's defining, testable line is that a validation error is "raised on our own channel **before any native call**." A server-rejected config is detected by the server, *after* the native call, and is normalized from a native exception — so it fails that test and is **operational** (ADR 0006), surfaced as `:jetstream-api-error` carrying `{:code :description}`.

The split is therefore "who detects it." What we can check pre-flight stays validation: `:invalid-name` (a malformed stream/consumer name), `:unknown-config-key` (an unrecognized config key), `:reserved-header` (a reserved `Nats-*` header set directly in a publish's `:headers` instead of via `:msg-id`/`:expect`). What only the server can reject is operational. Keeping the line at "before any native call" preserves ADR 0015's clean, testable defining property instead of redrawing it around "caller-misuse-ness," which is not testable.

## Side-band consume conditions route like slow-consumer

On CLJS, consume-time runtime conditions (`heartbeats_missed`, `consumer_deleted`, `stream_not_found`, `exceeded_limits`) are emitted on a separate `status()` async-iterable, not thrown; on the JVM they surface via exceptions / status on the poll. Normalized, they become operational `:type`s `:heartbeats-missed`, `:consumer-deleted`, `:exceeded-limits` (a backing-stream loss reuses `:stream-not-found`). They are **inherently per-consume**, structurally identical to how `:slow-consumer` is inherently per-subscription — so they follow that exact row of ADR 0006's routing table: delivered to the per-consume **`:on-error` only**, dropped if unset, never to the connection `:on-status`, never both. Terminal conditions (the consumer or its backing stream is gone) additionally end the consume — the returned handle completes. This applies the slow-consumer precedent; it is not a new rule.

## The new `:type`s

Operational (ADR 0006), rejecting the relevant operation's promise:

| Condition | err_code | `:type` |
|---|---|---|
| JetStream not enabled on server/account | 10039 | `:jetstream-not-enabled` (rejects the `(jetstream conn)` handle — ADR 0017) |
| Stream not found | 10059 | `:stream-not-found` |
| Consumer not found | 10014 | `:consumer-not-found` |
| Wrong last sequence (optimistic-concurrency / dedup `:expect` rejection) | 10071 / 10164 | `:wrong-last-sequence` |
| Any other JetStream API error (incl. server-rejected configs) | — | `:jetstream-api-error` (carries `{:code :description}`) |

Operational, side-band, routed to a consume's `:on-error` only: `:heartbeats-missed`, `:consumer-deleted`, `:exceeded-limits`.

Validation (ADR 0015), raised pre-flight on the operation's own channel: `:invalid-name`, `:unknown-config-key`, `:reserved-header`, and the normalized nats.js `InvalidArgument` / `InvalidOperation` family (e.g. binding an ordered consumer).

`:no-message-found` (10037) is **deferred**: its only producer is get-message / direct-get, which is out of Phase 2 scope, and pull `next` returns `nil` on an empty consumer rather than raising. The `:type` lands with direct-get.

## Consequences

- ADR 0006's canonical set gains the JetStream operational `:type`s; ADR 0015's open set gains the JetStream validation `:type`s; CONTEXT.md's Error and Validation-error terms enumerate both. Per ADR 0009, adding vocabulary members is a minor bump.
- Error normalization is a shared err_code → `:type` table (identical numbers on both legs), not per-leg exception branching.
- Server-rejected configs get no pre-flight guard beyond name and key checks; callers handle them as operational `:jetstream-api-error`.
