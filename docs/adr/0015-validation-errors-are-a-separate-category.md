# Caller-misuse validation errors are a separate category

The library throws five **validation** `:type`s — `:invalid-header`, `:invalid-max`, `:invalid-max-pending`, `:no-reply-subject` (core), and `:invalid-capacity` (blocking) — that are deliberately **not** in the canonical NATS error set of ADR 0006. They form a separate **Validation error** category, sharing only the *shape* (`ex-info` + `:type`) with the normalized NATS errors and nothing else. The split tracks a real distinction: the canonical set is **operational** — runtime conditions even bug-free code must handle (`:timeout`, `:connection-closed`, `:slow-consumer`) — while these five are **programmer errors**, caller misuse whose fix is to change the call, not branch on it at runtime. Folding them into the canonical set would dilute ADR 0006's thesis (it normalizes *native exceptions* into a portable representation; these are normalized from nothing — they are our own pre-flight guards) and falsify its stated consequence that the set "must be maintained as the underlying clients evolve their error reporting" — our guards track no client's error reporting.

A Validation error is defined by three guarantees, not by a sync/async axis (that axis does **not** separate the categories — `connect` rejects its promise with the operational `:connect-failed` *and* with the validation `:invalid-max`):

1. **Raised through the operation's own channel.** A synchronous operation **throws** it; a promise-returning operation that validates (`connect`, `service/create`) **rejects its promise** with it. The contract promises neither "always throws" nor "always rejects" — it promises the operation's own return convention.
2. **Before any native NATS call** — fail-fast on the argument; nothing reaches jnats/nats.js.
3. **Never an async sink.** This is *the* distinguishing, testable line: a Validation error never reaches `:on-error` or `:on-status`. The sinks are exclusively the NATS error model's channel — a NATS error *can* go to a sink; a Validation error never does.

| Validation `:type` | Operation | Channel |
|---|---|---|
| `:invalid-header` | `publish` (fire-and-forget, returns nil) | synchronous throw |
| `:invalid-max-pending` | `subscribe` | synchronous throw |
| `:no-reply-subject` | `reply` | synchronous throw |
| `:invalid-max` | `unsubscribe`, blocking `subscribe` | synchronous throw |
| `:invalid-capacity` | blocking `subscribe` | synchronous throw |
| `:invalid-max` | `connect` (guard runs inside connect's supplier) | promise rejection |

The set is **enumerated but open, and diagnostic-first**. Unlike the canonical NATS set — a stable contract consumers dispatch on in production — a validation `:type` is primarily diagnostic: it tells the developer *what* they got wrong. The five are enumerated (the test suite asserts exact keywords, so they must be named), individual members stay stable once shipped, but the *set is additive*: new guards will add new validation `:type`s — two landed in the tickets that prompted this ADR — and that is not the breaking change mutating the canonical set would be. Consumers should treat an unrecognized validation `:type` as their own misuse to fix, not assume the set exhaustive for production dispatch.

`:auth-invalid` **stays in the canonical set** despite also being a client-side pre-dial check (an nkey/seed mismatch caught before any dial, rejecting the `connect` promise — structurally identical to `:invalid-max` via `connect`). The line: `:auth-invalid` is *operational* (bad credentials from config/env are a runtime condition you handle — surface "auth failed", prompt re-auth) and it validates a value against the *NATS security model*, standing in for what the server would reject. The five validate *argument well-formedness against our own API contract* and have no runtime recovery. The line is fine but real, and this paragraph is why a future reader finds `:auth-invalid` in 0006 but `:invalid-max` here.

## Considered options

- **Extend the canonical `:type` set with the five.** Rejected: dilutes ADR 0006's normalized-from-native thesis, falsifies its "maintain as clients evolve" consequence (our guards track no client), and conflates programmer errors with operational conditions — erasing the very distinction (different handling discipline) that makes the error model useful. The canonical set would also lose its stability, since validation guards are the part most likely to grow.
- **A programmatic `:category` marker in `ex-data`** (e.g. `{:type :invalid-header :category :validation}`), so a consumer can ask "is this *any* validation error?" generically. Rejected: the canonical set carries no marker either — consumers already separate operational types by vocabulary alone — so a marker on only the validation types is asymmetric machinery. To make "is this operational?" answerable it would have to be added symmetrically across the whole error model, a larger decision than this contract. The five already ship as bare keywords; documentation-only keeps `ex-data` untouched and the model consistent.
- **A frozen, closed validation set** (every new guard is a contract change). Rejected: we keep adding guards — two just landed — so freezing would make routine hardening a breaking change. Validation `:type`s are diagnostic, not production dispatch targets, so the weaker "stable members, open set" guarantee is the honest one.

## Consequences

- The canonical NATS `:type` set in ADR 0006 stays stable and meaningful — only operational, normalized-from-native failures, plus `:auth-invalid` (operational auth). It gains a one-line cross-reference pointing here.
- CONTEXT.md gains a **Validation error** glossary term, distinct from **Error**, enumerating the five and characterizing the category (own-channel, never-a-sink, open, diagnostic-first).
- New caller-misuse guards add new validation `:type`s without a contract break; they need only be raised on the operation's own channel, before the native call, and never to a sink.
- A consumer wrapping a single one-shot op (e.g. `request`, which can yield operational `:timeout` and validation `:invalid-header`) that wants to branch generically on operational-vs-programmer must enumerate the validation set itself — there is no marker. Accepted: such generic branching is rare for programmer errors, and a symmetric `:category` axis remains available as a future, separate decision if it proves needed.
