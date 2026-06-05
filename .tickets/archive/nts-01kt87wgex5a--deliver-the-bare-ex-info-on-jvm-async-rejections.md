---
id: nts-01kt87wgex5a
title: Deliver the bare ex-info on JVM async rejections
status: closed
type: bug
priority: 2
mode: afk
created: '2026-06-04T02:37:34.045038123Z'
updated: '2026-06-05T00:32:57.598868908Z'
closed: '2026-06-05T00:29:14.792771292Z'
tags:
- review
- error
acceptance:
- title: A portable catch reading `(:type (ex-data e))` on a JVM request timeout yields `:timeout`, not nil
  done: true
- title: A JVM `:connect-failed` rejection delivers the bare ex-info with its `:type` readable via `(ex-data e)`
  done: true
- title: clj-kondo clean; suite green on JVM and Node
  done: true
links:
- nts-01kt87wg3rsd
---

## Description

JVM async rejections (request's BiFunction throw, connect's `:connect-failed`) surface through `.handle`/`.thenApply`/`.exceptionally` wrapped in a `CompletionException` whose own ex-data is nil, while JS delivers the bare ex-info. CONTEXT's Error contract says portable code reads `(:type (ex-data e))` on the rejection — true on JS, nil on a direct JVM async handler (the real ex-info hides under `.getCause`).

Unwrap `CompletionException` (and `ExecutionException`) at the JVM async-reject seam so a direct `.handle`/`.exceptionally`/promesa consumer sees the bare ex-info, matching JS and ADR 0006.

Coordinates with the request-hardening ticket (linked) — both touch JVM `-request`.

## Notes

**2026-06-05T00:29:14.792771292Z**

Unwrapped the CompletionException at the JVM async-reject seam so a raw .handle/.exceptionally/.whenComplete consumer reads (:type (ex-data e)) directly, matching JS and ADR 0006. Root cause was narrower than the ticket framed: promesa already peels the wrapper and deref's ExecutionException is inherent to .get (handled by the blocking layer) — the genuine bug was only the raw non-blocking seam. Fix (pure CompletableFuture interop, no promesa in lib): added deliver-bare, which re-completes a rejection via completeExceptionally(cause) (stores the cause unwrapped, unlike a thrown value the JDK wraps), applied to then/bind (covers request) and the connect future; generalized unwrap-completion to also peel ExecutionException. Tests: converted the JVM arms of request-no-responders/timeout-rejects and connect-failed-rejects from deref+.getCause (robust to the bug) to read the bare ex-info at .whenComplete, so they RED-before/GREEN-after and genuinely guard the seam. Verified: clj-kondo 0/0, JVM 94 tests/200 assertions, Node 71/128, all green. Perf: ~40ns/request (0.044% of a localhost round-trip), zero on the subscription delivery path.
