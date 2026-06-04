---
id: nts-01kt87wgex5a
title: Deliver the bare ex-info on JVM async rejections
status: open
type: bug
priority: 2
mode: afk
created: '2026-06-04T02:37:34.045038123Z'
updated: '2026-06-04T02:53:24.413864807Z'
tags:
- review
- error
acceptance:
- title: A portable catch reading `(:type (ex-data e))` on a JVM request timeout yields `:timeout`, not nil
  done: false
- title: A JVM `:connect-failed` rejection delivers the bare ex-info with its `:type` readable via `(ex-data e)`
  done: false
- title: clj-kondo clean; suite green on JVM and Node
  done: false
links:
- nts-01kt87wg3rsd
---

## Description

JVM async rejections (request's BiFunction throw, connect's `:connect-failed`) surface through `.handle`/`.thenApply`/`.exceptionally` wrapped in a `CompletionException` whose own ex-data is nil, while JS delivers the bare ex-info. CONTEXT's Error contract says portable code reads `(:type (ex-data e))` on the rejection — true on JS, nil on a direct JVM async handler (the real ex-info hides under `.getCause`).

Unwrap `CompletionException` (and `ExecutionException`) at the JVM async-reject seam so a direct `.handle`/`.exceptionally`/promesa consumer sees the bare ex-info, matching JS and ADR 0006.

Coordinates with the request-hardening ticket (linked) — both touch JVM `-request`.
