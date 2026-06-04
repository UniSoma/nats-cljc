---
id: nts-01kt87wg3rsd
title: Harden the `request` verb
status: open
type: bug
priority: 1
mode: afk
created: '2026-06-04T02:37:33.688031805Z'
updated: '2026-06-04T02:53:24.060080438Z'
tags:
- review
- request
acceptance:
- title: '`(request conn subject data)` issues a request with the 5000 ms default and resolves on both legs'
  done: false
- title: An encode failure (`:bytes` codec on non-bytes) rejects the returned promise with `:codec-error` on both legs, never a synchronous throw
  done: false
- title: An over-max payload request rejects with `{:type :max-payload-exceeded}` on both legs, no raw native throw
  done: false
- title: clj-kondo clean; suite green on JVM and Node
  done: false
links:
- nts-01kt87wgex5a
---

## Description

The portable `request` verb must work end-to-end as documented on both legs (JVM + Node). Three gaps:

(a) The documented 3-arity `(request conn subject data)` does not exist — only the 4-arity does — so the documented call throws `ArityException`. Add it, delegating to the 4-arity with the 5000 ms default.

(b) `request` encodes its payload with `codec/encode` outside the promise chain, so an encode failure escapes as a synchronous `:codec-error` throw instead of a rejected promise. Move the encode inside the promise so failure rejects, matching the reply-decode side.

(c) JVM `-request`'s synchronous-throw guard catches only `IllegalStateException`, so other native sync throws (e.g. an over-max payload's `IllegalArgumentException`) leak raw. Broaden it and normalize an over-max request to `:max-payload-exceeded` on both legs (ADR 0006 — request must reject with a typed ex-info, never throw raw).

Coordinates with the JVM bare-ex-info ticket (linked) — both touch JVM `-request`.
