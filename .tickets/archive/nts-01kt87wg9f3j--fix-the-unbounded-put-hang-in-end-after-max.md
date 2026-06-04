---
id: nts-01kt87wg9f3j
title: Fix the unbounded `.put` hang in `end-after-max!`
status: closed
type: bug
priority: 1
mode: afk
created: '2026-06-04T02:37:33.871589233Z'
updated: '2026-06-04T19:50:24.006015782Z'
closed: '2026-06-04T19:50:24.006015782Z'
tags:
- review
- blocking
acceptance:
- title: '`(unsubscribe sub max)` on a full buffer with no consumer returns nil synchronously, no hang'
  done: true
- title: The handler-path max on a full buffer does not block or leak the dispatcher thread
  done: true
- title: A regression test reproduces the original hang and passes after the fix
  done: true
- title: clj-kondo clean; JVM suite green (blocking layer is JVM-only)
  done: true
---

## Description

Reproduced on a live server. In the JVM-only blocking layer, `end-after-max!` ends a pull subscription with an unbounded `(.put queue poison)` — no clear-first — unlike the abrupt `poison!` and the bounded `.offer` enqueue loop. `.put` blocks when the buffer is full, so the graceful-max path can:

(a) hang its `unsubscribe` caller indefinitely instead of returning nil synchronously (ADR 0008), and
(b) pin/leak the jnats dispatcher thread when the handler hits the max on a full buffer.

Make `end-after-max!` clear-first then enqueue the poison without blocking, matching the abrupt path. Add a regression test reproducing the hang (capacity 2, full buffer, no consumer; `(unsubscribe sub 2)` must return nil promptly).

## Notes

**2026-06-04T19:50:24.006015782Z**

Replaced the unbounded (.put queue poison) in end-after-max! with a non-blocking evict-offer loop, extracted as poison-tail! and shared with abrupt poison! (= .clear + poison-tail!). On a full buffer the graceful auto-end now evicts the oldest to seat the end-of-stream sentinel instead of blocking, so (unsubscribe sub max) stays synchronous (ADR 0008/0012) and the handler path never pins the jnats dispatcher. Exactly-N delivery is guaranteed when :capacity > max (documented on subscribe/unsubscribe); below that it degrades to best-effort, recorded in new ADR 0013 (rejected: derive-end-of-stream, reserved-slot). Two RED->GREEN regression tests (unsubscribe-path + handler-path), designed to fail-loud not hang. JVM 89 tests/195 assertions, 0 fail; clj-kondo clean.
