---
id: nts-01kt87wg9f3j
title: Fix the unbounded `.put` hang in `end-after-max!`
status: open
type: bug
priority: 1
mode: afk
created: '2026-06-04T02:37:33.871589233Z'
updated: '2026-06-04T02:53:24.233319356Z'
tags:
- review
- blocking
acceptance:
- title: '`(unsubscribe sub max)` on a full buffer with no consumer returns nil synchronously, no hang'
  done: false
- title: The handler-path max on a full buffer does not block or leak the dispatcher thread
  done: false
- title: A regression test reproduces the original hang and passes after the fix
  done: false
- title: clj-kondo clean; JVM suite green (blocking layer is JVM-only)
  done: false
---

## Description

Reproduced on a live server. In the JVM-only blocking layer, `end-after-max!` ends a pull subscription with an unbounded `(.put queue poison)` — no clear-first — unlike the abrupt `poison!` and the bounded `.offer` enqueue loop. `.put` blocks when the buffer is full, so the graceful-max path can:

(a) hang its `unsubscribe` caller indefinitely instead of returning nil synchronously (ADR 0008), and
(b) pin/leak the jnats dispatcher thread when the handler hits the max on a full buffer.

Make `end-after-max!` clear-first then enqueue the poison without blocking, matching the abrupt path. Add a regression test reproducing the hang (capacity 2, full buffer, no consumer; `(unsubscribe sub 2)` must return nil promptly).
