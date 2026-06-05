---
id: nts-01kt87wghgg4
title: Guard the Reconnect `:max` Integer cap
status: closed
type: bug
priority: 2
mode: afk
created: '2026-06-04T02:37:34.128196662Z'
updated: '2026-06-05T00:47:45.615506674Z'
closed: '2026-06-05T00:47:45.615506674Z'
tags:
- review
- connect
acceptance:
- title: '`(connect {:reconnect {:max <huge>}})` produces the same typed outcome on both legs, no uncaught ArithmeticException on JVM and no silent accept on JS'
  done: true
- title: In-range `:max` values including the `0` and `-1` sentinels still configure reconnection correctly
  done: true
- title: clj-kondo clean; suite green on JVM and Node
  done: true
links:
- nts-01kt87whe06m
---

## Description

`with-reconnect` calls `(int max)` unguarded inside connect's supplier, so a `:reconnect {:max <beyond Integer range>}` throws an uncaught `ArithmeticException` (integer overflow) on the JVM — completing the connect promise with a raw, untyped exception — while JS accepts it silently.

Apply the same Integer-range guard `core/unsubscribe` already uses for its `max`, rejecting an out-of-range `:reconnect :max` with a typed validation error on both legs (following the unsubscribe-max precedent). Both legs must agree.

## Notes

**2026-06-05T00:47:45.615506674Z**

Guarded :reconnect {:max} against the JVM Integer cap. Both with-reconnect seams (jvm.clj / js.cljs) now reject a max outside [-1, 2147483647] (or a non-integer) with a typed {:type :invalid-max} ex-info before the native call, mirroring core/unsubscribe — so a huge max rejects connect identically on both legs (was a raw, untyped ArithmeticException on the JVM and a silent accept on JS). The -1 (unlimited) and 0 (off) sentinels still pass through. New portable test reconnect-max-beyond-integer-range-rejects asserts the :invalid-max rejection on both legs. clj-kondo clean; JVM 95 tests / Node 72 tests green. Public-contract home of :invalid-max is the linked HITL nts-01kt87whe06m.
