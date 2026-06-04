---
id: nts-01kt87wghgg4
title: Guard the Reconnect `:max` Integer cap
status: open
type: bug
priority: 2
mode: afk
created: '2026-06-04T02:37:34.128196662Z'
updated: '2026-06-04T02:53:24.498818372Z'
tags:
- review
- connect
acceptance:
- title: '`(connect {:reconnect {:max <huge>}})` produces the same typed outcome on both legs, no uncaught ArithmeticException on JVM and no silent accept on JS'
  done: false
- title: In-range `:max` values including the `0` and `-1` sentinels still configure reconnection correctly
  done: false
- title: clj-kondo clean; suite green on JVM and Node
  done: false
links:
- nts-01kt87whe06m
---

## Description

`with-reconnect` calls `(int max)` unguarded inside connect's supplier, so a `:reconnect {:max <beyond Integer range>}` throws an uncaught `ArithmeticException` (integer overflow) on the JVM — completing the connect promise with a raw, untyped exception — while JS accepts it silently.

Apply the same Integer-range guard `core/unsubscribe` already uses for its `max`, rejecting an out-of-range `:reconnect :max` with a typed validation error on both legs (following the unsubscribe-max precedent). Both legs must agree.
