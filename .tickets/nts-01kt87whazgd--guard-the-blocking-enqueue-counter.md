---
id: nts-01kt87whazgd
title: Guard the blocking `enqueue` counter
status: open
type: chore
priority: 3
mode: afk
created: '2026-06-04T02:37:34.943377493Z'
updated: '2026-06-04T02:53:25.335795701Z'
tags:
- review
- efficiency
- blocking
acceptance:
- title: With no `:max` armed, `enqueue` performs no counter swap per message
  done: false
- title: The auto-unsubscribe-after-N behavior when `:max` is armed still works
  done: false
- title: clj-kondo clean; JVM suite green (blocking layer is JVM-only)
  done: false
---

## Description

The blocking layer's `enqueue` runs `(swap! counter update :received inc)` on every buffered message even when no `:max` is armed — the common case, since `:max` stays nil until `(unsubscribe sub max)` — incurring a per-message atom CAS + map alloc that nothing consumes.

Guard the increment so it runs only when a `:max` is armed.
