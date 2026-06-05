---
id: nts-01kt87whazgd
title: Guard the blocking `enqueue` counter
status: closed
type: chore
priority: 3
mode: afk
created: '2026-06-04T02:37:34.943377493Z'
updated: '2026-06-05T20:09:15.545829239Z'
closed: '2026-06-05T20:09:15.545829239Z'
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

## Notes

**2026-06-05T20:09:15.545829239Z**

Won't-fix (perf not worth the complexity). The unarmed enqueue swap costs ~60-90ns + ~70B young-gen/msg, which is ~1-3% of consumer-side per-message CPU under the default EDN codec — the decode (~1-5us) plus ArrayBlockingQueue.offer and the message-map alloc on the same line dominate it by 10-50x. It only becomes a measurable slice of a core at sustained ~500k-1M msg/s, which the single-dispatcher, take-message-one-at-a-time, codec-bottlenecked pull path can't reach anyway (high throughput is the push Handler path, which has no such counter).

The fix is also not the one-line guard the ticket implies. Naively skipping the increment while unarmed breaks the pinned 'counted from subscription start' semantic — see core_test.clj:361 unsubscribe-max-already-received-ends-now (take 3, then arm max=2 must end now because received>=max). Recovering that baseline without a running tally needs the native delivered count (a new -delivered-count on the Sub protocol + reach into JvmSubscription), plus reseed-at-arm race handling. Net: adds coupling and race surface to shave ~100ns off a us-bound path — a complexity loss for an unmeasurable gain.
