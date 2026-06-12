---
id: nts-01ktwhg5zrg3
title: 'Flaky Node KV test: purge-deletes-removes-tombstoned-history-bucket-wide'
status: closed
type: bug
priority: 3
mode: afk
created: '2026-06-11T23:50:24.504067818Z'
updated: '2026-06-12T01:12:42.931993721Z'
closed: '2026-06-12T01:12:42.931993721Z'
tags:
- kv
- flaky-test
acceptance:
- title: Root cause of the surviving delete marker identified and fixed (test or impl)
  done: true
- title: Node suite green across 10 consecutive runs of the kv leg
  done: true
---

## Description

Intermittent failure on the Node leg (observed twice on 2026-06-11, roughly 1 in 3 runs; also once during an automated batch run): purge-deletes-removes-tombstoned-history-bucket-wide at test/nats_cljc/kv_test.cljc:958 fails with (= [] entries) — a delete marker survives kv/purge-deletes. Likely timing around the purge olderThan threshold: a tombstone written just before the purge call may be newer than the cutoff and is retained. Unrelated to the services work shipped the same day (KV code untouched). Reproduce by looping the Node suite (node target/node-tests.js) a few times against the ws-enabled server set.

## Notes

**2026-06-12T01:12:42.931993721Z**

Root cause: the JS leg passed olderMillis 0 to nats.js purgeDeletes, which keeps any delete marker with created >= Date.now() - olderMillis — a marker stamped in the same millisecond as the comparison survives, leaving the tombstone the test saw. Fixed in src/nats_cljc/kv/impl/js.cljs by passing -600000, pushing the cutoff 10 minutes into the future, which is exactly what jnats' deleteMarkersNoThreshold does internally (verified against the 2.25.3 bytecode), so both legs now share the same remove-all-now semantics. Flake reproduced pre-fix (run 2 of 6); post-fix the Node suite ran 10 consecutive greens, JVM leg green, clj-kondo clean. Committed as bee705f.
