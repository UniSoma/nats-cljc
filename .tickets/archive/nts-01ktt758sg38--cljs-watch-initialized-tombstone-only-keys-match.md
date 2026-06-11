---
id: nts-01ktt758sg38
title: 'CLJS watch :initialized: tombstone-only :keys match resolves before markers replay'
status: closed
type: task
priority: 3
mode: afk
created: '2026-06-11T02:11:12.303953667Z'
updated: '2026-06-11T02:31:57.597984044Z'
closed: '2026-06-11T02:31:57.597984044Z'
parent: nts-01ktsner23xc
tags:
- kv
- cljs
---

## Description

On the CLJS leg, @nats-io/kv 3.x exposes no initialized callback, so kv/impl/js.cljs derives the watch :initialized signal: a delta-0 delivery, or — when nothing replays — a filtered live-key probe. The probe counts LIVE keys, so a :keys filter matching only tombstoned keys resolves :initialized early, and the markers still replay after it (the code comment at the probe site records the trade). Revisit when nats.js grows an initialized/init_done signal for filtered watches, or normalize by counting markers in the probe (kvKeys includes no deleted keys, so this likely needs the history-based probe instead). Until then this is a known limitation: it only bites a :deliver :latest/:history watch whose :keys patterns match nothing but tombstones, and only for marker deliveries (cache builders ignoring deletes are unaffected). Add the missing edge test alongside the fix.

## Notes

**2026-06-11T02:24:01.775890380Z**

Verify-first findings (@nats-io/kv 3.3.1, nats-server 2.14.1, Node ws leg, live runtime probe): (1) history({key}) accepts both a single string and string[] — multi-key worked at runtime, matching the types.d.ts signature. (2) A filtered history matching nothing (literal and wildcard pattern) completes promptly (~0.7ms), zero entries — no hang, so the history-based probe cannot leave :initialized unresolved. (3) A tombstoned key's filtered history yields its markers (PUT delta 1, DEL delta 0) — markers ARE visible to history, the property the live-key probe lacks. (4) qi.stop() mid-iteration terminates cleanly, so the probe can short-circuit after the first entry (note: one already-buffered entry may still be yielded after stop — harmless for an emptiness check). Conclusion: replace the .keys probe in -kv-watch's :keys arm with a filtered-history emptiness probe; non-empty history guarantees at least one replayed entry, so the existing delta-0 path marks the true boundary. The no-:keys .status arm stays — .values counts markers already.

**2026-06-11T02:31:57.597984044Z**

Fixed by replacing the CLJS :keys probe: -kv-watch now probes filtered HISTORY (first entry only, then stop) instead of enumerating live keys. History sees Tombstone/purge markers, so a tombstone-only :keys match no longer resolves :initialized early — the markers replay and the existing delta-0 boundary fires after their deliveries settle, matching jnats endOfData. Verified at @nats-io/kv 3.3.1: history({key}) takes string|string[], a no-match filter completes promptly (~0.7ms, so :initialized can't hang), markers appear with correct deltas. Added the edge test watch-keys-tombstone-only-match-initializes-after-markers (snapshots delivered :operations at the instant :initialized resolves; CLJS leg slows handler settle per ADR 0007 so the race can't mask an early resolve) — red on Node before the fix ([] instead of [:delete]), green after; JVM was already correct via native endOfData. Full suite green on both legs, lint clean.
