---
id: nts-01ktn01kgb2c
title: Normalize all portable timestamps to the canonical cross-leg format
status: closed
type: bug
priority: 2
mode: afk
created: '2026-06-09T01:30:40.011598547Z'
updated: '2026-06-09T22:47:52.790474211Z'
closed: '2026-06-09T22:47:52.790474211Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
acceptance:
- title: stream-info :created and consumer-info :created emit the canonical UTC-millis Z format on both legs, byte-identical for a known instant
  done: true
- title: Every portable-facing timestamp routes through one shared canonical formatter per leg; no ISO_OFFSET_DATE_TIME or raw server-time passthrough remains in the portable maps
  done: true
- title: Tests assert string equality against a known instant for each date field (stream :created, consumer :created), replacing the loose iso-re shape match
  done: true
---

## Description

Wall-clock timestamps that cross the portable boundary must emit ONE canonical string — UTC, `Z`-terminated, exactly three fractional digits (millis), truncated not rounded — so the same instant is byte-identical on the JVM and Node legs (the ADR 0019/0020 pure-data parity invariant).

Today only `:js :timestamp` on a delivered pull message is normalized: each leg routes it through a `->canonical-timestamp` helper (JVM: `DateTimeFormatterBuilder.appendInstant(3)` over the metadata `ZonedDateTime`; CLJS: `Date#toISOString` over the JsMsg string). The other two surfaced timestamps still use the OLD un-normalized pattern and almost certainly diverge across legs:

- `stream-info->map` `:created` — JVM `.format ... ISO_OFFSET_DATE_TIME` (variable fractional digits, source offset) vs CLJS raw server RFC3339 string passthrough (full nanosecond precision).
- `consumer-info->map` `:created` — same split.

The suite only checks these against the loose `iso-re` (`\d{4}-..T..:..:...*`), which masks the divergence exactly as it did for `:timestamp`.

Scope: route both `:created` fields through the same per-leg canonical helper (the helpers already exist and accept these inputs directly — `:created` is a `ZonedDateTime` on the JVM and a string on CLJS), and tighten the tests. The principle is the durable part: any future API that surfaces a wall-clock timestamp (e.g. a direct `get`, stream-state first/last message times, KV) routes it through the one shared formatter rather than formatting ad hoc — there should be a single date seam per leg, no second place that touches `ISO_OFFSET_DATE_TIME` or passes a server time string through raw.

Note: canonical precision is milliseconds. The server may hand back nanoseconds; normalizing truncates to millis on both legs (matching `:timestamp`). If any field genuinely needs sub-milli fidelity, surface it as a separate nanos field rather than widening the canonical string format.

## Notes

**2026-06-09T22:47:52.790474211Z**

All portable wall-clock timestamps now route through one canonical formatter per leg: stream-info and consumer-info :created emit the UTC-millis Z form (truncated, byte-identical across JVM/Node), no ISO_OFFSET_DATE_TIME or raw server-string passthrough remains, and deep-module tests assert exact string equality for a known offset-bearing nanosecond instant (live assertions tightened from iso-re to canonical-ts-re).
