---
id: nts-01ktvn3fy51e
title: Investigate what the 3.4.0 nats-io floor unlocks
status: closed
type: task
priority: 3
mode: hitl
created: '2026-06-11T15:34:08.581677608Z'
updated: '2026-06-12T02:16:24.625960189Z'
closed: '2026-06-12T02:16:24.625960189Z'
tags:
- services
- deps
- research
links:
- nts-01ktvn87why4
---

## Description

Phase 4 (services) floors the nats-io trio (nats-core, @nats-io/jetstream, @nats-io/kv) at 3.4.0 — see ADR 0026, since @nats-io/services@3.4.0 peer-requires nats-core@3.4.0. Independently of shipping services, survey what else moving from 3.3.1 to 3.4.0 (and matching jnats) makes available: new core/JetStream/KV features, bug fixes, API additions, or deprecations we could now adopt or should be aware of. Output: a short note on anything worth a follow-up ticket. JVM jnats changelog and the nats-io JS 3.4.0 release notes are the sources.

## Notes

**2026-06-12T02:12:10.487657866Z**

Findings — what the 3.4.0 nats-io floor (and jnats 2.25.3) unlocks.

Versioning facts: nats.js has no stable releases between 3.3.1 and 3.4.0 (only 3.4.0 canaries); all packages bumped lockstep via nats.js PR #410 (2026-05-08). jnats 2.25.3 (2026-05-07) is the latest published release — nothing newer to move to. @nats-io/services 3.4.0 itself is a pure lockstep bump, no services-specific changes. Both releases' shared headline is nats-server 2.14 support, and notably both clients shipped the same 2.14 feature set within a day of each other.

Worth a follow-up ticket:
1. Server 2.14 JetStream features now available on BOTH legs in matched form: consumer reset (JS #391 reset()/resetConsumer(); jnats #1562), stream consumer-source config (JS #399 StreamSource.consumer + AckPolicy.FlowControl; jnats #1565 + #1563 $JS.FC), batched publish / fast ingest (JS #379 startFastIngest(), experimental + StreamConfig allow_batched; jnats batch publish since 2.23.0), and message scheduling incl. cron (JS #381, ADR-51; jnats schedule headers #1543 on the 2.23.0 base). If/when the portable API grows consumer-management or 2.14 surfaces, the dependency floor is no longer the blocker — server version is.
2. KV markerTTL fix (JS #367): put() on a deleted/purged key previously dropped markerTTL/sequence tracking by delegating to update(). We inherit the fix for free, but it touches the same marker/TTL territory as our recent purge-deletes work — worth a quick check that existing KV tests would catch a regression here, and whether any workaround in our impl is now dead.

Awareness only (no ticket needed):
- JS inbox format changed to Go-style _INBOX.<nuid>.<token> (#398) — only matters for permission rules/wiretaps that pattern-match inbox subjects.
- JS push-consumer stall fix (#375): client now responds to Nats-Consumer-Stalled heartbeat headers — inherited silently.
- JS core additions with no JVM counterpart, so no portable-API candidates: Symbol.asyncDispose on connection/subscription (#396), setServers()/getServers() + reconnectToServer option (#400/#403), hardened protocol-integer parsing (#373).
- jnats 2.25.x behavior changes already live under our 2.25.3 pin: payload size now counts header bytes (#1525), stricter subject validation (#1501), simplified-consumer accounting counts processed not received (#1528), object-store purge on re-put (#1491), and a WebSocket masking fix (% 8 -> % 4, #1546) relevant to our ws-enabled JVM test leg.

Sources: github.com/nats-io/nats.js/releases (v3.4.0), compare v3.3.1...v3.4.0; github.com/nats-io/nats.java/releases (2.25.1-2.25.3).

**2026-06-12T02:16:24.625960189Z**

Surveyed the 3.3.1->3.4.0 nats.js delta and the jnats 2.25.x line. No stable JS releases in between; services 3.4.0 is a pure lockstep bump; jnats 2.25.3 is already the latest. Shared headline is nats-server 2.14 support (consumer reset, consumer-source, batched publish, cron scheduling) now matched on both legs — blocked on server version, not deps. Full findings note on the ticket; no follow-up tickets per maintainer decision.
