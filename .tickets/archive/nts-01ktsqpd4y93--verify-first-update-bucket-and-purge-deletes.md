---
id: nts-01ktsqpd4y93
title: 'Verify-first: update-bucket and purge-deletes'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:56.601086515Z'
updated: '2026-06-11T00:59:32.824519051Z'
closed: '2026-06-11T00:59:32.824519051Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: update-bucket and purge-deletes are each verified present-or-absent on both natives at the pinned versions, with REPL/test evidence rather than docs inference
  done: true
- title: Each op supported by both natives is implemented portably with facade tests on both legs
  done: true
- title: Each op missing on either leg is dropped, with a follow-up ticket opened and linked to the epic
  done: true
deps:
- nts-01ktsqnbtp7t
---

## Description

Two ops ship only if both native clients expose them at the pinned versions (jnats 2.25.3, @nats-io/kv 3.3.1): `update-bucket` (closed config map, same key set as create-bucket) and `purge-deletes` (remove all Tombstones from a Bucket).

Verify each on both legs first — toolchain behavior is a hypothesis until a REPL or test confirms it. For each op both natives support: implement portably with facade tests on both legs. For each op missing on either leg: drop it from Phase 3 and open a follow-up ticket linked to the epic — never reach around the native KV client to raw stream calls (ADR 0003).

## Notes

**2026-06-11T00:58:58.349572452Z**

Verify-first evidence: purge-deletes present on both natives — jnats 2.25.3 KeyValue.purgeDeletes()/purgeDeletes(KeyValuePurgeOptions) confirmed via reflection in the REPL, @nats-io/kv 3.3.1 Bucket.prototype.purgeDeletes confirmed a function at runtime in Node — implemented portably (no-threshold on both legs, overriding the natives' 30-minute grace) with a facade test green on JVM and Node. update-bucket absent on the Node leg — Kvm exposes only create/open/list at runtime, and Bucket.init on an existing stream only binds (streams.info, never streams.update) — dropped; follow-up nts-01ktt30j2ty5 opened under the epic.

**2026-06-11T00:59:32.824519051Z**

Verified both ops on both natives at the pinned versions with REPL/runtime evidence: purge-deletes exists on both (jnats KeyValue.purgeDeletes via reflection; @nats-io/kv Bucket.prototype.purgeDeletes at runtime) and ships portably as kv/purge-deletes — Bucket-wide removal of every Tombstoned key's history, marker included, with the natives' 30-minute grace overridden to none on both legs — covered by a facade test green on JVM and Node. update-bucket is JVM-only (@nats-io/kv 3.3.1 Kvm has no bucket-config update; Bucket.init never streams.update), so it is dropped per ADR 0003 with follow-up nts-01ktt30j2ty5 opened under the epic.
