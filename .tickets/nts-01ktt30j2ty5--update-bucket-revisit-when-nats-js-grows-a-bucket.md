---
id: nts-01ktt30j2ty5
title: 'update-bucket: revisit when nats.js grows a bucket-config update'
status: open
type: task
priority: 3
mode: hitl
created: '2026-06-11T00:58:43.673964823Z'
updated: '2026-06-11T00:58:43.673964823Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
---

## Description

update-bucket (re-apply a closed config map to an existing Bucket, same key set as create-bucket) was verified present-or-absent on both natives at the pinned versions and DROPPED from Phase 3: jnats 2.25.3 exposes it (KeyValueManagement.update(KeyValueConfiguration), confirmed by reflection), but @nats-io/kv 3.3.1 has no bucket-config update path — Kvm exposes only create/open/list, and Bucket.init on an existing stream merely binds (jsm.streams.info, never streams.update; confirmed at runtime against the installed package). Per ADR 0003 we never reach around the native KV client to raw stream calls, so the op cannot ship portably. Revisit when @nats-io/kv grows a bucket-config update; until then JVM-only consumers can use jnats directly.
