---
id: nts-01ktsqpd4y93
title: 'Verify-first: update-bucket and purge-deletes'
status: open
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:56.601086515Z'
updated: '2026-06-10T21:40:56.601086515Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: update-bucket and purge-deletes are each verified present-or-absent on both natives at the pinned versions, with REPL/test evidence rather than docs inference
  done: false
- title: Each op supported by both natives is implemented portably with facade tests on both legs
  done: false
- title: Each op missing on either leg is dropped, with a follow-up ticket opened and linked to the epic
  done: false
deps:
- nts-01ktsqnbtp7t
---

## Description

Two ops ship only if both native clients expose them at the pinned versions (jnats 2.25.3, @nats-io/kv 3.3.1): `update-bucket` (closed config map, same key set as create-bucket) and `purge-deletes` (remove all Tombstones from a Bucket).

Verify each on both legs first — toolchain behavior is a hypothesis until a REPL or test confirms it. For each op both natives support: implement portably with facade tests on both legs. For each op missing on either leg: drop it from Phase 3 and open a follow-up ticket linked to the epic — never reach around the native KV client to raw stream calls (ADR 0003).
