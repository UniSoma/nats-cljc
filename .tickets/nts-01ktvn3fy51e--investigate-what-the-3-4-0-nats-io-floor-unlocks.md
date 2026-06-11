---
id: nts-01ktvn3fy51e
title: Investigate what the 3.4.0 nats-io floor unlocks
status: open
type: task
priority: 3
mode: hitl
created: '2026-06-11T15:34:08.581677608Z'
updated: '2026-06-11T15:36:49.785503418Z'
tags:
- services
- deps
- research
links:
- nts-01ktvn87why4
---

## Description

Phase 4 (services) floors the nats-io trio (nats-core, @nats-io/jetstream, @nats-io/kv) at 3.4.0 — see ADR 0026, since @nats-io/services@3.4.0 peer-requires nats-core@3.4.0. Independently of shipping services, survey what else moving from 3.3.1 to 3.4.0 (and matching jnats) makes available: new core/JetStream/KV features, bug fixes, API additions, or deprecations we could now adopt or should be aware of. Output: a short note on anything worth a follow-up ticket. JVM jnats changelog and the nats-io JS 3.4.0 release notes are the sources.
