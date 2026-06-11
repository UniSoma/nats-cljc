---
id: nts-01ktvnz0v6pp
title: nats-io trio floor 3.4.0 with @nats-io/services in lockstep
status: in_progress
type: chore
priority: 2
mode: afk
created: '2026-06-11T15:49:10.629969334Z'
updated: '2026-06-11T16:03:52.206787564Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: nats-core, @nats-io/jetstream and @nats-io/kv are pinned at 3.4.0 in deps.cljs :npm-deps and the root package.json, in one change
  done: false
- title: '@nats-io/services is declared unconditionally at 3.4.0, lockstep-pinned with the trio'
  done: false
- title: The full existing test suite is green on JVM and Node against the 3.4.0 floor
  done: false
- title: Existing bundle-check and externs-check guards stay green; clj-kondo lint is clean
  done: false
---

## Description

Enacts the dependency half of ADR 0026 in isolation, before any service code exists: lift the nats-io trio (nats-core, @nats-io/jetstream, @nats-io/kv) from 3.3.1 to 3.4.0 in one change, and add @nats-io/services at 3.4.0, lockstep-pinned, declared unconditionally in src/deps.cljs :npm-deps and added to the root package.json for the test legs.

No service code lands here — nothing imports the new package yet, so bundles are unchanged; the :services bundle/externs guards arrive with the tracer slice.

The point of slicing this off: the floor move is the behavior-breaking risk of Phase 4, and it must be proven green by us, not a consumer. Verification is the full existing suite (core, JetStream, KV) on JVM + Node against the shared server, plus the existing bundle-check/externs-check guards and lint.
