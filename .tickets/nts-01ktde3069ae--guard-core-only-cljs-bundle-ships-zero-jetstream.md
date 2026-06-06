---
id: nts-01ktde3069ae
title: 'Guard: core-only CLJS bundle ships zero JetStream bytes'
status: open
type: task
priority: 2
mode: afk
created: '2026-06-06T03:02:10.373045376Z'
updated: '2026-06-06T03:02:10.373045376Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- bundle
acceptance:
- title: A core-only CLJS entry (requires only nats-cljc.core) produces an optimized bundle with zero @nats-io/jetstream bytes
  done: false
- title: The guard fails loudly if a JetStream import leaks into a core-reachable namespace
  done: false
deps:
- nts-01ktde2z9786
---

## Description

Prove the ADR 0016 bundle-isolation consequence: declaring @nats-io/jetstream unconditionally must NOT pull JetStream bytes into a bundle whose code never requires the JetStream facade. Add a build guard that compiles a core-only CLJS entry (requires only nats-cljc.core) to an optimized bundle and asserts the output contains zero @nats-io/jetstream bytes, catching any accidental fold of the JetStream import into a shared core namespace. Best landed last so it guards the whole JetStream surface. Covers user story 39.
