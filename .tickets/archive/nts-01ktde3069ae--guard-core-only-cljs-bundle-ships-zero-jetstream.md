---
id: nts-01ktde3069ae
title: 'Guard: core-only CLJS bundle ships zero JetStream bytes'
status: closed
type: task
priority: 2
mode: afk
created: '2026-06-06T03:02:10.373045376Z'
updated: '2026-06-10T00:02:40.052263296Z'
closed: '2026-06-10T00:02:40.052263296Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- bundle
acceptance:
- title: A core-only CLJS entry (requires only nats-cljc.core) produces an optimized bundle with zero @nats-io/jetstream bytes
  done: true
- title: The guard fails loudly if a JetStream import leaks into a core-reachable namespace
  done: true
deps:
- nts-01ktde2z9786
---

## Description

Prove the ADR 0016 bundle-isolation consequence: declaring @nats-io/jetstream unconditionally must NOT pull JetStream bytes into a bundle whose code never requires the JetStream facade. Add a build guard that compiles a core-only CLJS entry (requires only nats-cljc.core) to an optimized bundle and asserts the output contains zero @nats-io/jetstream bytes, catching any accidental fold of the JetStream import into a shared core namespace. Best landed last so it guards the whole JetStream surface. Covers user story 39.

## Notes

**2026-06-09T23:58:55.451739436Z**

Landed in 34ad338: :core-bundle-check shadow build (entry nats-cljc.core only), bb bundle:check, and a CI node-job step that release-compile the core-only bundle and fail on any JetStream marker. Verified green (0 marker hits, 281KB bundle) and red (temporary @nats-io/jetstream require in nats-cljc.impl.js -> 345KB bundle, both markers present, exit 1). Guard left for verifier to close.

**2026-06-10T00:02:40.052263296Z**

Added the ADR 0016 bundle-isolation guard: a :core-bundle-check shadow build in /home/jonasrodrigues/projects/nats/shadow-cljs.edn whose single entry is nats-cljc.core, released to an optimized browser bundle; a `bb bundle:check` task in bb.edn and a CI node-job step in .github/workflows/ci.yml that release-compile it and fail loudly if the output contains any JetStream marker ("jetstream" or "$JS.API" — neither occurs in nats-core or core-reachable sources). Verified green (0 marker hits, 281KB bundle) and red by temporarily requiring @nats-io/jetstream in nats-cljc.impl.js (345KB bundle, both markers hit, exit 1), then reverted. Lint clean; JVM (171 tests/487 assertions) and Node (142 tests/399 assertions) legs both green.
