---
id: nts-01ktde2z9786
title: 'JetStream context entry: (jetstream conn) verified at entry'
status: open
type: feature
priority: 1
mode: afk
created: '2026-06-06T03:02:09.446935968Z'
updated: '2026-06-06T03:02:09.446935968Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- context
acceptance:
- title: (jetstream conn) resolves to a JetStream context against a JetStream-enabled server, identically on JVM and Node
  done: false
- title: Against a JetStream-disabled server, (jetstream conn) rejects with ex-info :type :jetstream-not-enabled on BOTH legs (proving the forced JVM round-trip)
  done: false
- title: The JetStream-enabled anon :4222 harness is in place (jetstream{} + gitignored store_dir) and running-tests docs pin nats-server >= 2.12
  done: false
- title: '@nats-io/jetstream is declared unconditionally and lockstep-pinned to the nats-core version for the CLJS dev/test legs'
  done: false
- title: The portable .cljc context-entry test passes on JVM + Node
  done: false
---

## Description

Walking skeleton for Phase 2: stand up the structure every later slice rides on, then prove the single end-to-end thread of obtaining a JetStream context. From a Connection, (jetstream conn) returns a Promise of a JetStream context holding both the data and management planes (ADR 0017). Obtaining it verifies JetStream is enabled by forcing a JS-info round-trip on BOTH legs (native on CLJS, added inside the off-thread wrap on the JVM), so :jetstream-not-enabled (err 10039) surfaces at the handle identically on JVM and Node, never deferred to the first operation. Establishes: the JetStream-enabled test harness (a jetstream{} block plus a gitignored store_dir on the existing anon :4222 server, nats-server >= 2.12, default :storage :memory in tests; DD-9); the unconditional, lockstep-pinned @nats-io/jetstream CLJS dependency for the dev/test legs (ADR 0016); the new internal JetStream protocol each platform Connection record implements (ADR 0005 style); both per-leg context records; the portable nats-cljc.jetstream facade namespace; and a JetStream-specific CLJS impl namespace that confines the @nats-io/jetstream import so core-only bundles stay JetStream-free (ADR 0016). Seeds the shared err_code->:type normalization table with :jetstream-not-enabled. Impl-time grounding: the JVM verify-at-entry round-trip is a required obligation; removing it silently reintroduces cross-leg asymmetry. Covers user stories 1, 2, 38.
