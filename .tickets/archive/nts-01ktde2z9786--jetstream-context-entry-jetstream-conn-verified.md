---
id: nts-01ktde2z9786
title: 'JetStream context entry: (jetstream conn) verified at entry'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-06T03:02:09.446935968Z'
updated: '2026-06-06T16:09:47.517594700Z'
closed: '2026-06-06T16:09:47.517594700Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- context
acceptance:
- title: (jetstream conn) resolves to a JetStream context against a JetStream-enabled server, identically on JVM and Node
  done: true
- title: Against a JetStream-disabled server, (jetstream conn) rejects with ex-info :type :jetstream-not-enabled on BOTH legs (proving the forced JVM round-trip)
  done: true
- title: The JetStream-enabled anon :4222 harness is in place (jetstream{} + gitignored store_dir) and running-tests docs pin nats-server >= 2.12
  done: true
- title: '@nats-io/jetstream is declared unconditionally and lockstep-pinned to the nats-core version for the CLJS dev/test legs'
  done: true
- title: The portable .cljc context-entry test passes on JVM + Node
  done: true
---

## Description

Walking skeleton for Phase 2: stand up the structure every later slice rides on, then prove the single end-to-end thread of obtaining a JetStream context. From a Connection, (jetstream conn) returns a Promise of a JetStream context holding both the data and management planes (ADR 0017). Obtaining it verifies JetStream is enabled by forcing a JS-info round-trip on BOTH legs (native on CLJS, added inside the off-thread wrap on the JVM), so :jetstream-not-enabled (err 10039) surfaces at the handle identically on JVM and Node, never deferred to the first operation. Establishes: the JetStream-enabled test harness (a jetstream{} block plus a gitignored store_dir on the existing anon :4222 server, nats-server >= 2.12, default :storage :memory in tests

## Notes

**2026-06-06T16:09:47.517594700Z**

Phase-2 walking skeleton landed. Portable nats-cljc.jetstream facade + JetStream protocol, extended onto each leg's Connection record from JetStream-only impl namespaces (keeps @nats-io/jetstream out of a core-only CLJS bundle, ADR 0016). (jetstream conn) returns a native promise of a context holding both data and management planes (ADR 0017), verifying JetStream is enabled at entry via a forced $JS.API.INFO round-trip on both legs — native on CLJS (jetstreamManager), off-thread on the JVM (getAccountStatistics) — so it rejects :jetstream-not-enabled (err 10039) at the handle, never deferred to the first op. Shared err_code->:type table (ADR 0020). JetStream-enabled anon :4222 harness (jetstream{} + gitignored store_dir, nats-server >= 2.12) and lockstep-pinned @nats-io/jetstream. Portable .cljc context-entry test green on JVM + Node.
