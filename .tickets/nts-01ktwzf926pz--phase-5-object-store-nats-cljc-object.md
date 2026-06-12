---
id: nts-01ktwzf926pz
title: 'Phase 5: Object Store (nats-cljc.object)'
status: open
type: epic
priority: 2
mode: hitl
created: '2026-06-12T03:54:34.950315921Z'
updated: '2026-06-12T03:54:34.950315921Z'
tags:
- object-store
- phase-5
---

## Description

Portable Object Store facade over JetStream, per the README roadmap (Phase 5). Same shape as the KV phase: bucket-style lifecycle and operator surface, put/get of arbitrarily large objects (chunked over JetStream), metadata, links, and watch — on both legs (jnats on the JVM, @nats-io/obj on CLJS, version-pinned and unconditional like the rest of the nats family). Scope, vocabulary (speak Object Store vocabulary, not the stream substrate — cf. ADR 0023 for KV), error model extensions, and the CLJS dependency story all need refinement before breakdown. To be grilled/refined before child tickets are cut.
