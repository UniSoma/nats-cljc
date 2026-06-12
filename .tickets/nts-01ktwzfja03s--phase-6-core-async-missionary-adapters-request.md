---
id: nts-01ktwzfja03s
title: 'Phase 6: core.async + missionary adapters; request-many'
status: open
type: epic
priority: 2
mode: hitl
created: '2026-06-12T03:54:44.416610684Z'
updated: '2026-06-12T03:54:44.416610684Z'
tags:
- async-adapters
- phase-6
---

## Description

Per the README roadmap (Phase 6), two strands: (1) opt-in subscription adapters that surface core/JetStream/KV-watch deliveries as core.async channels and missionary flows — without adding either as a runtime dependency (same opt-in posture as the :transit/:json codecs, ADR 0004), layered over the existing promise-return backpressure contract; (2) request-many scatter-gather — one request, a bounded gather of multiple replies (the primitive that services discovery already uses internally). Adapter API shape, backpressure mapping (promise-return handler -> channel/flow semantics), and where request-many lives (core vs a new ns) all need refinement before breakdown. To be grilled/refined before child tickets are cut.
