---
id: nts-01ktwvf42qxj
title: 'README: add JetStream section'
status: closed
type: chore
priority: 4
mode: afk
created: '2026-06-12T02:44:35.536041522Z'
updated: '2026-06-12T02:59:33.490484839Z'
closed: '2026-06-12T02:59:33.490484839Z'
tags:
- docs
links:
- nts-01ktwsa68gvy
---

## Description

nats-cljc.jetstream shipped in Phase 2 (0.2.0) and carries seven ADRs (0016-0022), yet it is the only shipped public namespace with no README section — the README jumps from 'ClojureScript-only: native await' straight to KV, and JetStream exists only as a Roadmap bullet.

Add a '## JetStream (nats-cljc.jetstream)' section matching the KV/Services pattern (~25 lines with examples): streams, consumers, acked publish, ack/nak/term, pull-consume through the promise-return handler (ADR 0018). Prose must match the JetStream ADRs (0016-0022). Deliberately a README section, not a standalone cljdoc article — the README's strength is being one coherent tour, and a first standalone guide article would re-fragment the sidebar that nts-01ktwsa68gvy is cleaning up.

Split out of the grilling session on nts-01ktwsa68gvy (2026-06-12): authoring work with a different review bar than that mechanical sidebar chore.

## Notes

**2026-06-12T02:59:33.490484839Z**

Added the README JetStream section before KV, matching the KV/Services pattern: intro (context verified at entry, ADR 0017), a p/let example (create-stream, create-consumer, acked publish, consume+ack), bullets covering closed config maps (ADR 0015/0020/0021), PubAck/msg-id/expect, the pull triad with promise-return backpressure (ADR 0018), acks as sugar over publish (ADR 0019), ordered-consumer, get-message, per-consume :on-error (ADR 0020), and the unconditional @nats-io/jetstream note (ADR 0016). Example verified verbatim via nREPL against the live :4222 JetStream server — PubAck resolved, handler received decoded data, ack path clean (and the aborted first run incidentally confirmed :msg-id dedup with :duplicate true). Lint clean; full suites green JVM (237) + Node (206).
