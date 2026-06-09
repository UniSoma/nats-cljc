---
id: nts-01ktde3036sg
title: 'Ordered consumer: gap-free, no-ack stream replay'
status: closed
type: feature
priority: 2
mode: afk
created: '2026-06-06T03:02:10.273856573Z'
updated: '2026-06-09T23:29:56.329863463Z'
closed: '2026-06-09T23:29:56.329863463Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- ordered
acceptance:
- title: An ordered consumer replays all published messages of a Stream in sequence order, gap-free, with no acks, on both legs
  done: true
- title: It is ephemeral (leaves no durable consumer behind) and reuses the pull delivery surface (next/fetch/consume)
  done: true
- title: Portable integration test passes on JVM + Node
  done: true
deps:
- nts-01ktde2zr4f0
---

## Description

An Ordered consumer for single-client, gap-free replay reusing the pull triad (next/fetch/consume). (ordered-consumer js-ctx stream opts) yields a pull handle that replays the Stream in order taking NO acknowledgements; it is ephemeral, server-managed, and automatically recreated if a sequence gap appears. Covers user story 34.

## Notes

**2026-06-09T23:29:56.329863463Z**

Implemented the Ordered consumer slice: (jet/ordered-consumer ctx stream opts) resolves to an ordered pull handle (jnats OrderedConsumerContext on the JVM, nats.js consumers.ordered on CLJS) — a server-managed ephemeral with ack policy none, client-recreated on a sequence gap — and the pull triad gained handle-first arities ((next handle opts), (fetch handle opts), (consume handle handler opts)) sharing the named arities' guards, decode, refill knobs, and drainable consume handle. The closed opts map (:filter-subjects/:deliver-policy) rejects unknown keys pre-flight; both legs surface :stream-not-found operationally. Three new portable integration tests cover gap-free in-order replay with no acks via fetch+next, continuous consume in order with drain, ephemerality (no durable consumer left behind, ack policy none), and the pre-flight validation rejections. clj-kondo clean; full suite green on JVM (167 tests/463 assertions) and Node (138 tests/375 assertions). Key files: src/nats_cljc/jetstream.cljc, src/nats_cljc/protocol.cljc, src/nats_cljc/jetstream/consumer.cljc, src/nats_cljc/jetstream/impl/jvm.clj, src/nats_cljc/jetstream/impl/js.cljs, test/nats_cljc/jetstream_test.cljc. Ticket left open for the verifier.
