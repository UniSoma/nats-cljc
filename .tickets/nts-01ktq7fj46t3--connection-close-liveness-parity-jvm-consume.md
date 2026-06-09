---
id: nts-01ktq7fj46t3
title: 'Connection-close liveness parity: JVM consume handle stays active after the connection closes'
status: open
type: bug
priority: 2
mode: hitl
created: '2026-06-09T22:19:06.241572827Z'
updated: '2026-06-09T22:19:06.241572827Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
acceptance:
- title: After the owning connection closes, the JVM consume handle's -active? reports false
  done: false
- title: JVM and CLJS consume handles report the same -active? value after connection close, covered by a test on both legs
  done: false
- title: The drain-window semantics of ADR 0022 (active mid-drain; gave-up drain leaves active true) still hold
  done: false
links:
- nts-01ktde300gz3
---

## Description

ADR 0022 pins `-active?` as flipping false once "the connection ends it" — the JVM consume handle doesn't deliver that today. jnats never touches a `MessageConsumer`'s `stopped`/`finished` flags on connection teardown, and the handle's own `closed?` atom (`src/nats_cljc/jetstream/impl/jvm.clj`, `JvmConsumeHandle`) flips only on `-unsubscribe` — so after the connection closes, `(active? handle)` keeps reporting true indefinitely. On CLJS the parity holds for free: connection close ends the `ConsumerMessages` iterator, the drive loop sees `done`, and `drive-consume!` flips the `active?` atom false (`src/nats_cljc/jetstream/impl/js.cljs`).

Discovered while reconciling drain-window `-active?` semantics (ADR 0022); connection-close was out of that scope.

## Design

The JVM handle needs a connection-aware liveness source. Candidate directions, to be settled at implementation: consult the owning connection's status in `-active?` (the handle would need a connection or status-supplier reference at construction); or flip the handle's `closed?` from the connection's status listener on `:closed`. Whichever is chosen, the drain-window and gave-up-drain semantics of ADR 0022 must be preserved — connection close is a new false-input, not a change to the existing ones.
