---
id: nts-01ktq7fj46t3
title: 'Connection-close liveness parity: JVM consume handle stays active after the connection closes'
status: closed
type: bug
priority: 2
mode: hitl
created: '2026-06-09T22:19:06.241572827Z'
updated: '2026-06-09T22:36:41.201474216Z'
closed: '2026-06-09T22:36:41.201474216Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
acceptance:
- title: After the owning connection closes, the JVM consume handle's -active? reports false
  done: true
- title: JVM and CLJS consume handles report the same -active? value after connection close, covered by a test on both legs
  done: true
- title: The drain-window semantics of ADR 0022 (active mid-drain; gave-up drain leaves active true) still hold
  done: true
links:
- nts-01ktde300gz3
---

## Description

ADR 0022 pins `-active?` as flipping false once "the connection ends it" — the JVM consume handle doesn't deliver that today. jnats never touches a `MessageConsumer`'s `stopped`/`finished` flags on connection teardown, and the handle's own `closed?` atom (`src/nats_cljc/jetstream/impl/jvm.clj`, `JvmConsumeHandle`) flips only on `-unsubscribe` — so after the connection closes, `(active? handle)` keeps reporting true indefinitely. On CLJS the parity holds for free: connection close ends the `ConsumerMessages` iterator, the drive loop sees `done`, and `drive-consume!` flips the `active?` atom false (`src/nats_cljc/jetstream/impl/js.cljs`).

Discovered while reconciling drain-window `-active?` semantics (ADR 0022); connection-close was out of that scope.

## Design

The JVM handle needs a connection-aware liveness source. Candidate directions, to be settled at implementation: consult the owning connection's status in `-active?` (the handle would need a connection or status-supplier reference at construction); or flip the handle's `closed?` from the connection's status listener on `:closed`. Whichever is chosen, the drain-window and gave-up-drain semantics of ADR 0022 must be preserved — connection close is a new false-input, not a change to the existing ones.

## Notes

**2026-06-09T22:32:51.882342238Z**

Fix landed in 2edb820: JvmJetStreamContext carries the owning Connection; JvmConsumeHandle gets a conn-closed? supplier (CLOSED only) as a third -active? false-input. Both-legs integration test consume-inactive-after-connection-close; full JVM (158 tests) and Node (129 tests) suites green; drain-window/gave-up-drain tests untouched and passing.

**2026-06-09T22:36:41.201474216Z**

Landed: JvmJetStreamContext carries the owning jnats Connection and JvmConsumeHandle consults a conn-closed? supplier (terminal CLOSED only) as a third -active? false-input, so connection close flips the JVM handle inactive — parity with the CLJS drive loop's iterator-done flip. Transient disconnect/reconnect keeps the handle active on both legs; ADR 0022 drain-window and gave-up-drain semantics unchanged. Covered by the both-legs integration test consume-inactive-after-connection-close (a second connection owns the consume); full JVM and Node suites green.
