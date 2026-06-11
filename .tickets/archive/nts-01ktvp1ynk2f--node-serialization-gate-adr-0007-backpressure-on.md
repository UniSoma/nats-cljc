---
id: nts-01ktvp1ynk2f
title: 'Node serialization gate: ADR-0007 backpressure on endpoint handlers'
status: closed
type: task
priority: 2
mode: afk
created: '2026-06-11T15:50:46.694833655Z'
updated: '2026-06-11T18:38:32.009060574Z'
closed: '2026-06-11T18:38:32.009060574Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: A slow async handler demonstrably delays the next request to the same endpoint on Node (serial delivery), with the test watched red on a known-bad arrangement first
  done: true
- title: :processing-time-ns for that endpoint reflects the awaited handler duration
  done: true
- title: JVM serialization is covered by the same portable test
  done: true
- title: 'If the JS native cannot await the handler: either the impl drives the endpoint async iterator, or the services Handler contract is narrowed and documented — the choice is recorded'
  done: true
deps:
- nts-01ktvnzj8kwp
- nts-01ktvp17f0r7
---

## Description

The explicit verification gate from the epic's testing decisions, written as tests and watched red on a known-bad input before trusting green (AGENTS.md: verify toolchain behavior, don't infer it). Endpoint handler delivery must follow ADR 0007 — serial per endpoint, promise-return backpressure, never blocking. The JVM leg falls out of the dispatcher blocking on the CompletionStage; the JS leg is the open question.

Tests: a slow async handler must demonstrably delay the next request to the same endpoint on Node, and the endpoint's `:processing-time-ns` must reflect the awaited duration, not just the synchronous callback time. The same portable test covers JVM serialization.

If the native JS callback does not await the returned promise, follow the epic's pre-decided fork: drive the endpoint async iterator instead of the callback, or narrow the Handler contract for services and document it. Either outcome is recorded (docs and, if narrowed, the relevant ADR/glossary touch-up).

## Notes

**2026-06-11T18:38:32.009060574Z**

ADR-0007 serialization gate for service endpoints. JS impl: replaced the nats.js callback subscription (which, verified empirically, does NOT await the handler's returned promise) with iterator-driven delivery (road 2) — drive-endpoint! loops the QueuedIterator that .addEndpoint returns, awaiting the handler between .next pulls, so serial-per-endpoint delivery + promise-return backpressure engage and the awaited duration lands in the iterator's profile timer. nats.js never countErrors on the iterator path, so a thrown/rejected handler (and respond-error, which throws per ADR 0025) is counted via endpoint-stats (native stats located off the Service handler entry, impl-confined). Portable test asserts serial delivery and processing-time-ns >= awaited floor; WATCHED RED on the callback arrangement (gap 0, processing-time 0) on Node first. JVM serialization falls out of the dispatcher blocking on the CompletionStage, same test. Fork RECORDED in ADR 0007 (drive the iterator, contract unchanged). Verified: bb lint clean, JVM 232 / Node 203 tests 0 failures, bundle:check + externs:check green.
