---
id: nts-01ktvp1ynk2f
title: 'Node serialization gate: ADR-0007 backpressure on endpoint handlers'
status: in_progress
type: task
priority: 2
mode: afk
created: '2026-06-11T15:50:46.694833655Z'
updated: '2026-06-11T18:04:30.875642347Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: A slow async handler demonstrably delays the next request to the same endpoint on Node (serial delivery), with the test watched red on a known-bad arrangement first
  done: false
- title: :processing-time-ns for that endpoint reflects the awaited handler duration
  done: false
- title: JVM serialization is covered by the same portable test
  done: false
- title: 'If the JS native cannot await the handler: either the impl drives the endpoint async iterator, or the services Handler contract is narrowed and documented — the choice is recorded'
  done: false
deps:
- nts-01ktvnzj8kwp
- nts-01ktvp17f0r7
---

## Description

The explicit verification gate from the epic's testing decisions, written as tests and watched red on a known-bad input before trusting green (AGENTS.md: verify toolchain behavior, don't infer it). Endpoint handler delivery must follow ADR 0007 — serial per endpoint, promise-return backpressure, never blocking. The JVM leg falls out of the dispatcher blocking on the CompletionStage; the JS leg is the open question.

Tests: a slow async handler must demonstrably delay the next request to the same endpoint on Node, and the endpoint's `:processing-time-ns` must reflect the awaited duration, not just the synchronous callback time. The same portable test covers JVM serialization.

If the native JS callback does not await the returned promise, follow the epic's pre-decided fork: drive the endpoint async iterator instead of the callback, or narrow the Handler contract for services and document it. Either outcome is recorded (docs and, if narrowed, the relevant ADR/glossary touch-up).
