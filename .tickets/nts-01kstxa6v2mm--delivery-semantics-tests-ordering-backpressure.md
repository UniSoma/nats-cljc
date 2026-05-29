---
id: nts-01kstxa6v2mm
title: Delivery-semantics tests (ordering + backpressure)
status: open
type: task
priority: 1
mode: afk
created: '2026-05-29T22:22:40.993654794Z'
updated: '2026-05-29T22:22:40.993654794Z'
tags:
- needs-triage
acceptance:
- title: Single-subscription ordering test passes on all three platforms — messages delivered in publish order
  done: false
- title: A handler that returns a pending promise delays delivery of the next message until that promise settles
  done: false
- title: The suite demonstrates that no cross-subscription ordering guarantee is assumed
  done: false
- title: Handlers never block the underlying client thread or event loop
  done: false
deps:
- nts-01kstx8ysgv5
---

## Description

Pin the subscription delivery contract with a portable suite. Within a single subscription, messages are delivered **in order, one at a time**; there is **no ordering guarantee across** subscriptions. Handlers must never block — to do ordered async work without overrunning, a handler **returns a promise** and delivery of the next message waits for it to settle (backpressure without core.async).

ADR 0007 (serial + ordered per subscription; promise-return backpressure).
