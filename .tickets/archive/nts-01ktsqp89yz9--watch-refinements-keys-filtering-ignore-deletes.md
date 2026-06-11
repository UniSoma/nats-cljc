---
id: nts-01ktsqp89yz9
title: 'Watch refinements: :keys filtering, :ignore-deletes?, :on-error routing'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:51.636126494Z'
updated: '2026-06-11T01:19:52.596106380Z'
closed: '2026-06-11T01:19:52.596106380Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: :keys with a single pattern delivers only matching keys; multiple patterns deliver their union
  done: true
- title: :ignore-deletes? true suppresses delete/purge Entries; the default delivers them with :operation visible
  done: true
- title: A watch decode failure routes to the watch's :on-error when set, else to the connection's :on-status as an :error event
  done: true
- title: Portable facade tests pass on both legs
  done: true
deps:
- nts-01ktsqnjj6e1
- nts-01ktsqnbtp7t
---

## Description

The remaining Watch options. `:keys` restricts a Watch to one or many subject-style key patterns, so consumers observe only the keys they care about. `:ignore-deletes?` suppresses Tombstone and purge-marker deliveries, letting cache-maintenance and event-log consumers each pick their semantics.

A decode failure on a watch delivery routes to the watch's `:on-error` if set, else to the connection's `:on-status` as an `:error` event — the established sink semantics (ADR 0007), identical to core subscriptions and jetstream consume.

## Notes

**2026-06-11T01:19:52.596106380Z**

Shipped the remaining Watch options on both legs. :keys takes one subject-style pattern or a vector (union), unvalidated like the keys filter; jnats watch(List,...) / nats.js key:string[] underneath, with a CLJS filtered live-key probe so a pattern matching nothing still resolves :initialized. :ignore-deletes? true suppresses delete AND purge Entries portably — native IGNORE_DELETE on the JVM, our own loop filter on CLJS (nats.js' ignoreDeletes skips only DEL and would swallow the delta-0 initialized boundary); the default delivers markers with :operation visible. :on-error is the per-Watch sink with core-subscription semantics (ADR 0006/0007): decode failure, throwing handler, or rejecting handler promise routes there when set, else to the connection's :on-status as an :error event (strict override), the Watch surviving — Bucket handles now carry on-status for the fallback, and -kv-watch takes a normalized opts map. Three new portable facade tests; JVM and Node suites green, kondo clean. Commit 22801dc.
