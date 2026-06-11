---
id: nts-01ktsqp89yz9
title: 'Watch refinements: :keys filtering, :ignore-deletes?, :on-error routing'
status: in_progress
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:51.636126494Z'
updated: '2026-06-11T01:03:35.428203550Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: :keys with a single pattern delivers only matching keys; multiple patterns deliver their union
  done: false
- title: :ignore-deletes? true suppresses delete/purge Entries; the default delivers them with :operation visible
  done: false
- title: A watch decode failure routes to the watch's :on-error when set, else to the connection's :on-status as an :error event
  done: false
- title: Portable facade tests pass on both legs
  done: false
deps:
- nts-01ktsqnjj6e1
- nts-01ktsqnbtp7t
---

## Description

The remaining Watch options. `:keys` restricts a Watch to one or many subject-style key patterns, so consumers observe only the keys they care about. `:ignore-deletes?` suppresses Tombstone and purge-marker deliveries, letting cache-maintenance and event-log consumers each pick their semantics.

A decode failure on a watch delivery routes to the watch's `:on-error` if set, else to the connection's `:on-status` as an `:error` event — the established sink semantics (ADR 0007), identical to core subscriptions and jetstream consume.
