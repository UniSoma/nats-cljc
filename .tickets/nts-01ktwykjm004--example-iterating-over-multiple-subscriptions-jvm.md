---
id: nts-01ktwykjm004
title: 'Example: Iterating Over Multiple Subscriptions (JVM-only) (blocking.iterating-subscriptions)'
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:27.232391540Z'
updated: '2026-06-12T03:39:27.232391540Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/messaging/iterating-multiple-subscriptions/rust to nats-cljc.

File: examples/examples/blocking/iterating_subscriptions.clj (stub scaffolded; implement -main).
Surface: blocking.core subscribe / take-message / messages — the one JVM-only exception (no Node leg; runs via bb example:jvm only)
Run: `bb example:jvm blocking.iterating-subscriptions` / `bb example:node blocking.iterating-subscriptions` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).
