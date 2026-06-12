---
id: nts-01ktwykj3gm6
title: 'Example: Work-queue Stream (jetstream.workqueue-stream)'
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:26.704682954Z'
updated: '2026-06-12T03:39:26.704682954Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/jetstream/workqueue-stream/go to nats-cljc.

File: examples/examples/jetstream/workqueue_stream.cljc (stub scaffolded; implement -main).
Surface: create-stream :retention :work-queue + exactly-one-consumer semantics
Run: `bb example:jvm jetstream.workqueue-stream` / `bb example:node jetstream.workqueue-stream` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).
