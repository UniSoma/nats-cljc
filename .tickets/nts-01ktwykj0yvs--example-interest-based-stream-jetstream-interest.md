---
id: nts-01ktwykj0yvs
title: 'Example: Interest-based Stream (jetstream.interest-stream)'
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:26.622144757Z'
updated: '2026-06-12T03:39:26.622144757Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/jetstream/interest-stream/go to nats-cljc.

File: examples/examples/jetstream/interest_stream.cljc (stub scaffolded; implement -main).
Surface: create-stream :retention :interest + consumer-driven retention
Run: `bb example:jvm jetstream.interest-stream` / `bb example:node jetstream.interest-stream` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).
