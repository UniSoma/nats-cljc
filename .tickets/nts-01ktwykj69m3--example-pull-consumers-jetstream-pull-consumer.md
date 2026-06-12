---
id: nts-01ktwykj69m3
title: 'Example: Pull Consumers (jetstream.pull-consumer)'
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:26.793269551Z'
updated: '2026-06-12T03:39:26.793269551Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/jetstream/pull-consumer/go to nats-cljc.

File: examples/examples/jetstream/pull_consumer.cljc (stub scaffolded; implement -main).
Surface: create-consumer (durable/ephemeral) + consume / next + ack family
Run: `bb example:jvm jetstream.pull-consumer` / `bb example:node jetstream.pull-consumer` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).
