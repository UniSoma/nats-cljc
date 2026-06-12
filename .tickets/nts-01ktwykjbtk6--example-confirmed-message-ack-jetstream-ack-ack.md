---
id: nts-01ktwykjbtk6
title: 'Example: Confirmed Message Ack (jetstream.ack-ack)'
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:26.970061239Z'
updated: '2026-06-12T03:39:26.970061239Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/jetstream/ack-ack/go to nats-cljc.

File: examples/examples/jetstream/ack_ack.cljc (stub scaffolded; implement -main).
Surface: double-ack vs fire-and-forget ack
Run: `bb example:jvm jetstream.ack-ack` / `bb example:node jetstream.ack-ack` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).
