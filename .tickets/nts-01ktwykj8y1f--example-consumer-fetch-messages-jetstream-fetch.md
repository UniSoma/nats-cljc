---
id: nts-01ktwykj8y1f
title: 'Example: Consumer - Fetch Messages (jetstream.fetch-messages)'
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:26.878037981Z'
updated: '2026-06-12T03:39:26.878037981Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/jetstream/consumer-fetch-messages/java to nats-cljc.

File: examples/examples/jetstream/fetch_messages.cljc (stub scaffolded; implement -main).
Surface: fetch batch semantics (:batch / :max-bytes / :expires-ms)
Run: `bb example:jvm jetstream.fetch-messages` / `bb example:node jetstream.fetch-messages` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).
