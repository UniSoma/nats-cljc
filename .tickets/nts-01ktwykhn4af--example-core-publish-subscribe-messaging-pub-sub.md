---
id: nts-01ktwykhn4af
title: 'Example: Core Publish-Subscribe (messaging.pub-sub)'
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:26.244407237Z'
updated: '2026-06-12T03:39:26.244407237Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/messaging/pub-sub/cli to nats-cljc.

File: examples/examples/messaging/pub_sub.cljc (stub scaffolded; implement -main).
Surface: core publish / subscribe / unsubscribe
Run: `bb example:jvm messaging.pub-sub` / `bb example:node messaging.pub-sub` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).
