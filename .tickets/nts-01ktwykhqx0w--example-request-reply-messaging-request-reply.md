---
id: nts-01ktwykhqx0w
title: 'Example: Request-Reply (messaging.request-reply)'
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:26.333263935Z'
updated: '2026-06-12T03:39:26.333263935Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/messaging/request-reply/go to nats-cljc.

File: examples/examples/messaging/request_reply.cljc (stub scaffolded; implement -main).
Surface: core request / reply / :queue groups
Run: `bb example:jvm messaging.request-reply` / `bb example:node messaging.request-reply` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).
