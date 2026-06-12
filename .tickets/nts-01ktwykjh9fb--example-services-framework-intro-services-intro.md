---
id: nts-01ktwykjh9fb
title: 'Example: Services Framework Intro (services.intro)'
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:27.144973126Z'
updated: '2026-06-12T03:39:27.144973126Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/services/intro/go to nats-cljc.

File: examples/examples/services/intro.cljc (stub scaffolded; implement -main).
Surface: service/create, endpoints, Discovery (ping/info/stats)
Run: `bb example:jvm services.intro` / `bb example:node services.intro` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).
