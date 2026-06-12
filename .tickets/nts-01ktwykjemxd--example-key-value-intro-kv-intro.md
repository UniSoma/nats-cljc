---
id: nts-01ktwykjemxd
title: 'Example: Key-Value Intro (kv.intro)'
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:27.059941043Z'
updated: '2026-06-12T03:39:27.059941043Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/kv/intro/go to nats-cljc.

File: examples/examples/kv/intro.cljc (stub scaffolded; implement -main).
Surface: kv buckets, put/get/delete, CAS update via Revision, history, watch
Run: `bb example:jvm kv.intro` / `bb example:node kv.intro` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).
