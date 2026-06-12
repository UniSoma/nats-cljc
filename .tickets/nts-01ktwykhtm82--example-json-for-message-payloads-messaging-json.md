---
id: nts-01ktwykhtm82
title: 'Example: JSON for Message Payloads (messaging.json-payloads)'
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:26.420166887Z'
updated: '2026-06-12T03:39:26.420166887Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/messaging/json/go to nats-cljc.

File: examples/examples/messaging/json_payloads.cljc (stub scaffolded; implement -main).
Surface: codec.json vs the EDN default
Run: `bb example:jvm messaging.json-payloads` / `bb example:node messaging.json-payloads` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).
