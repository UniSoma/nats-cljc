---
id: nts-01ktwykhxs3e
title: 'Example: Limits-based Stream (jetstream.limits-stream)'
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:26.521106241Z'
updated: '2026-06-12T03:39:26.521106241Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/jetstream/limits-stream/go to nats-cljc.

File: examples/examples/jetstream/limits_stream.cljc (stub scaffolded; implement -main).
Surface: create-stream :retention :limits / :max-age-ms, update-stream, purge/delete (NOTE: upstream also demos max-msgs/max-bytes — known config gap, see umbrella note)
Run: `bb example:jvm jetstream.limits-stream` / `bb example:node jetstream.limits-stream` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).
