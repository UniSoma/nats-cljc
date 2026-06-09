---
id: nts-01ktmkk6hxwc
title: 'Consumer verb: update-consumer (explicit, mirrors update-stream)'
status: in_progress
type: feature
priority: 2
mode: afk
created: '2026-06-08T21:53:05.081326277Z'
updated: '2026-06-09T23:01:08.924345367Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
acceptance:
- title: update-consumer changes an existing durable's mutable config on both legs; rejects when the consumer is absent
  done: false
- title: Portable integration test passes on JVM + Node
  done: false
links:
- nts-01ktde2zjhpf
---

## Description

create-consumer is create-only on both legs (jnats .createConsumer / nats.js consumers.add action=create), matching the Stream sibling and ADR-37's decouple-creation-from-consumption + anti-clobber rationale. Config updates are therefore a deliberate, separate verb. (jet/update-consumer ctx stream config) updates an existing durable's config, rejecting :consumer-not-found if absent (jnats .updateConsumer / nats.js consumers.update action=update). Mirrors the planned update-stream in nts-01ktde2zfmb8.
