---
id: nts-01ktmkk6hxwc
title: 'Consumer verb: update-consumer (explicit, mirrors update-stream)'
status: closed
type: feature
priority: 2
mode: afk
created: '2026-06-08T21:53:05.081326277Z'
updated: '2026-06-09T23:15:43.083084446Z'
closed: '2026-06-09T23:15:43.083084446Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
acceptance:
- title: update-consumer changes an existing durable's mutable config on both legs; rejects when the consumer is absent
  done: true
- title: Portable integration test passes on JVM + Node
  done: true
links:
- nts-01ktde2zjhpf
---

## Description

create-consumer is create-only on both legs (jnats .createConsumer / nats.js consumers.add action=create), matching the Stream sibling and ADR-37's decouple-creation-from-consumption + anti-clobber rationale. Config updates are therefore a deliberate, separate verb. (jet/update-consumer ctx stream config) updates an existing durable's config, rejecting :consumer-not-found if absent (jnats .updateConsumer / nats.js consumers.update action=update). Mirrors the planned update-stream in nts-01ktde2zfmb8.

## Notes

**2026-06-09T23:15:43.083084446Z**

Added jet/update-consumer, the explicit consumer-config update verb mirroring update-stream. The facade (src/nats_cljc/jetstream.cljc) validates the stream name and the closed consumer config pre-flight, then dispatches to a new -update-consumer ConsumerManager protocol method. The CLJS leg rides nats.js consumers.update (which natively read-merges over the current config); the JVM leg reproduces the read-merge-write via a ConsumerConfiguration builder seeded from getConsumerInfo, sharing key application with create through a new apply-consumer-config. A missing durable rejects :consumer-not-found on both legs (10014 via the shared error table). Portable integration test consumer-update-changes-config (memory storage, :4222) covers the changed key, the merge keeping absent keys, and the not-found rejection; clj-kondo clean, JVM (164 tests/445 assertions) and Node (135/359) legs green.
