---
id: nts-01ktde2zjhpf
title: 'Consumer tracer: durable consumer CRUD + info/list'
status: open
type: feature
priority: 1
mode: afk
created: '2026-06-06T03:02:09.741599863Z'
updated: '2026-06-06T03:02:09.741599863Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- consumers
acceptance:
- title: create-consumer builds a durable consumer from a portable config map (ack-policy, deliver-policy, ack-wait-ms, max-deliver, filter subjects) on both legs
  done: false
- title: consumer-info returns a normalized map with delivered/ack-floor/pending and ISO-8601 timestamps; list-consumers and consumer-names enumerate them
  done: false
- title: delete-consumer removes it; a subsequent info surfaces :consumer-not-found
  done: false
- title: Unknown config key / malformed name raise pre-flight validation types; deep-module unit test covers consumer-config round-trips
  done: false
- title: Portable integration test passes on JVM + Node
  done: false
deps:
- nts-01ktde2zcap4
---

## Description

Durable pull Consumer management through every layer, reusing and extending the config-translation + validation + error modules from the stream tracer. create-consumer builds a durable Consumer on a Stream from a portable closed kebab config map: :ack-policy, :deliver-policy (keyword enums), :ack-wait-ms (ms-in-key), :max-deliver, and filter subjects. delete-consumer removes a delivery cursor. consumer-info returns a normalized kebab map exposing delivery progress (delivered, ack-floor, pending) with ISO-8601 timestamps; list-consumers / consumer-names enumerate a Stream's consumers. A missing consumer is operational :consumer-not-found; unknown keys / malformed names are pre-flight validation (:unknown-config-key / :invalid-name). Extend the deep-module unit test with the consumer-config round-trips. Covers user stories 9, 10, 11.
