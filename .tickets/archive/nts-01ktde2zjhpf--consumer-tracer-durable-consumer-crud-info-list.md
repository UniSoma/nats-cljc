---
id: nts-01ktde2zjhpf
title: 'Consumer tracer: consumer CRUD (durable + ephemeral) + info/list'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-06T03:02:09.741599863Z'
updated: '2026-06-08T23:15:15.561166803Z'
closed: '2026-06-08T23:15:15.561166803Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- consumers
acceptance:
- title: create-consumer builds a durable consumer from a portable config map (ack-policy, deliver-policy, ack-wait-ms, max-deliver, filter subjects) on both legs
  done: true
- title: consumer-info returns a normalized map with delivered/ack-floor/pending and ISO-8601 timestamps; list-consumers and consumer-names enumerate them
  done: true
- title: delete-consumer removes it; a subsequent info surfaces :consumer-not-found
  done: true
- title: Unknown config key / malformed name raise pre-flight validation types; deep-module unit test covers consumer-config round-trips
  done: true
- title: Portable integration test passes on JVM + Node
  done: true
- title: create-consumer is create-only on both legs (a config-changing re-create rejects identically); config updates are the separate update-consumer verb
  done: true
- title: create-consumer builds an ephemeral consumer with :durable? false (named or server-assigned); :durable? defaults true and a durable requires :name
  done: true
- title: consumer-info round-trips a single-element :filter-subjects on both legs and returns a derived :durable?
  done: true
deps:
- nts-01ktde2zcap4
links:
- nts-01ktmkk6hxwc
---

## Description

Durable pull Consumer management through every layer, reusing and extending the config-translation + validation + error modules from the stream tracer. create-consumer builds a durable Consumer on a Stream from a portable closed kebab config map: :ack-policy, :deliver-policy (keyword enums), :ack-wait-ms (ms-in-key), :max-deliver, and filter subjects. delete-consumer removes a delivery cursor. consumer-info returns a normalized kebab map exposing delivery progress (delivered, ack-floor, pending) with ISO-8601 timestamps; list-consumers / consumer-names enumerate a Stream's consumers. A missing consumer is operational :consumer-not-found; unknown keys / malformed names are pre-flight validation (:unknown-config-key / :invalid-name). Extend the deep-module unit test with the consumer-config round-trips. Covers user stories 9, 10, 11.

## Notes

**2026-06-08T23:15:15.561166803Z**

Consumer CRUD landed durable + ephemeral on both legs. create-consumer is create-only (jnats .createConsumer / nats.js consumers.add action=create), not upsert — a config-changing re-create rejects identically; updates are the separate update-consumer verb (nts-01ktmkk6hxwc). Durability is an explicit :name + :durable? flag (default true); ephemerals set name-only (named or server-assigned), the durable_name-absence discriminant verified symmetric. consumer-info returns a derived :durable?. Single-element :filter-subjects round-trips natively on every supported server (>=2.12 populates the plural field on both clients, verified); finding #2's singular-storage premise holds only pre-2.12, so no coalesce shipped. Finding #6 folded in as :missing-required-key. Cleanups: into-array, with-api-error helper, wire-var docstrings. Recorded in ADR 0021; CONTEXT.md gained Durable/Ephemeral consumer terms. JVM 131/309, Node 104/229, 0 failures.
