---
id: nts-01ktvp17f0r7
title: 'Discovery: ping, info, stats — narrowing, bounding, normalized EDN'
status: open
type: feature
priority: 2
mode: afk
created: '2026-06-11T15:50:22.932715847Z'
updated: '2026-06-11T15:50:22.932715847Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: ping resolves a vector of identity maps for running Services; info adds :description and :endpoints; stats adds :started and per-endpoint counters — kebab EDN, wire type discriminator dropped, identical shape on both legs
  done: false
- title: Discovery narrows by :name and by :id to a specific Service or instance
  done: false
- title: :max-results and :timeout-ms bound the fan-out so the gather terminates predictably
  done: false
- title: 'Stats counters move: handled requests increment the request count, and an error reply (respond-error or thrown handler) increments the endpoint''s error count'
  done: false
- title: :processing-time-ns and :average-processing-time-ns are nanosecond integers; :started is the canonical timestamp string; the per-endpoint custom :data blob arrives as parsed JSON-to-EDN
  done: false
- title: A Service created with zero endpoints is discoverable via ping, info, and stats
  done: false
- title: Portable facade tests pass on both legs
  done: false
deps:
- nts-01ktvnzj8kwp
- nts-01ktvnzz2ejg
---

## Description

The client side of the surface: `(ping conn opts?)`, `(info conn opts?)`, `(stats conn opts?)`, each → Promise<vector>, hanging directly off the Connection (ADR 0024 — no Discovery handle, no local introspection of a hosted Service; self-inspection is a wire request narrowed by `:name`/`:id`). `opts` = `{:name :id :max-results :timeout-ms}`; the bounded $SRV.* fan-out is drained from the native QueuedIterator (JS) / List (JVM) into an EDN vector so the gather terminates predictably.

Normalization, byte-identical across legs: kebab-case EDN with the wire `type` discriminator dropped; durations as `:processing-time-ns` / `:average-processing-time-ns` integers in nanoseconds; `:started` as the canonical UTC-millis timestamp string (same form as KV `:created`); the per-endpoint custom `:data` blob passes through as parsed JSON→EDN, not via the connection codec.

`ping` resolves identity maps; `info` adds `:description` and `:endpoints`; `stats` adds `:started` and per-endpoint counters. The error counter is driven through the error slice's verbs — a respond-error or a throwing handler increments the endpoint's error count (the half of that story deferred from the errors slice). A Service created with zero endpoints is legal and still answers $SRV.* — assert it is discoverable.
