---
id: nts-01ktvp17f0r7
title: 'Discovery: ping, info, stats — narrowing, bounding, normalized EDN'
status: closed
type: feature
priority: 2
mode: afk
created: '2026-06-11T15:50:22.932715847Z'
updated: '2026-06-11T17:40:37.403682520Z'
closed: '2026-06-11T17:40:37.403682520Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: ping resolves a vector of identity maps for running Services; info adds :description and :endpoints; stats adds :started and per-endpoint counters — kebab EDN, wire type discriminator dropped, identical shape on both legs
  done: true
- title: Discovery narrows by :name and by :id to a specific Service or instance
  done: true
- title: :max-results and :timeout-ms bound the fan-out so the gather terminates predictably
  done: true
- title: 'Stats counters move: handled requests increment the request count, and an error reply (respond-error or thrown handler) increments the endpoint''s error count'
  done: true
- title: :processing-time-ns and :average-processing-time-ns are nanosecond integers; :started is the canonical timestamp string; the per-endpoint custom :data blob arrives as parsed JSON-to-EDN
  done: true
- title: A Service created with zero endpoints is discoverable via ping, info, and stats
  done: true
- title: Portable facade tests pass on both legs
  done: true
deps:
- nts-01ktvnzj8kwp
- nts-01ktvnzz2ejg
---

## Description

The client side of the surface: `(ping conn opts?)`, `(info conn opts?)`, `(stats conn opts?)`, each → Promise<vector>, hanging directly off the Connection (ADR 0024 — no Discovery handle, no local introspection of a hosted Service; self-inspection is a wire request narrowed by `:name`/`:id`). `opts` = `{:name :id :max-results :timeout-ms}`; the bounded $SRV.* fan-out is drained from the native QueuedIterator (JS) / List (JVM) into an EDN vector so the gather terminates predictably.

Normalization, byte-identical across legs: kebab-case EDN with the wire `type` discriminator dropped; durations as `:processing-time-ns` / `:average-processing-time-ns` integers in nanoseconds; `:started` as the canonical UTC-millis timestamp string (same form as KV `:created`); the per-endpoint custom `:data` blob passes through as parsed JSON→EDN, not via the connection codec.

`ping` resolves identity maps; `info` adds `:description` and `:endpoints`; `stats` adds `:started` and per-endpoint counters. The error counter is driven through the error slice's verbs — a respond-error or a throwing handler increments the endpoint's error count (the half of that story deferred from the errors slice). A Service created with zero endpoints is legal and still answers $SRV.* — assert it is discoverable.

## Notes

**2026-06-11T17:40:37.403682520Z**

Shipped client-side Discovery: (ping/info/stats conn opts?) off the Connection (ADR 0024, no Discovery handle), each -> Promise<vector>. New Discovery protocol (-ping/-info/-stats) extended onto each Connection record from the service impls. JVM wraps jnats Discovery(conn, maxTimeMillis, maxResults); JS uses Svcm.client(RequestManyOptions {strategy count}) draining the QueuedIterator. Normalization byte-identical across legs: kebab EDN, wire type discriminator dropped, durations as integer :processing-time-ns/:average-processing-time-ns, :started via canonical instant (KV :created form), custom :data parsed JSON->EDN (JsonValue walker on JVM, js->clj on JS; not the connection codec). name+id narrows to one instance (JVM null / JS no-responders both normalize to empty vector); max-results/timeout-ms bound the fan-out (defaults 5000ms/10 = jnats DEFAULT_DISCOVERY_*). respond-error made terminal (sends reply then throws) so the native counts the endpoint error like an auto-500 -- closes the counter half deferred from the errors slice; single-reply caller still reads the explicit code/description. Zero-endpoint service discoverable with empty :endpoints. Verified: lint clean; JVM 786 / Node 687 assertions 0 failures; bundle:check + externs:check green. Red-before-green watched: Node id-miss first rejected :no-responders before the []-normalization landed. Note: custom :data JSON->EDN is normalization-code-only (create exposes no stats-data-supplier surface), so it is unit-proven via REPL not the facade suite; the rest of that AC (integer durations, canonical :started) is facade-tested.
