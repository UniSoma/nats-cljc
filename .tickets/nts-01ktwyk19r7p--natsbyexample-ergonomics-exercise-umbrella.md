---
id: nts-01ktwyk19r7p
title: natsbyexample ergonomics exercise (umbrella)
status: open
type: epic
priority: 2
mode: hitl
created: '2026-06-12T03:39:09.496708442Z'
updated: '2026-06-23T19:34:15.646097214Z'
tags:
- examples
links:
- nts-01kvtzksb69t
---

## Description

Port the implementable https://natsbyexample.com examples to nats-cljc as runnable, portable .cljc programs under examples/, to (a) exercise the public API's ergonomics first-hand and (b) seed committed repository examples.

## Division of labor

- Jonas hand-writes every example (the point is feeling the ergonomics) and logs friction as notes on the example's ticket.
- The agent stays hands-off the example code: it proposed the list, scaffolded the harness (examples/ tree, :examples deps alias, shadow :examples node-script build, bb example:jvm / example:node tasks), tracks progress here, and in phase 2 processes the notes into a cross-example synthesis to grill on.

## Workflow per example

1. `knot start` the child ticket; implement in its stub file.
2. Done when it runs to completion on BOTH legs (`bb example:jvm <name>` and `bb example:node <name>`) against the local ci/nats.conf server (TCP :4222 / ws :8080, JetStream-enabled), printing the upstream example's narrative as it goes, and cleaning up its streams/buckets/consumers so re-runs are idempotent.
3. Friction notes use tagged prefixes so phase-2 synthesis can aggregate: `gap:` (API can't express it), `wart:` (expressible but awkward), `doc:` (couldn't find/understand from docs), `win:` (beats the upstream version). Untagged prose is fine too.
4. Close the child; the agent processes notes when a category completes.

## Advisory order

messaging (pub-sub, request-reply, json-payloads) first to shake out the harness, then jetstream (streams before consumers before acks), then kv, services, and the blocking-core JVM-only exception last.

## Out of scope (decided at grilling)

Push/queue-push/multi-stream consumers (lib is pull-only by design), API migration, object store (no surface), pull-consumer-limits (config gap — pre-filed as a finding), concurrent message processing (platform dispatch semantics, not API), protobuf payloads (dep weight; JSON already exercises codec swap), subject-mapped partitions and everything server-side (auth configs, topologies, embedded, integrations, operations).

Examples mirror upstream slugs/structure on purpose, so a reader can diff ours against the Go/Python versions side-by-side.

## Notes

**2026-06-12T03:39:35.715376054Z**

gap: (pre-filed at grilling, before any implementation) The closed stream-config map is #{:name :subjects :storage :retention :max-age-ms} — no :max-msgs / :max-bytes, so the limits-stream example cannot demo two of upstream's three limits. Same family: the consumer-config map #{:name :durable? :ack-policy :deliver-policy :ack-wait-ms :max-deliver :filter-subjects} has no :max-batch / :max-expires, which is why the 'Pull Consumer - Applying Limits' upstream example was excluded outright.
