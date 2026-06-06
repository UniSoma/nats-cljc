---
id: nts-01ktde2zcap4
title: 'Stream tracer: create + info + delete from a portable config map'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-06T03:02:09.542782854Z'
updated: '2026-06-06T17:43:04.089857579Z'
closed: '2026-06-06T17:43:04.089857579Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- streams
acceptance:
- title: create-stream from a portable closed kebab config map (keyword enums, ms-in-key durations) creates the stream on both legs; round-tripped stream-info returns a normalized kebab map with ISO-8601 timestamps
  done: true
- title: delete-stream removes it; a subsequent info/delete surfaces :stream-not-found
  done: true
- title: An unknown config key raises validation :unknown-config-key and a malformed name raises :invalid-name, both pre-flight (no native call)
  done: true
- title: A server-rejected config surfaces operational :jetstream-api-error with {:code :description}
  done: true
- title: Deep-module unit test (no server) covers config kebab<->native round-trips and closed-key rejection; portable integration test passes on JVM + Node
  done: true
deps:
- nts-01ktde2z9786
---

## Description

First Stream tracer through every layer. Create a Stream from a portable, CLOSED kebab-keyword config map and read its normalized info back, then delete it, proving the stream config-translation deep module end-to-end on both legs. Config uses keyword enums (:storage :file|:memory, :retention :limits|:interest|:work-queue) and durations as integer milliseconds with the unit in the key (:max-age-ms), translated to Duration on the JVM and Nanos on CLJS. The map is closed: an unrecognized/misspelled key is the validation error :unknown-config-key and a malformed name is :invalid-name, both raised pre-flight on the operation's own channel before any native call (ADR 0015). StreamInfo is returned as a curated, normalized kebab map (not raw native passthrough) with ISO-8601 timestamps. A missing stream is operational :stream-not-found; a config the SERVER rejects (e.g. illegal subject overlap) is operational :jetstream-api-error carrying {:code :description}, not validation, because it is detected after the native call (ADR 0020). Build the config-translation + closed-key-validation deep module with a no-server unit test (kebab<->native round-trips + closed-key rejection) alongside the portable integration test. Covers user stories 3, 5, 7, 36, 37.

## Notes

**2026-06-06T17:43:04.089857579Z**

Stream tracer landed: portable create-stream/stream-info/delete-stream over a closed kebab config map. Pre-flight validation raises :unknown-config-key/:invalid-name before any native call (ADR 0015); enum keywords + ms durations translate to/from jnats StreamConfiguration (Duration) and nats.js StreamConfig (Nanos) through shared wire tables; round-tripped stream-info normalizes to a kebab map with an ISO-8601 :created. Server rejections surface operational :stream-not-found (err 10059) and :jetstream-api-error carrying {:code :description} (ADR 0020). Deep-module round-trip unit + portable integration tests pass on JVM + Node.
