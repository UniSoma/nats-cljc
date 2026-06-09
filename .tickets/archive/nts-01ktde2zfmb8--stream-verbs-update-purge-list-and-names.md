---
id: nts-01ktde2zfmb8
title: 'Stream verbs: update + purge + list (and names)'
status: closed
type: feature
priority: 2
mode: afk
created: '2026-06-06T03:02:09.648894380Z'
updated: '2026-06-09T23:00:57.897478859Z'
closed: '2026-06-09T23:00:57.897478859Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- streams
acceptance:
- title: update-stream changes config on an existing stream (verified via stream-info) on both legs
  done: true
- title: purge-stream drops messages but keeps the stream (info shows 0 messages, stream still exists)
  done: true
- title: list-streams returns normalized StreamInfo maps and stream-names returns the names
  done: true
- title: Portable integration test passes on JVM + Node
  done: true
deps:
- nts-01ktde2zcap4
---

## Description

Complete Stream management on the config/error foundation from the stream tracer. update-stream changes an existing stream's config (e.g. retention or limits) without recreating it; purge-stream drops a stream's messages while keeping its definition; list-streams returns normalized StreamInfo maps and stream-names returns the names, for server-side discovery. Same portable closed kebab maps, keyword enums, ms-in-key durations, and ISO-8601 normalization as the tracer. Covers user stories 4, 6, 8.

## Notes

**2026-06-09T23:00:57.897478859Z**

Completed the Stream management surface: update-stream (merge semantics — keys present override, absent keys keep current values, with the JVM leg reproducing nats.js' read-merge-write so both legs behave identically), purge-stream (resolves {:purged <count>}, stream definition survives), list-streams (normalized StreamInfo maps), and stream-names (each leg's dedicated names endpoint). Added the four StreamManager protocol methods, facade verbs with the established pre-flight validation chains, JVM jnats and CLJS @nats-io/jetstream implementations (generalizing the CLJS lister drain to a shared normalize-fn helper), and three portable integration tests against the :4222 server with :memory streams. clj-kondo clean; JVM leg (163 tests / 440 assertions) and Node leg (134 tests / 354 assertions) both green; a red-check confirmed a non-merging update would fail the test (server rejects with 10052). Files: src/nats_cljc/protocol.cljc, src/nats_cljc/jetstream.cljc, src/nats_cljc/jetstream/impl/jvm.clj, src/nats_cljc/jetstream/impl/js.cljs, test/nats_cljc/jetstream_test.cljc. Ticket left in_progress for the verifier.
