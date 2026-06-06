---
id: nts-01ktde2zfmb8
title: 'Stream verbs: update + purge + list (and names)'
status: open
type: feature
priority: 2
mode: afk
created: '2026-06-06T03:02:09.648894380Z'
updated: '2026-06-06T03:02:09.648894380Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- streams
acceptance:
- title: update-stream changes config on an existing stream (verified via stream-info) on both legs
  done: false
- title: purge-stream drops messages but keeps the stream (info shows 0 messages, stream still exists)
  done: false
- title: list-streams returns normalized StreamInfo maps and stream-names returns the names
  done: false
- title: Portable integration test passes on JVM + Node
  done: false
deps:
- nts-01ktde2zcap4
---

## Description

Complete Stream management on the config/error foundation from the stream tracer. update-stream changes an existing stream's config (e.g. retention or limits) without recreating it; purge-stream drops a stream's messages while keeping its definition; list-streams returns normalized StreamInfo maps and stream-names returns the names, for server-side discovery. Same portable closed kebab maps, keyword enums, ms-in-key durations, and ISO-8601 normalization as the tracer. Covers user stories 4, 6, 8.
