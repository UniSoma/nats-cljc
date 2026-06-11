---
id: nts-01ktsqp3v5vc
title: 'History archaeology: revision-pinned get and keys'
status: in_progress
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:47.072394769Z'
updated: '2026-06-11T00:32:41.277318902Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: get with :revision resolves to the Entry at that exact past Revision
  done: false
- title: get pinned to a delete/purge marker revision delivers the marker Entry with its :operation visible, identically on both legs (pinned by test)
  done: false
- title: keys resolves to a vector of live key strings; deleted and purged keys are excluded
  done: false
- title: keys accepts a subject-style filter restricting the result
  done: false
- title: Portable facade tests pass on both legs
  done: false
deps:
- nts-01ktsqnbtp7t
---

## Description

Cheap archaeology on a Bucket. `(get bucket key {:revision n})` fetches the Entry at an exact past Revision — including delete/purge markers, delivered as marker Entries with their `:operation` visible rather than hidden. Each native's behavior on a marker revision is unverified; normalize both legs to deliver-the-marker-Entry and pin it with a test (the epic's verify-first note).

`(keys bucket)` with an optional subject-style filter resolves to a fully-realized vector of key strings, deleted/purged keys excluded, so a Bucket's live contents are enumerable (precedent: stream-names / consumer-names).
