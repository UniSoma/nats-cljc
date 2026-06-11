---
id: nts-01ktsqp3v5vc
title: 'History archaeology: revision-pinned get and keys'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:47.072394769Z'
updated: '2026-06-11T00:47:43.541390714Z'
closed: '2026-06-11T00:47:43.541390714Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: get with :revision resolves to the Entry at that exact past Revision
  done: true
- title: get pinned to a delete/purge marker revision delivers the marker Entry with its :operation visible, identically on both legs (pinned by test)
  done: true
- title: keys resolves to a vector of live key strings; deleted and purged keys are excluded
  done: true
- title: keys accepts a subject-style filter restricting the result
  done: true
- title: Portable facade tests pass on both legs
  done: true
deps:
- nts-01ktsqnbtp7t
---

## Description

Cheap archaeology on a Bucket. `(get bucket key {:revision n})` fetches the Entry at an exact past Revision — including delete/purge markers, delivered as marker Entries with their `:operation` visible rather than hidden. Each native's behavior on a marker revision is unverified; normalize both legs to deliver-the-marker-Entry and pin it with a test (the epic's verify-first note).

`(keys bucket)` with an optional subject-style filter resolves to a fully-realized vector of key strings, deleted/purged keys excluded, so a Bucket's live contents are enumerable (precedent: stream-names / consumer-names).

## Notes

**2026-06-11T00:47:43.541390714Z**

Shipped revision-pinned get and keys on the KV facade. (get bucket key {:revision n}) reads the Entry at that exact past Revision; pinned to a delete/purge marker revision it delivers the marker Entry with :operation visible and :value nil on both legs, normalized per the epic's verify-first note: nats.js' revision get surfaces the marker natively, while jnats hides it behind null, so the JVM leg reconstructs it from the backing stream's message via the public KeyValueEntry(MessageInfo) constructor (JetStreamManagement carried on the Bucket handle). An unassigned or other-key Revision reads as absent (nil) on both legs (10037 normalized on the JVM; verified null on nats.js). (keys bucket filter?) resolves to a fully-realized vector of live key strings — deleted/purged excluded natively on both legs (verified) — with an optional subject-style filter, no match resolving to []. -kv-get gained the revision param, -kv-keys joined BucketEntries, and the CLJS QueuedIterator drain was factored into drain-qi shared by history/keys. Pinned by tests on both legs; full suite green on JVM and Node; clj-kondo clean. Commit ee0923a.
