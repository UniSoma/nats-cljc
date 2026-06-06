---
id: nts-01ktde2zv0fm
title: 'consume: promise-return backpressure + drainable handle'
status: open
type: feature
priority: 1
mode: afk
created: '2026-06-06T03:02:10.012551632Z'
updated: '2026-06-06T03:02:10.012551632Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- pull
acceptance:
- title: consume delivers via a promise-return handler and returns a drainable/unsubscribable handle on both legs
  done: false
- title: A slow promise-returning handler measurably gates the pull rate (backpressure observed in timing)
  done: false
- title: Refill knobs (:batch, :threshold as a count, :expires-ms, :idle-heartbeat-ms, :max-bytes) are accepted portably; :threshold converts count->percent on the JVM
  done: false
- title: No :slow-consumer is ever raised by pull; refill-decision deep-module unit test (no server) passes
  done: false
- title: Portable integration test passes on JVM + Node
  done: false
deps:
- nts-01ktde2zr4f0
---

## Description

Continuous, backpressured delivery reusing the core handler contract (ADR 0007/0018). consume takes a (fn [msg] ...) that may return a promise; the runtime waits for that promise to settle before delivering the next message and refilling, giving per-message backpressure with NO async dependency: the client's read rate gates its own pull rate. consume returns a drainable/unsubscribable handle, exactly like a core Subscription. Refill knobs: :batch (max messages per pull window), :threshold (refill when the buffered COUNT drops below it; portable unit is a count, the JVM converts count->percent), :expires-ms, :idle-heartbeat-ms, :max-bytes (ADR 0018). Build the refill-decision deep module (buffered/threshold/batch -> pull amount) with a no-server unit test. There is NO :slow-consumer / :max-pending in pull: a slow handler simply slows the pull; flow control is the knobs plus the handler's promise. Covers user stories 18, 19, 22, 23.
