---
id: nts-01ktde2zv0fm
title: 'consume: promise-return backpressure + drainable handle'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-06T03:02:10.012551632Z'
updated: '2026-06-09T19:59:23.058048387Z'
closed: '2026-06-09T19:59:23.058048387Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- pull
acceptance:
- title: consume delivers via a promise-return handler and returns a drainable/unsubscribable handle on both legs
  done: true
- title: A slow promise-returning handler measurably gates the pull rate (backpressure observed in timing)
  done: true
- title: Refill knobs (:batch, :threshold as a count, :expires-ms, :idle-heartbeat-ms, :max-bytes) are accepted portably; :threshold converts count->percent on the JVM
  done: true
- title: No :slow-consumer is ever raised by pull; refill-decision deep-module unit test (no server) passes
  done: true
- title: Portable integration test passes on JVM + Node
  done: true
deps:
- nts-01ktde2zr4f0
---

## Description

Continuous, backpressured delivery reusing the core handler contract (ADR 0007/0018). consume takes a (fn [msg] ...) that may return a promise; the runtime waits for that promise to settle before delivering the next message and refilling, giving per-message backpressure with NO async dependency: the client's read rate gates its own pull rate. consume returns a drainable/unsubscribable handle, exactly like a core Subscription. Refill knobs: :batch (max messages per pull window), :threshold (refill when the buffered COUNT drops below it; portable unit is a count, the JVM converts count->percent), :expires-ms, :idle-heartbeat-ms, :max-bytes (ADR 0018). Build the refill-decision deep module (buffered/threshold/batch -> pull amount) with a no-server unit test. There is NO :slow-consumer / :max-pending in pull: a slow handler simply slows the pull; flow control is the knobs plus the handler's promise. Covers user stories 18, 19, 22, 23.

## Notes

**2026-06-09T19:29:17.529501419Z**

TDD slices 1-2 done (uncommitted): refill deep module nats-cljc.jetstream.refill (threshold->percent: P=ceil(100*(batch-T)/batch), jnats repulls at pending<=batch-max(1,batch*P/100); validate-opts -> :invalid-threshold/:invalid-expires) + jet/consume tracer (promise-of-handle; -js-consume in JetStreamData; JVM ConsumerContext.consume + blocking onMessage road-2, CLJS consume() async-iterable drive loop; handle = Drainable/Sub, drain settles via isFinished poll on JVM / close() on CLJS). Gotchas: jnats BaseConsumeOptions$Builder is PROTECTED -> thresholdPercent/expiresIn need Clojure 1.12 param-tagged calls through ConsumeOptions$Builder; ns named *.consume clashes with var jet/consume on CLJS (hence refill); :idle-heartbeat-ms accepted but jnats derives cadence from expires. Remaining slices: backpressure test, unsubscribe (max -> :invalid-max), all-knobs+refill-across-batches test, full suites + commit. Lint clean, JVM 149/393 + Node 123/313 green.

**2026-06-09T19:59:23.058048387Z**

consume landed: promise-of-handle continuous pull on both legs (ADR 0018). Handler gets pure-data {:subject :data :js} one at a time; a returned promise gates delivery+refill (road-2: JVM blocking onMessage / CLJS drive-loop awaits before next .next), so a slow handler slows the pull and no :slow-consumer is ever raised. Handle is a core Subscription's shape — core/drain settles once the open pull winds down (bounded by :expires-ms; JVM delivers buffered first, CLJS discards), core/unsubscribe stops abruptly+idempotently and rejects any :max with :invalid-max. Refill deep module nats-cljc.jetstream.refill: threshold->percent (P=ceil(100*(batch-T)/batch)) + validate-opts. KEY DECISION (verify-don't-infer caught it): the byte window (:max-bytes) and the message-count window (:batch/:threshold) are MUTUALLY EXCLUSIVE — nats.js forbids a user setting max_messages+max_bytes, so the portable contract is their intersection (:exclusive-window pre-flight on both legs); the JS ->consume-options omits max_messages for a byte window. Backpressure test uses the default (buffered) batch so msg2 delivers causally on settle (a :batch 1 design delivered msg2 at ~3x expires via jnats pull-cycling, not the gate — slow+misleading). Tests: deep-module-refill-decision (incl. exclusivity + byte-window), consume-applies-backpressure, consume-unsubscribe-stops-and-rejects-max, consume-refill-across-batches, consume-byte-window-delivers. Lint 0/0, JVM 153/409, Node 127/329, 0 shadow warnings. Follow-up nts-01ktde300gz3 (per-consume :on-error) still open — handler/decode throws are caught-and-swallowed in both delivery loops.
