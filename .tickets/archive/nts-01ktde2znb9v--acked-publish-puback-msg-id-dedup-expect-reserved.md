---
id: nts-01ktde2znb9v
title: 'Acked publish: PubAck, :msg-id dedup, :expect, reserved-header guard'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-06T03:02:09.831566820Z'
updated: '2026-06-06T20:15:03.216676278Z'
closed: '2026-06-06T20:15:03.216676278Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- publish
acceptance:
- title: publish resolves to a normalized PubAck {:stream :seq :duplicate :domain} on both legs
  done: true
- title: Re-publishing with the same :msg-id within the dedup window returns :duplicate true
  done: true
- title: An :expect whose :last-seq is wrong rejects with :wrong-last-sequence
  done: true
- title: A reserved Nats-* key in user :headers is rejected pre-flight as :reserved-header; :timeout-ms and a per-call :codec override both take effect
  done: true
- title: Portable integration test passes on JVM + Node
  done: true
deps:
- nts-01ktde2zcap4
---

## Description

Acked publish into a Stream through every layer. (publish js-ctx subject data opts) returns a Promise<PubAck> where PubAck is the normalized {:stream :seq :duplicate :domain}. Opts: :msg-id (server-side dedup within the dedup window; the PubAck :duplicate is true on a retry); :expect {:last-seq :last-msg-id :stream :last-subject-seq} (optimistic-concurrency/dedup, a mismatch surfaced as operational :wrong-last-sequence; ADR 0020); :timeout-ms (a missing PubAck rejects rather than hangs); :codec (per-call override, only :data is codec'd). :msg-id/:expect are the SANCTIONED way to set reserved Nats-* headers; a reserved Nats-* key set directly in user :headers is rejected pre-flight as validation :reserved-header (ADR 0015/0020). Impl-time grounding: prefer the native publishAsync path on the JVM; fall back to off-thread sync publish if its in-flight cap misbehaves. Covers user stories 12-17.

## Notes

**2026-06-06T20:15:03.216676278Z**

Acked publish landed end-to-end on both legs: (jet/publish ctx subject data opts) returns a promise of the normalized PubAck {:stream :seq :duplicate :domain}. New JetStreamData protocol (-js-publish) carries the data plane, extended per-leg from the JetStream-only impls (ADR 0016). The context now captures the connection's default codec so :data is encoded with it unless a per-call :codec overrides (ADR 0011). New pure pub deep module guards reserved Nats-* user headers pre-flight (:reserved-header, ADR 0015); facade pre-flight chains guard -> normalize -> encode so failures reject the promise. Per-leg ->publish-options maps :msg-id/:expect/:timeout-ms to native options (Duration vs ms), and ->pub-ack normalizes the ack (:domain nil when absent). Wrong :expect -> :wrong-last-sequence (err 10071): the JVM unwraps CompletionException>RuntimeException>JetStreamApiException via .handle, CLJS reuses api-error. msg-id dedup -> :duplicate true. Renamed the pure module to nats-cljc.jetstream.pub to avoid a CLJS ns-var clash with the facade's publish var. Unit + integration tests pass on JVM (119/272) + Node (93/193); clj-kondo clean; advanced-compile externs gate clean.
