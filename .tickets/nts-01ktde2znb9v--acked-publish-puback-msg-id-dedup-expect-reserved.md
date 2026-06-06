---
id: nts-01ktde2znb9v
title: 'Acked publish: PubAck, :msg-id dedup, :expect, reserved-header guard'
status: open
type: feature
priority: 1
mode: afk
created: '2026-06-06T03:02:09.831566820Z'
updated: '2026-06-06T03:02:09.831566820Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- publish
acceptance:
- title: publish resolves to a normalized PubAck {:stream :seq :duplicate :domain} on both legs
  done: false
- title: Re-publishing with the same :msg-id within the dedup window returns :duplicate true
  done: false
- title: An :expect whose :last-seq is wrong rejects with :wrong-last-sequence
  done: false
- title: A reserved Nats-* key in user :headers is rejected pre-flight as :reserved-header; :timeout-ms and a per-call :codec override both take effect
  done: false
- title: Portable integration test passes on JVM + Node
  done: false
deps:
- nts-01ktde2zcap4
---

## Description

Acked publish into a Stream through every layer. (publish js-ctx subject data opts) returns a Promise<PubAck> where PubAck is the normalized {:stream :seq :duplicate :domain} (DD-5). Opts: :msg-id (server-side dedup within the dedup window; the PubAck :duplicate is true on a retry); :expect {:last-seq :last-msg-id :stream :last-subject-seq} (optimistic-concurrency/dedup, a mismatch surfaced as operational :wrong-last-sequence; ADR 0020); :timeout-ms (a missing PubAck rejects rather than hangs); :codec (per-call override, only :data is codec'd). :msg-id/:expect are the SANCTIONED way to set reserved Nats-* headers; a reserved Nats-* key set directly in user :headers is rejected pre-flight as validation :reserved-header (ADR 0015/0020). Impl-time grounding: prefer the native publishAsync path on the JVM; fall back to off-thread sync publish if its in-flight cap misbehaves. Covers user stories 12-17.
