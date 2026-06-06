---
id: nts-01ktde2zr4f0
title: 'Pull tracer: next + fetch deliver pure-data messages with :js metadata'
status: open
type: feature
priority: 1
mode: afk
created: '2026-06-06T03:02:09.917488457Z'
updated: '2026-06-06T03:02:09.917488457Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- pull
acceptance:
- title: fetch returns a Promise<vector> of up to N pure-data messages {:subject :data :headers :js {...}} on both legs
  done: false
- title: next returns a Promise of one message, or nil when the consumer is empty
  done: false
- title: Delivered messages are pure data (no native object); :js carries stream/consumer/stream-seq/delivery-seq/delivered/pending/redelivered/domain/ack-subject with an ISO-8601 :timestamp
  done: false
- title: Portable integration test (acked-publish then pull) passes on JVM + Node
  done: false
deps:
- nts-01ktde2zjhpf
- nts-01ktde2znb9v
---

## Description

First pull-delivery tracer: read messages from a Consumer as pure data. next returns a Promise of a single message or nil (poll one with a timeout); fetch returns a Promise of a bounded vector of up to N messages (ADR 0018). Build the per-leg js-msg->raw lift (the JetStream counterpart to the core msg->raw): it reads native JetStream metadata via each client's native accessor and CAPTURES THE ACK-SUBJECT STRING, then discards the native object, so the delivered message is pure data {:subject :data :headers :js {...}} (ADR 0019). The :js map carries {:stream :consumer :stream-seq :delivery-seq :delivered :pending :redelivered :timestamp :domain :ack-subject} with an ISO-8601 :timestamp and :redelivered = (delivered > 1). Populate the stream deterministically with Acked publish, then assert delivery, message purity, and :js fidelity. Covers user stories 20, 21, 25, 26.
