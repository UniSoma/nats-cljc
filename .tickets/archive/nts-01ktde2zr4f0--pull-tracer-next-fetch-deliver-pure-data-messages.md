---
id: nts-01ktde2zr4f0
title: 'Pull tracer: next + fetch deliver pure-data messages with :js metadata'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-06T03:02:09.917488457Z'
updated: '2026-06-09T18:20:39.305112356Z'
closed: '2026-06-09T18:20:39.305112356Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- pull
acceptance:
- title: fetch returns a Promise<vector> of up to N pure-data messages {:subject :data :headers :js {...}} on both legs
  done: true
- title: next returns a Promise of one message, or nil when the consumer is empty
  done: true
- title: Delivered messages are pure data (no native object); :js carries stream/consumer/stream-seq/delivery-seq/delivered/pending/redelivered/domain/ack-subject with an ISO-8601 :timestamp
  done: true
- title: Portable integration test (acked-publish then pull) passes on JVM + Node
  done: true
deps:
- nts-01ktde2zjhpf
- nts-01ktde2znb9v
---

## Description

First pull-delivery tracer: read messages from a Consumer as pure data. next returns a Promise of a single message or nil (poll one with a timeout); fetch returns a Promise of a bounded vector of up to N messages (ADR 0018). Build the per-leg js-msg->raw lift (the JetStream counterpart to the core msg->raw): it reads native JetStream metadata via each client's native accessor and CAPTURES THE ACK-SUBJECT STRING, then discards the native object, so the delivered message is pure data {:subject :data :headers :js {...}} (ADR 0019). The :js map carries {:stream :consumer :stream-seq :delivery-seq :delivered :pending :redelivered :timestamp :domain :ack-subject} with an ISO-8601 :timestamp and :redelivered = (delivered > 1). Populate the stream deterministically with Acked publish, then assert delivery, message purity, and :js fidelity. Covers user stories 20, 21, 25, 26.

## Notes

**2026-06-08T23:34:50.383425301Z**

Pull tracer landed on both legs (TDD, 2 vertical slices). New facade verbs jet/fetch (Promise<vector> up to :batch, default 100) + jet/next (Promise<msg-or-nil>, polls with :expires-ms); next excludes clojure.core/next. Two new JetStreamData protocol methods -js-fetch/-js-next. Per-leg js-msg->raw lift built on the now-public core/msg->raw: (-> (core/msg->raw m) (dissoc :reply) (assoc :js {...})) — reads native metadata, captures the $JS.ACK reply as :js :ack-subject, discards the native object (ADR 0019). :js = {:stream :consumer :stream-seq :delivery-seq :delivered :pending :redelivered :timestamp(ISO) :domain :ack-subject}; :redelivered derived (delivered>1) so legs agree; :domain coerced nil when empty. JVM uses JetStream.getConsumerContext -> ConsumerContext.next(long)/fetch(FetchConsumeOptions)->FetchConsumer.nextMessage, off-thread. CLJS uses js.consumers.get -> Consumer.next({expires})/fetch({max_messages,expires}), draining the async-iterable ConsumerMessages. Facade owns validation + codec decode (decode-js-msg, the JetStream counterpart to core/decode-msg). Tests: pull-fetch-delivers-pure-data + pull-next-delivers-one-or-nil, green on JVM (133 tests) + Node (106). Externs gate + lint clean.

**2026-06-09T18:20:39.305112356Z**

Pull tracer landed: jet/fetch (bounded batch) and jet/next (poll-one) resolve to pure-data messages {:subject :data :js} (+ :headers when present), decoded with the context codec — no native object survives the per-leg js-msg->raw lift. JetStream metadata under :js (:stream :consumer :stream-seq :delivery-seq :delivered :pending :redelivered :timestamp :domain :ack-subject); :redelivered derived (delivered > 1); ack-subject moved under :js so a mistaken (reply conn js-msg) can't publish to it (ADR 0019). New -js-next/-js-fetch protocol methods, extended per-leg. Review fixes folded in: :timestamp normalized to one canonical UTC-millis ISO-8601 form on both legs (JVM appendInstant(3) / CLJS Date#toISOString) so the same instant is byte-identical; new nats-cljc.jetstream.pull deep module pre-flight-validates :expires-ms (sub-1000ms or non-integer -> portable :invalid-expires before any native call) and homes the :batch default (100) + 1000ms floor constants both legs reference; error-path closes the ConsumerMessages/FetchConsumer so a lift raising mid-batch releases the pull subscription; blocking pulls run on a per-connection cached daemon IO pool instead of ForkJoinPool.commonPool (shut down on close/drain) so long-poll parking can't starve management ops; core/trim-headers extracted, shared by decode-msg and decode-js-msg. Both legs green (JVM 345 assertions, Node 267), lint clean. Committed d005fd1.
