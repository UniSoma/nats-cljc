---
id: nts-01ktde2zxvgd
title: 'Ack family: ack/nak/term/working + double-ack as sugar over publish'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-06T03:02:10.103513522Z'
updated: '2026-06-09T18:59:00.098237767Z'
closed: '2026-06-09T18:59:00.098237767Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- acks
acceptance:
- title: ack/nak/term/working return nil synchronously and are idempotent (double-ack of the same message never throws) on both legs
  done: true
- title: nak triggers redelivery (nak with :delay-ms delays it); term and ack both stop redelivery; working postpones the ack-wait timer
  done: true
- title: double-ack returns a Promise<bool> confirmed by the server
  done: true
- title: Ack-payload deep-module unit test (no server) produces correct bytes for +ACK / -NAK / -NAK{delay} / +WPI / +TERM; (reply conn js-msg ...) raises :no-reply-subject
  done: true
- title: Portable integration test passes on JVM + Node
  done: true
deps:
- nts-01ktde2zr4f0
---

## Description

Acknowledge delivered JetStream messages as SUGAR OVER PUBLISH to the captured ack-subject, not via native .ack() methods (ADR 0019). ack (processed -> stop redelivery), nak (redeliver, optional :delay-ms), term (give up -> never redeliver), working (still processing -> postpone the ack-wait timer) are synchronous, return nil, and are idempotent (a redundant ack is a harmless publish the server ignores; working is exempt from terminality). double-ack returns a Promise<bool> and is sugar over request to the ack subject (named double-ack, not ack-sync, because ours is async). Build the ack-payload-construction deep module (msg+opts -> wire bytes for +ACK / -NAK / -NAK{delay} / +WPI / +TERM) with a no-server unit test for each verb. Because the ack address lives under :js and never as a top-level :reply, a mistaken core (reply conn js-msg ...) raises :no-reply-subject. Covers user stories 27-33.

## Notes

**2026-06-09T18:59:00.098237767Z**

Ack family landed as sugar over publish (ADR 0019), one portable code path on both legs: (jet/ack conn msg), nak (+ optional :delay-ms, sent as JSON nanoseconds), term, working publish +ACK/-NAK/+TERM/+WPI to the message's :js :ack-subject — synchronous, nil, idempotent; double-ack is sugar over request, resolving Promise<bool> true on the server's empty confirmation reply (verified: the server confirms redundant acks too, so a second double-ack resolves true, never throws). Deep module nats-cljc.jetstream.acks owns payload construction + the guarded ack-subject accessor (:no-ack-subject on a non-JetStream message); a mistaken core (reply conn js-msg) raises :no-reply-subject since the lift drops :reply. Named acks, not ack: a CLJS sub-namespace and same-named var share one JS object path, so jetstream.ack would collide with the facade's ack verb (caught by the Node leg). Integration covers redelivery semantics: nak redelivers, delay postpones (test is unit-sensitive: ms-instead-of-ns goes red), ack/term stop redelivery, working postpones ack-wait. JVM 147/372 + Node 121/294 green, lint clean. Commit a2354ef.
