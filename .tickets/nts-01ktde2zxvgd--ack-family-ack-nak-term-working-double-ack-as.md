---
id: nts-01ktde2zxvgd
title: 'Ack family: ack/nak/term/working + double-ack as sugar over publish'
status: open
type: feature
priority: 1
mode: afk
created: '2026-06-06T03:02:10.103513522Z'
updated: '2026-06-06T03:02:10.103513522Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- acks
acceptance:
- title: ack/nak/term/working return nil synchronously and are idempotent (double-ack of the same message never throws) on both legs
  done: false
- title: nak triggers redelivery (nak with :delay-ms delays it); term and ack both stop redelivery; working postpones the ack-wait timer
  done: false
- title: double-ack returns a Promise<bool> confirmed by the server
  done: false
- title: Ack-payload deep-module unit test (no server) produces correct bytes for +ACK / -NAK / -NAK{delay} / +WPI / +TERM; (reply conn js-msg ...) raises :no-reply-subject
  done: false
- title: Portable integration test passes on JVM + Node
  done: false
deps:
- nts-01ktde2zr4f0
---

## Description

Acknowledge delivered JetStream messages as SUGAR OVER PUBLISH to the captured ack-subject, not via native .ack() methods (ADR 0019). ack (processed -> stop redelivery), nak (redeliver, optional :delay-ms), term (give up -> never redeliver), working (still processing -> postpone the ack-wait timer) are synchronous, return nil, and are idempotent (a redundant ack is a harmless publish the server ignores; working is exempt from terminality). double-ack returns a Promise<bool> and is sugar over request to the ack subject (named double-ack, not ack-sync, because ours is async). Build the ack-payload-construction deep module (msg+opts -> wire bytes for +ACK / -NAK / -NAK{delay} / +WPI / +TERM) with a no-server unit test for each verb. Because the ack address lives under :js and never as a top-level :reply, a mistaken core (reply conn js-msg ...) raises :no-reply-subject. Covers user stories 27-33.
