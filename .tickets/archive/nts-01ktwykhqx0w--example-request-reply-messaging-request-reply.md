---
id: nts-01ktwykhqx0w
title: 'Example: Request-Reply (messaging.request-reply)'
status: closed
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:26.333263935Z'
updated: '2026-06-22T21:49:35.979396882Z'
closed: '2026-06-22T21:49:35.979396882Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/messaging/request-reply/go to nats-cljc.

File: examples/examples/messaging/request_reply.cljc (stub scaffolded; implement -main).
Surface: core request / reply / :queue groups
Run: `bb example:jvm messaging.request-reply` / `bb example:node messaging.request-reply` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).

## Notes

**2026-06-22T21:49:35.979396882Z**

Ported messaging.request-reply to nats-cljc; runs to completion on both legs (JVM + Node), identical output, clj-kondo clean. Section 1 is the upstream-faithful service: subscribe greet.*, three awaited request/reply round-trips (joe/sue/bob print in order via sequential p/run!/p/do), then drain the sub and show the request to greet.pam reject with :no-responders. Section 2 (deliberate extension beyond upstream, per ticket scope) demonstrates :queue groups — two responders in group 'greeters' share six requests; tally proves exactly-once-per-group (sums to 6, split not guaranteed even). Commented in the upstream Go pattern. Fixed during review: removed an unexercised 'I don't understand' handler branch (not in upstream), and the await race (intermediate p/let body forms aren't awaited — sequenced via p/run!+p/do instead). Verified p/run! is sequential-await on both legs.
