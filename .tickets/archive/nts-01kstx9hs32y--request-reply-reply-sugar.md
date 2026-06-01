---
id: nts-01kstx9hs32y
title: request/reply + reply sugar
status: closed
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:22:19.427377383Z'
updated: '2026-06-01T22:26:05.494261325Z'
closed: '2026-06-01T22:26:05.494261325Z'
acceptance:
- title: '`(nats/request conn subject data opts)` resolves to a decoded `Message` on all three platforms'
  done: true
- title: '`(nats/reply conn msg data)` answers the request `:reply` subject and returns `nil`'
  done: true
- title: A request to a subject with no subscribers rejects with `ex-info` `:type :no-responders`
  done: true
- title: A request whose responders never answer within `:timeout-ms` rejects with `:type :timeout`
  done: true
deps:
- nts-01kstx8ysgv5
---

## Description

Request/reply over the core round-trip. `request` returns `Promise<Message>`; `reply` is sugar for responding to a message `:reply` subject (returns `nil`). Distinguish the two failure modes by canonical `:type`: `:timeout` when responders exist but none answer within `:timeout-ms`, and `:no-responders` when nobody subscribes the subject.

ADRs: 0002 (native-promise one-shots), 0006 (normalized errors).

## Notes

**2026-06-01T22:26:05.494261325Z**

request/reply + reply sugar shipped and green on JVM + Node (browser CI-only). nats/request resolves to a decoded {:subject :data :reply} message and rejects with typed ex-info :no-responders vs :timeout (JVM via .useTimeoutException distinguishing CancellationException/TimeoutException; JS via nats-core's RequestError/TimeoutError/NoRespondersError). nats/reply is publish sugar over the request's :reply inbox, returning nil; subscribe now surfaces :reply on every delivery. Post-review hardening: reply throws :no-reply-subject instead of publishing to a nil subject, and the raw msg->raw (per impl) + decode-msg (core) mappings were deduped across the subscribe/request paths.
