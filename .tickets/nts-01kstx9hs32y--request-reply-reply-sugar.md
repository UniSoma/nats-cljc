---
id: nts-01kstx9hs32y
title: request/reply + reply sugar
status: open
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:22:19.427377383Z'
updated: '2026-05-30T19:53:03.947442419Z'
acceptance:
- title: '`(nats/request conn subject data opts)` resolves to a decoded `Message` on all three platforms'
  done: false
- title: '`(nats/reply conn msg data)` answers the request `:reply` subject and returns `nil`'
  done: false
- title: A request to a subject with no subscribers rejects with `ex-info` `:type :no-responders`
  done: false
- title: A request whose responders never answer within `:timeout-ms` rejects with `:type :timeout`
  done: false
deps:
- nts-01kstx8ysgv5
---

## Description

Request/reply over the core round-trip. `request` returns `Promise<Message>`; `reply` is sugar for responding to a message `:reply` subject (returns `nil`). Distinguish the two failure modes by canonical `:type`: `:timeout` when responders exist but none answer within `:timeout-ms`, and `:no-responders` when nobody subscribes the subject.

ADRs: 0002 (native-promise one-shots), 0006 (normalized errors).
