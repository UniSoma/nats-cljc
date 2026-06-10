---
id: nts-01ktshvh6k6n
title: Expose JetStream direct get (get-message, :no-message-found) on the jetstream API
status: open
type: feature
priority: 2
mode: afk
created: '2026-06-10T19:58:53.138945492Z'
updated: '2026-06-10T21:41:10.309449300Z'
tags:
- jetstream
links:
- nts-01ktsner23xc
- nts-01ktsqptj7fd
---

## Description

Phase 3 KV wraps the native KV clients (jnats KeyValue / @nats-io/kv), so the jetstream-level direct-get API the README's Phase 3 line promised ('on a direct-get foundation (getMessage, :no-message-found)') is no longer needed by KV and was dropped from Phase 3 scope. It remains a worthwhile standalone jetstream addition: a one-shot promise-returning get-message on the JetStream context (by stream sequence / last-by-subject), rejecting with a new normalized :no-message-found error :type when nothing matches. Adding a vocabulary member is a minor bump per ADR 0009. Reword the README Phase 3 roadmap line when this or the KV release lands.
