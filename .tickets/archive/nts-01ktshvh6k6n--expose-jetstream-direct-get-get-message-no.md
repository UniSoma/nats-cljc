---
id: nts-01ktshvh6k6n
title: Expose JetStream direct get (get-message, :no-message-found) on the jetstream API
status: closed
type: feature
priority: 2
mode: afk
created: '2026-06-10T19:58:53.138945492Z'
updated: '2026-06-10T22:45:25.771217653Z'
closed: '2026-06-10T22:45:25.771217653Z'
tags:
- jetstream
links:
- nts-01ktsner23xc
- nts-01ktsqptj7fd
---

## Description

Phase 3 KV wraps the native KV clients (jnats KeyValue / @nats-io/kv), so the jetstream-level direct-get API the README's Phase 3 line promised ('on a direct-get foundation (getMessage, :no-message-found)') is no longer needed by KV and was dropped from Phase 3 scope. It remains a worthwhile standalone jetstream addition: a one-shot promise-returning get-message on the JetStream context (by stream sequence / last-by-subject), rejecting with a new normalized :no-message-found error :type when nothing matches. Adding a vocabulary member is a minor bump per ADR 0009. Reword the README Phase 3 roadmap line when this or the KV release lands.

## Notes

**2026-06-10T22:45:25.771217653Z**

Added nats-cljc.jetstream/get-message: a one-shot promise-returning direct read of a stored message off a Stream, selected by exactly one of {:seq n} (stream sequence) or {:last-by-subject subj} (newest on a subject), resolving a pure-data {:subject :data :seq :timestamp} (plus :headers when present) — no :js consumer metadata, nothing to ack. A no-match rejects with the new normalized operational :type :no-message-found (err 10037, added to the shared err-code table; the nats.js leg re-raises the null it absorbs natively so both legs agree, carrying {:code :description}); a missing stream stays :stream-not-found. Pre-flight validation: :invalid-name for a malformed stream name, :unknown-config-key for an unrecognized query key, and the new validation :type :invalid-query for a query not selecting by exactly one well-formed selector. Covered by unit + live tests on JVM and Node (both suites green); CONTEXT.md vocabularies and CHANGELOG (minor bump per ADR 0009) updated. README Phase 3 roadmap reword left to the 0.4.0 release ticket as planned.
