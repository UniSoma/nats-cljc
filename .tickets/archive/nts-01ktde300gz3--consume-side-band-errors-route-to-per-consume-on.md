---
id: nts-01ktde300gz3
title: Consume side-band errors route to per-consume :on-error
status: closed
type: feature
priority: 2
mode: afk
created: '2026-06-06T03:02:10.188611974Z'
updated: '2026-06-09T23:52:31.821809296Z'
closed: '2026-06-09T23:52:31.821809296Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- errors
acceptance:
- title: Deleting a consumer mid-consume delivers a normalized :consumer-deleted to that consume's :on-error and completes the handle, on both legs
  done: true
- title: :heartbeats-missed / :consumer-deleted / :exceeded-limits normalize identically on both legs and reach the per-consume :on-error only (dropped if unset, never :on-status)
  done: true
- title: Side-band classifier unit test (no server) covers the err_code->:type mapping
  done: true
- title: Portable integration test passes on JVM + Node
  done: true
deps:
- nts-01ktde2zv0fm
links:
- nts-01ktq7fj46t3
---

## Description

Route consume-time runtime conditions to a per-consume :on-error, mirroring how core's per-subscription :slow-consumer is routed (ADR 0006/0020). On CLJS these conditions arrive on a separate status() async-iterable; on the JVM via exceptions/status on the poll; the adapter normalizes both into operational :type s :heartbeats-missed, :consumer-deleted, :exceeded-limits (a backing-stream loss reuses :stream-not-found). They are inherently per-consume, so they follow the slow-consumer routing row EXACTLY: delivered to the consume's :on-error ONLY, dropped if unset, never to the connection :on-status, never both. Terminal conditions (the consumer or its backing stream is gone) additionally END the consume; the returned handle completes. Completes the shared err_code->:type table and adds the side-band classifier unit test. Exercised via fault injection (e.g. delete the consumer mid-consume). Covers user story 24.

## Notes

**2026-06-09T20:25:04.261400359Z**

Besides server-issued side-band conditions, the consume delivery loops on both legs currently catch-and-swallow the HANDLER's own throws and decode failures with zero signal (JVM onMessage catch, CLJS .next .catch). A handler that throws on every message spins the pull loop forever, indistinguishable from an empty stream. The per-consume :on-error sink this ticket builds is the natural home for routing handler/decode throws too — consider adding an acceptance criterion for it here rather than a separate ticket. (Source: ephemeral review.md, do not reference it or this note in committed code/docs.)

**2026-06-09T23:52:31.821809296Z**

Consume side-band conditions now route to a per-consume :on-error on both legs, exactly like core's :slow-consumer row: sink only, dropped if unset, never :on-status. A shared case-insensitive 409-status classifier in nats-cljc.jetstream.error normalizes :heartbeats-missed / :consumer-deleted / :exceeded-limits (backing-stream loss reuses :stream-not-found) identically on JVM (via new ErrorListener heartbeatAlarm/pullStatus* registry routing keyed by [stream consumer]) and CLJS (via the ConsumerMessages status() pump plus abort_on_missing_resource); terminal conditions end the consume and complete the handle even without a sink, and previously-swallowed handler/decode throws now reach the same sink with delivery continuing. Covered by a no-server classifier unit test and portable integration tests (consumer deleted mid-consume, drop-if-unset, handler throw); clj-kondo clean, JVM (171 tests/487 assertions) and Node (142 tests/399 assertions) legs both green.
