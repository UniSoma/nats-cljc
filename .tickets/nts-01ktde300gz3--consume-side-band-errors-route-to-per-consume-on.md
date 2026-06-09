---
id: nts-01ktde300gz3
title: Consume side-band errors route to per-consume :on-error
status: open
type: feature
priority: 2
mode: afk
created: '2026-06-06T03:02:10.188611974Z'
updated: '2026-06-09T20:25:04.261400359Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- errors
acceptance:
- title: Deleting a consumer mid-consume delivers a normalized :consumer-deleted to that consume's :on-error and completes the handle, on both legs
  done: false
- title: :heartbeats-missed / :consumer-deleted / :exceeded-limits normalize identically on both legs and reach the per-consume :on-error only (dropped if unset, never :on-status)
  done: false
- title: Side-band classifier unit test (no server) covers the err_code->:type mapping
  done: false
- title: Portable integration test passes on JVM + Node
  done: false
deps:
- nts-01ktde2zv0fm
---

## Description

Route consume-time runtime conditions to a per-consume :on-error, mirroring how core's per-subscription :slow-consumer is routed (ADR 0006/0020). On CLJS these conditions arrive on a separate status() async-iterable; on the JVM via exceptions/status on the poll; the adapter normalizes both into operational :type s :heartbeats-missed, :consumer-deleted, :exceeded-limits (a backing-stream loss reuses :stream-not-found). They are inherently per-consume, so they follow the slow-consumer routing row EXACTLY: delivered to the consume's :on-error ONLY, dropped if unset, never to the connection :on-status, never both. Terminal conditions (the consumer or its backing stream is gone) additionally END the consume; the returned handle completes. Completes the shared err_code->:type table and adds the side-band classifier unit test. Exercised via fault injection (e.g. delete the consumer mid-consume). Covers user story 24.

## Notes

**2026-06-09T20:25:04.261400359Z**

Besides server-issued side-band conditions, the consume delivery loops on both legs currently catch-and-swallow the HANDLER's own throws and decode failures with zero signal (JVM onMessage catch, CLJS .next .catch). A handler that throws on every message spins the pull loop forever, indistinguishable from an empty stream. The per-consume :on-error sink this ticket builds is the natural home for routing handler/decode throws too — consider adding an acceptance criterion for it here rather than a separate ticket. (Source: ephemeral review.md, do not reference it or this note in committed code/docs.)
