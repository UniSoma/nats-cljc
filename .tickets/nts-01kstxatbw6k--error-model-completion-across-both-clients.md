---
id: nts-01kstxatbw6k
title: Error-model completion across both clients
status: open
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:23:00.986432509Z'
updated: '2026-05-29T22:23:00.986432509Z'
tags:
- needs-triage
acceptance:
- title: Each canonical error `:type` is reproduced and asserted with identical shape on both `io.nats:jnats` and `@nats-io/nats-core`
  done: false
- title: A handler that throws is caught and routed to `:on-status :error` (or the subscription `:on-error`) without killing the subscription
  done: false
- title: A decode failure on a subscription is routed to the per-sub `:on-error` / status `:error` sink as `:codec-error`
  done: false
- title: '`:slow-consumer` is surfaced as a status event and `:max-pending` is honored'
  done: false
deps:
- nts-01kstx9hs32y
- nts-01kstx9pbqe5
- nts-01kstxa377qb
---

## Description

Complete the canonical error normalization across both native clients so portable code always reads `(:type (ex-data e))` rather than branching on host exception types. Normalize the remaining members of the canonical set not covered by earlier slices: `:connect-failed`, `:connection-closed`, `:permissions-violation`, `:max-payload-exceeded`, `:protocol-error`, `:drained` (the request `:timeout`/`:no-responders` and the `:codec-error` arrive with their own slices).

Route async failures to their sinks: a throwing handler and a decode failure reach the connection `:on-status :error` sink and/or a per-subscription `:on-error`; surface `:slow-consumer` and honor `:max-pending`.

ADRs: 0006 (normalized errors — one-shots reject, async failures hit `:on-status :error` or per-sub `:on-error`), 0007 (delivery; slow consumer).
