---
id: nts-01ktw1vef0dp
title: Scrub tracker-artifact references and stale docs after Phase 4
status: closed
type: task
priority: 3
mode: afk
created: '2026-06-11T19:16:56.408461582Z'
updated: '2026-06-11T23:24:07.233629089Z'
closed: '2026-06-11T23:24:07.233629089Z'
tags:
- services
- review
- docs
acceptance:
- title: No code comment, test comment, or ADR sentence cites slices, the epic, or other tracker artifacts
  done: true
- title: ADR 0015 and CONTEXT.md acknowledge service/create as a promise-rejecting validation point
  done: true
- title: ADR 0005's public-surface enumeration includes nats-cljc.service
  done: true
- title: clj-kondo clean; no behaviour change (docs and comments only)
  done: true
links:
- nts-01ktvn87why4
---

## Description

Review cleanup, two halves. (1) Committed artifacts cite tracker work-breakdown: a 'discovery slice's contract' comment in the JS service impl, 'pre-slice respond' and 'deferred from the errors slice' comments in the service tests, and ADR 0007's new section ends by citing 'the epic's pre-decided fork'. Rewrite each to state the actual constraint or decision in its own terms — committed code and ADRs must stand without the tracker. (2) Phase 4 made two docs stale: ADR 0015 section 1 and CONTEXT.md still call connect 'the one promise-returning operation that validates', but service/create now also rejects with validation errors; and ADR 0005's 'public surface is exactly...' enumeration omits nats-cljc.service.

## Notes

**2026-06-11T23:17:53.266574066Z**

Implemented. Two of the four cited comments ('discovery slice's contract' in the JS service impl, 'deferred from the errors slice' in the service tests) were already gone — removed by the sibling tickets that closed earlier. Remaining scrubs: service_test.cljc 'pre-slice respond' comment rewritten to state the actual red-before-green lever (a respond ignoring the bound codec); ADR 0007's closing sentence now states the fork (drive the iterator vs narrow the Handler contract) and its resolution in its own terms, no epic citation. Stale docs: ADR 0015 §1 and CONTEXT.md now say 'a promise-returning operation that validates (connect, service/create) rejects its promise'; ADR 0005's public-surface enumeration gains nats-cljc.service and the internals enumeration gains nats-cljc.service.impl.* (config, jvm/js legs). Out of scope, noted as pre-existing debt: older slice-citing comments in core/jetstream/kv impl+tests, AC-number comments in blocking/core_test and jetstream_test, and nts- ids in ADRs 0011/0021 (plus ADR 0015's 'tickets that prompted this ADR') predate Phase 4. clj-kondo clean; JVM 236/806 green; Node 206/701 green. Not committed.

**2026-06-11T23:24:07.233629089Z**

Scrubbed the remaining tracker-artifact references from Phase-4 artifacts and refreshed the stale docs. Two of the four cited comments were already removed by sibling tickets; the service test's respond comment and ADR 0007's closing sentence now state their constraints in their own terms. ADR 0015 and CONTEXT.md acknowledge service/create as a promise-rejecting validation point alongside connect; ADR 0005's enumerations gain nats-cljc.service and nats-cljc.service.impl.*. Pre-existing tracker references from earlier phases were out of scope and noted as debt. Docs and comments only; no behaviour change.
