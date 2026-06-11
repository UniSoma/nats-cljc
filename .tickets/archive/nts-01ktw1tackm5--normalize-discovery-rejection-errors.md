---
id: nts-01ktw1tackm5
title: Normalize discovery rejection errors
status: closed
type: task
priority: 2
mode: afk
created: '2026-06-11T19:16:19.462135645Z'
updated: '2026-06-11T21:43:07.448995784Z'
closed: '2026-06-11T21:43:07.448995784Z'
tags:
- services
- review
acceptance:
- title: Discovery rejections on both legs are ex-info with a canonical ADR-0006 :type (no raw host errors leak)
  done: true
- title: A test on each leg observes a normalized :type from a forced discovery failure, watched red against the raw-error behaviour first
  done: true
- title: :no-responders handling is unchanged
  done: true
deps:
- nts-01ktw1t03s6y
links:
- nts-01ktvn87why4
---

## Description

Discovery (ping/info/stats) only normalizes :no-responders today; any other native failure mid-fan-out (e.g. connection closed) rejects the promise with a raw host error — a bare jnats exception on the JVM, a rethrown native error on JS — instead of an ex-info carrying a canonical :type per ADR 0006. Map native discovery failures on both legs to the normalized error model so a consumer branches on :type identically across legs.

Depends on the JVM Discovery offload slice — same functions, avoid conflicting edits.

## Notes

**2026-06-11T21:28:47.423291867Z**

Implemented: discovery rejections normalized on both legs. JVM: ISE from the Discovery fan-out mapped by connection status (CLOSED -> :connection-closed, CONNECTED drain block -> :drained) via a shared discover helper wrapping off-thread. JS: gather's catch maps DrainingConnectionError -> :drained and ClosedConnectionError -> :connection-closed (isDraining selects :drained closed-after-drain); no-responders -> [] unchanged. New portable test discovery-on-a-closed-connection-rejects-connection-closed watched red on both legs (raw host errors carried nil ex-data), then green. Lint clean; full suites green: JVM 235/805, Node 205/701.

**2026-06-11T21:43:07.448995784Z**

Discovery rejections now reject with canonical ADR-0006 ex-info on both legs. JVM maps the jnats IllegalStateException by connection status (CLOSED -> :connection-closed, drain block -> :drained, other statuses rethrow raw) via a shared discover helper; JS maps ClosedConnectionError/DrainingConnectionError in gather's catch, with isDraining selecting :drained for closed-after-drain. :no-responders -> [] normalization unchanged. One portable test, watched red first, asserts ping/info/stats all reject :connection-closed on a closed connection on both legs; lint clean, JVM and Node suites green.
