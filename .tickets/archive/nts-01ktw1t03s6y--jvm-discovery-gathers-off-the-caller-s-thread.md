---
id: nts-01ktw1t03s6y
title: JVM Discovery gathers off the caller's thread
status: closed
type: bug
priority: 1
mode: afk
created: '2026-06-11T19:16:08.946804652Z'
updated: '2026-06-11T19:44:48.458215922Z'
closed: '2026-06-11T19:44:48.458215922Z'
tags:
- services
- review
acceptance:
- title: ping/info/stats on the JVM offload the blocking gather to the connection's io-executor (ADR 0002 idiom)
  done: true
- title: -create-service follows the same offload shape
  done: true
- title: A JVM test proves the calling thread is not blocked for the fan-out window (watched red against the inline implementation first)
  done: true
- title: Existing discovery tests stay green on JVM + Node
  done: true
links:
- nts-01ktvn87why4
---

## Description

The standards review of Phase 4 found the JVM leg of Discovery (ping/info/stats in nats-cljc.service.impl.jvm) wraps the blocking jnats Discovery fan-out in a then on an already-completed future, which executes inline — so the caller's thread blocks for the whole fan-out window (up to :timeout-ms, default 5000ms) despite the promise-shaped return. This violates ADR 0001 (no blocking call leaks into the portable surface) and ADR 0002. The repo's established idiom offloads blocking round-trips via supplyAsync on the connection's io-executor (see the KV and JetStream JVM impls, which cite ADR 0002). -create-service shares the inline shape but does no gathering round-trip — fix it for consistency while there.

End-to-end: a consumer calling (service/ping conn) on the JVM gets their thread back immediately; the gather runs on the io-executor and the promise resolves with the same normalized vector as today.

## Notes

**2026-06-11T19:44:48.458215922Z**

JVM Discovery (ping/info/stats) and -create-service now offload their blocking jnats work via supplyAsync on the connection's io-executor (ADR 0002 idiom, mirroring the JetStream/KV off-thread helper, incl. RejectedExecutionException -> :connection-closed on a close race). Watched the new JVM test go red against the inline impl (caller blocked 2002ms of a 2000ms fan-out window) then green after the offload. Existing discovery tests needed their deref budgets bumped 5000->10000ms: the gather window no longer elapses inside the call, so the deref now races the full default 5000ms fan-out. Full suites green on JVM (233 tests) and Node (203 tests; one unrelated KV purge-deletes flake passed on re-run).
