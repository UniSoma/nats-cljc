---
id: nts-01kt87wgq6q0
title: Guard a throwing async sink on the JVM
status: closed
type: bug
priority: 2
mode: afk
created: '2026-06-04T02:37:34.310527491Z'
updated: '2026-06-05T01:52:38.673646904Z'
closed: '2026-06-05T01:52:38.673646904Z'
tags:
- review
- error
acceptance:
- title: A subscription whose `:on-error` throws still delivers the next message on the JVM, matching Node and browser
  done: true
- title: The throw does not escape into the jnats dispatcher
  done: true
- title: clj-kondo clean; suite green on JVM and Node
  done: true
---

## Description

A subscription whose `:on-error`/`:on-status` sink itself throws diverges: JS `consume!` wraps the sink call in try/catch and continues delivery (the sub survives, ADR 0007), but JVM `route-error!` calls the sink with no guard, so a throw escapes the onMessage catch into jnats' dispatcher run-loop — version-dependent behavior instead of the wrapper's sub-survives contract.

Wrap the JVM sink invocations in try/catch so a throwing sink is swallowed and the next message is still delivered, matching JS and ADR 0007.

## Notes

**2026-06-05T01:52:38.673646904Z**

Guarded the JVM async sink: wrapped route-error!'s on-error/on-status invocations in (try ... (catch Exception _ nil)), the JVM mirror of JS consume!'s route funnel, so a throwing sink can't escape onMessage into jnats' dispatcher run-loop (ADR 0007). Catches Exception not Throwable, so a JVM Error still propagates (matches the onMessage catch). Found no clean e2e trigger on this jnats version (dispatcher's default exceptionOccurred swallows the escape silently), so drove the fix with a seam test on route-error! (RED 2 errors -> GREEN), plus a portable integration test for Node parity. clj-kondo clean; JVM 99/215 and Node 75/141 green.
