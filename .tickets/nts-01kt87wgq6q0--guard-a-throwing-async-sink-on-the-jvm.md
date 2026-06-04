---
id: nts-01kt87wgq6q0
title: Guard a throwing async sink on the JVM
status: open
type: bug
priority: 2
mode: afk
created: '2026-06-04T02:37:34.310527491Z'
updated: '2026-06-04T02:53:24.687300119Z'
tags:
- review
- error
acceptance:
- title: A subscription whose `:on-error` throws still delivers the next message on the JVM, matching Node and browser
  done: false
- title: The throw does not escape into the jnats dispatcher
  done: false
- title: clj-kondo clean; suite green on JVM and Node
  done: false
---

## Description

A subscription whose `:on-error`/`:on-status` sink itself throws diverges: JS `consume!` wraps the sink call in try/catch and continues delivery (the sub survives, ADR 0007), but JVM `route-error!` calls the sink with no guard, so a throw escapes the onMessage catch into jnats' dispatcher run-loop — version-dependent behavior instead of the wrapper's sub-survives contract.

Wrap the JVM sink invocations in try/catch so a throwing sink is swallowed and the next message is still delivered, matching JS and ADR 0007.
