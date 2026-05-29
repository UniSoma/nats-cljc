---
id: nts-01kstxb0758h
title: Blocking convenience layer (Phase 1.5, JVM-only)
status: open
type: feature
priority: 2
mode: afk
created: '2026-05-29T22:23:06.980203620Z'
updated: '2026-05-29T22:23:06.980203620Z'
tags:
- needs-triage
acceptance:
- title: '`nats-cljc.blocking.core` exists and is JVM-only (not compiled or loaded on ClojureScript)'
  done: false
- title: '`connect` and `close` block and return / complete synchronously'
  done: false
- title: '`subscribe` returns a pull handle; `(take-message sub timeout-ms)` blocks for at most the timeout and returns the next `{:subject :data}` or `nil`, draining in order from a bounded queue'
  done: false
- title: A failed one-shot throws an `ex-info` carrying the same canonical `:type` as the async core rejected promise
  done: false
- title: A JVM-only test suite exercises the connect -> subscribe -> take-message loop -> close path
  done: false
deps:
- nts-01kstxa377qb
---

## Description

A JVM-only parallel subtree `nats-cljc.blocking.core` offering synchronous ergonomics over the same core, with identical verb names. One-shots block and return the value directly (a rejected promise is unwrapped to the same canonical `ex-info` `:type` the async core produces). `subscribe` returns a pull handle (no callback handler); `take-message` blocks up to a timeout, pulling from a bounded queue. `close` blocks until closed. Not loaded on ClojureScript.

Ships after the async core is complete (slices 2-9). ADR 0008 (blocking layer is a JVM-only parallel subtree that ships after the core).
