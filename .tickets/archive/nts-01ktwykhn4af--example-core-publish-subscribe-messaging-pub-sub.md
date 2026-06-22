---
id: nts-01ktwykhn4af
title: 'Example: Core Publish-Subscribe (messaging.pub-sub)'
status: closed
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:26.244407237Z'
updated: '2026-06-22T00:32:52.120546409Z'
closed: '2026-06-22T00:32:52.120546409Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/messaging/pub-sub/cli to nats-cljc.

File: examples/examples/messaging/pub_sub.cljc (stub scaffolded; implement -main).
Surface: core publish / subscribe / unsubscribe
Run: `bb example:jvm messaging.pub-sub` / `bb example:node messaging.pub-sub` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).

## Notes

**2026-06-21T21:50:22.241131276Z**

gap: A failed connect (wrong URL/port, server down, bad auth) is SILENT on the Node/cljs leg — green exit, zero output. Root cause: (1) examples.main dispatcher discards the promise -main returns (bootstrap is `cljs.core.apply(examples.main._main, process.argv.slice(2))`), so nothing observes the rejection; (2) promesa's cljs derived promises are its own PromiseImpl (promesa/impl/promise.js), NOT native js/Promise — a rejected PromiseImpl nobody awaits is just a dead object, invisible to Node's unhandledRejection detector, so no warning + exit 0. Verified Node v24 DOES exit 1 on an unobserved *native* chained rejection, so this is promesa-specific, not Node leniency. JVM leg masks it because the old code derefd (rethrows on reject).

doc: examples authors need to know cljs needs the ws:// endpoint (ADR 0001), and that connect failures vanish on Node without an explicit p/catch. The per-platform URL fork (#?(:clj "nats://..4222" :cljs "ws://..8080")) is mandatory, not cosmetic.

fix: hardened examples.main/-main to attach a terminal p/catch that logs + sets process.exitCode 1, so any example's connect/run failure is loud on the Node leg.

**2026-06-22T00:20:21.406777073Z**

Docstrings should be comprehensible, self-contained and enough for the user to discover and use the library. For instance, in `nats.core/connnect`: what are all the possible options? Example usage etc...

**2026-06-22T00:26:23.479464945Z**

Multiple libraries in the upstream examples support a pattern of asking for the next message and await for it. Take Python, for example: `msg = await sub.next_msg(timeout=0.1)`. I understand in this library we leaned towards a dispatcher pattern from Java. Is it worth to also consider an alternative API to grab messages one by one in the core namespace?

**2026-06-22T00:32:52.120546409Z**

Ported messaging.pub-sub to nats-cljc, runs to completion on both legs (JVM + Node), printing the upstream narrative (3 greet.* messages; both greet.bob publishes correctly absent — first pre-subscription, second post-unsubscribe max 3). Truly cljc.
