---
id: nts-01ksxx84gzkf
title: 'Code-quality follow-ups: test-idiom consolidation + variant dispatch (auth/codec)'
status: open
type: chore
priority: 3
mode: afk
created: '2026-05-31T02:19:16.380631698Z'
updated: '2026-06-02T03:10:28.150476112Z'
tags:
- tech-debt
- code-quality
acceptance:
- title: Test suite consolidated suite-wide onto a shared connect/teardown envelope + case table, no per-test clj/cljs copy-paste fork; JVM + Node legs green before and after
  done: false
- title: Auth (and, where it shares the shape, codec) dispatch reshaped from presence-flag cond-> to explicit tagged-variant selection; :seed no longer overloaded across nkey/jwt
  done: false
links:
- nts-01kstxa6v2mm
- nts-01kstx9pbqe5
---

## Description

Two structural follow-ups surfaced by a thermo-nuclear code-quality review of the closed implementation slices (tracer bullet, lifecycle/status, basic + advanced auth). Both are low-urgency: the code is green and correct. Captured here so they don't get buried inside a feature ticket's scope. Each is best landed alongside the linked slice rather than as a standalone refactor of green, closed-ticket code.

## Finding 1 — Consolidate the test idiom (relates to delivery-semantics tests, ADR 0008)

All 14 deftests in test/nats_cljc/core_test.cljc use a per-test #?(:clj <blocking> :cljs (async done ...)) fork. The 5 happy-path auth tests (core_test.cljc:135-218) are near-identical copy-paste differing only in :servers URL, :auth map, and message string.

Move the inputs to a case table and extract a portable connect/teardown/settle envelope so new tests don't multiply the fork.

CAVEAT: do this suite-wide, not just the auth block — a narrow conversion creates two idioms in one file (worse than the current uniform copy-paste). Cleanest landing is alongside the ADR 0008 blocking-convenience layer, which already reworks the test async idiom. The input table is a clear win; the cross-platform driver unification (CLJS must fold cases into one sequential promise chain under a single async done, collapsing five named failures into one) is a judgment call — keep it boring.

## Finding 2 — Explicit variant dispatch for auth (and codec) (relates to codecs slice)

with-auth dispatches on incidental key-presence and overloads :seed for two methods: nkey vs jwt is forked by an inner :jwt check inside the seed branch (impl/jvm.clj:82-88, impl/js.cljs:77-83). The :auth option is really a tagged variant — exactly one of {:token} / {:user :pass} / {:nkey :seed} / {:jwt :seed} / {:creds}. Dispatching on presence flags hides that model and is mildly brittle (a stray :seed alongside another shape silently changes behavior).

The codec registry (codec.cljc, today a case over :edn, growing to transit/json + custom protocol) is the same shape of problem. If you reshape one, reshape both toward an explicit tagged-variant dispatch rather than presence-flag cond->.

Low urgency — five cases each. Worth a consistent model as both grow, not a speculative change now.

## Notes

**2026-06-02T03:10:28.150476112Z**

Finding 2's codec-dispatch half is satisfied by nts-01kstx9pbqe5 (codecs slice): nats-cljc.codec now dispatches through an ICodec protocol + defonce registry (resolve-codec: instance pass-through, keyword lookup), which superseded the original `case` over codec keywords. Still open under this ticket: the auth-variant dispatch reshape, and Finding 1 (test-idiom consolidation).
