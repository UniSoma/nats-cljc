---
id: nts-01ksxx84gzkf
title: 'Code-quality follow-ups: test-idiom consolidation + variant dispatch (auth/codec)'
status: closed
type: chore
priority: 3
mode: afk
created: '2026-05-31T02:19:16.380631698Z'
updated: '2026-06-03T21:36:04.561406110Z'
closed: '2026-06-03T21:36:04.561406110Z'
tags:
- tech-debt
- code-quality
acceptance:
- title: Auth dispatch reshaped to explicit tagged-variant selection (token / user-pass / nkey / jwt / creds); :seed no longer overloaded across nkey vs jwt; with-auth reshaped in jvm.clj + js.cljs; auth tests green on JVM + Node
  done: true
- title: server-error-type consolidated into one shared .cljc, both legs call it, and the cross-leg classifier test exercises the single fn; JVM + Node green
  done: true
- title: consume! (js.cljs) flattened — Promise.resolve seed + extra .then(step) hop removed — preserving the sync-throw/reject funnel and per-publisher ordering; Node delivery + backpressure tests green
  done: true
- title: 'Test idiom: shared connect/settle/teardown envelope + auth case table landed suite-wide; per-platform fork and five named auth deftests preserved (no CLJS driver-unification); JVM + Node green before and after'
  done: true
links:
- nts-01kstxa6v2mm
- nts-01kstx9pbqe5
---

## Description

Code-quality follow-ups surfaced by review of since-closed implementation slices. Every item is a low-urgency cleanup of green, correct code — none block anything. **This Description is the authoritative, current spec; it supersedes the dated Notes below wherever they conflict.**

### Status (patched 2026-06-03)

Already resolved — do NOT redo:

- **Codec variant dispatch** — superseded by the codecs slice (nts-01kstx9pbqe5): `codec.cljc` now dispatches through an `ICodec` protocol + registry (`resolve-codec`), not a `case` over keywords.
- **Registry leak on unsubscribe** — `impl/jvm.clj` `-unsubscribe` now opens with `(swap! registry dissoc dispatcher)` (jvm.clj:97), the shared-dissoc the original Note 3 demanded; the unsubscribe slices that shipped after it route through it.

Premise change: the original plan was to graft each item onto its in-flight feature slice. Those slices (delivery-semantics tests, codecs, auth, unsubscribe, error-model, blocking layer) have all closed, so these are now **standalone** cleanups. Each task below is scoped to stay mechanical and AFK-safe, and lands as its own revertable commit.

### Remaining work (each independently verifiable)

**1. Auth — explicit tagged-variant dispatch.**
`with-auth` dispatches on incidental key-presence and overloads `:seed` across nkey vs jwt (an inner `(if jwt ...)` inside the seed branch). `:auth` is really exactly one of `{:token}` / `{:user :pass}` / `{:nkey :seed}` / `{:jwt :seed}` / `{:creds}`; presence-flag `cond->` hides that and is brittle (a stray `:seed` beside another shape silently changes behavior).
- Files: `src/nats_cljc/impl/jvm.clj:315-325`, `src/nats_cljc/impl/js.cljs:285-295`.
- Select on an explicit variant (e.g. derive the tag, then dispatch) rather than presence flags; `:seed` must not be shared across two methods. Per-variant builder/opts output is unchanged.
- Verify: the auth tests (`core_test.cljc` `auth-with-*` + `auth-with-mismatched-nkey-rejects`) green on JVM + Node.

**2. Consolidate `server-error-type`.**
The classifier logic is duplicated across legs (`impl/jvm.clj:256`, `impl/js.cljs:217`) — bodies are identical, docstrings differ. The cross-leg unit test checks each leg's own copy, so a divergence would slip through.
- Move the one classifier into a shared `.cljc` — a new `nats-cljc.error` ns matching the `codec/->codec-error` precedent, or `protocol.cljc`; both legs call it.
- Leave `op-state-error` and the route/route-error! decision per-leg — their detection is genuinely platform-coupled and only a one-line `ex-info` is shareable (extraction would cost more than it saves).
- Verify: the classifier unit test exercises the single shared fn; JVM + Node green.

**3. Flatten `consume!` (js.cljs).**
The per-message `(js/Promise.resolve)` seed plus the extra `(.then (fn [_] (step)))` add two needless microtask hops and a throwaway promise on the hot path.
- File: `src/nats_cljc/impl/js.cljs:105-114`.
- Flatten while PRESERVING: the single funnel that routes a sync decode throw, a sync handler throw, and a rejecting handler promise alike to the sub's `on-error` (else connection `on-status` `:error`) and then continues; and per-publisher ordering (await the handler before pulling the next message — the backpressure invariant from ADR 0007).
- Verify: Node delivery + backpressure tests green — `single-subscription-delivers-in-order`, `pending-promise-handler-applies-backpressure`, `subscriptions-are-independent`.

**4. Test-idiom consolidation.**
The 53 deftests in `test/nats_cljc/core_test.cljc` each repeat a per-test `#?(:clj <blocking> :cljs (async done ...))` fork; the 5 auth happy-path tests (`core_test.cljc:230-298`) are near-identical copy-paste differing only in the `:auth` map and a message string.
- Extract a shared connect / settle / teardown envelope helper and use it **suite-wide** (one idiom — a narrow auth-only conversion is explicitly worse, as it leaves two idioms in one file), plus a case table for the 5 auth inputs.
- BOUNDED — the AFK guardrail, keep it boring: KEEP the per-platform `#?(:clj/:cljs)` fork and KEEP five separately-named auth deftests. Do NOT fold the CLJS cases into one sequential promise chain under a single `async done` (that collapses five named failures into one — the judgment call the original flagged; out of AFK scope). If the only way to share the envelope cleanly forces that fold, stop and leave a note rather than guess.
- Verify: JVM + Node legs green before and after (the suite must stay byte-for-byte green — this is a pure refactor).

### Global verification

`clj-kondo --lint src test` clean; full suite green on JVM + Node (docs/agents/running-tests.md). One small commit per item so any can be reverted independently.

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

**2026-06-03T01:11:26.722257585Z**

Code-review follow-ups from nts-01kstxatbw6k (error-model). Deferred cleanups, no correctness impact:

(1) Consolidate server-error-type: it is byte-identical in impl/jvm.clj and impl/js.cljs (the cross-leg unit test today checks each leg's own copy, so drift would slip through). Move the one classifier into a shared .cljc (protocol.cljc, or a new nats-cljc.error ns matching the codec/->codec-error precedent) and have both legs call it. Leave op-state-error and the route/route-error! decision per-leg — their detection is genuinely platform-coupled and only a one-line ex-info is shareable (extraction costs more than it saves).

(2) Simplify consume! in impl/js.cljs: the per-message (js/Promise.resolve) seed + extra (.then (fn [_] (step))) add two needless microtask hops and a throwaway promise on the hot path. Flatten while PRESERVING the validated sync-throw routing and per-publisher ordering; needs its own Node test run.

(3) Registry-leak breadcrumb (impl/jvm.clj): the dispatcher->sink registry is dissoc'd only in JvmSubscription -drain. When the deferred unsubscribe op lands, it MUST route through the same dissoc (ideally a single subscription-teardown hook all exits share), else an unsubscribed-not-drained sub leaks its dispatcher + on-error closure until the connection closes.

**2026-06-03T20:58:24.460687843Z**

Patched 2026-06-03 for AFK readiness. Verified each item against current code: codec variant dispatch DONE (codecs slice ICodec protocol+registry); registry-leak-on-unsubscribe DONE (jvm.clj:97 -unsubscribe now dissocs the registry). Still open: auth variant dispatch (with-auth presence-flag cond-> + :seed overload, jvm.clj:315-325 / js.cljs:285-295), server-error-type consolidation (duplicated jvm.clj:256 / js.cljs:217), consume! microtask flatten (js.cljs:105-114), and test-idiom consolidation (now 53 deftests, not the 14 at capture). Description rewritten as the authoritative spec with current file:line + per-item verify; 2 vague ACs replaced by 4 concrete verifiable ones. The "land alongside the in-flight slice" premise has expired (all linked slices closed) — these are now standalone. Test-idiom AC bounded to the boring half (shared envelope + auth case table, keep per-platform fork + named deftests, NO CLJS driver-unification).

**2026-06-03T21:36:04.561406110Z**

All four code-quality follow-ups landed, one revertable commit each, JVM 83/183 + Node 62/118 green throughout. (1) with-auth reshaped to explicit tagged-variant dispatch (auth-variant + case) in jvm.clj + js.cljs — :seed no longer overloaded across nkey/jwt. (2) server-error-type consolidated into a shared nats-cljc.error .cljc (codec/->codec-error precedent); both legs call it and the cross-leg classifier test exercises the single fn. (3) consume! flattened — the per-message Promise.resolve seed + extra .then(step) replaced by a js/Promise. executor that captures sync throws into the one route funnel, preserving per-publisher backpressure. (4) test idiom consolidated: shared with-conn connect/settle/teardown envelope applied suite-wide (42 happy-path tests) + auth-cases table behind five still-named auth deftests; per-platform fork kept, no CLJS driver-unification; connect-rejection / no-connect-unit / gated-teardown tests left as genuine outliers.
