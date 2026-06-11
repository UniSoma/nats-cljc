---
id: nts-01ktsner23xc
title: 'Phase 3: KV (nats-cljc.kv)'
status: closed
type: epic
priority: 1
mode: afk
created: '2026-06-10T21:01:48.478791146Z'
updated: '2026-06-11T02:54:14.826978924Z'
closed: '2026-06-11T02:54:14.826978924Z'
tags:
- kv
- phase-3
links:
- nts-01ktshvh6k6n
---

## Description

## Problem Statement

A Clojure/ClojureScript developer using nats-cljc has portable core pub/sub (Phase 1) and JetStream (Phase 2), but no Key/Value access. NATS KV is the natural choice for config, caches, coordination state, and last-value registries — yet today reaching it from this library means dropping to per-platform native interop (jnats `KeyValue` on the JVM, `@nats-io/kv` on Node/browser), forfeiting exactly what nats-cljc exists to provide: one `.cljc` API, decoded Clojure data, normalized errors, and identical consumer code on all three platforms.

## Solution

Phase 3 ships `nats-cljc.kv`: a portable facade wrapping each platform's native KV client. A consumer obtains a KV context from a connection (verified at entry, like the JetStream context), opens or creates a Bucket to get a Bucket handle, and works with Entries — plain maps whose `:value` is decoded through the Bucket's one Codec. The surface covers the five write verbs (`put`, `create`, `update`, `delete`, `purge`) with Revision-based compare-and-set, the read family (`get` — latest or revision-pinned, `history`, `keys`), Watch with a single `:deliver` mode and an `:initialized` Promise, and Bucket management (create/open/delete/names/list/status). Failures speak KV vocabulary (`:bucket-not-found`, `:wrong-revision`) per ADR 0023 — never the stream substrate.

## User Stories

1. As a portable-library consumer, I want to obtain a KV context from my Connection with `(kv conn)`, so that the same KV code compiles and runs unchanged on the JVM, Node, and the browser.
2. As a portable-library consumer, I want the KV context verified at entry (rejecting with `:jetstream-not-enabled` when the account lacks JetStream), so that misconfiguration surfaces once, early, instead of per-operation.
3. As a developer, I want `create-bucket` to take a closed kebab-case config map and resolve to a Bucket handle, so that I can provision and immediately use a Bucket in one step.
4. As a developer, I want `open-bucket` to verify the Bucket exists and reject with `:bucket-not-found`, so that a misspelled bucket name is one obvious early rejection rather than a per-op surprise.
5. As a developer, I want the Bucket handle to bind one Codec (the connection's default unless overridden at open), so that every value I read or write is ordinary Clojure data without per-call ceremony.
6. As a developer, I want `(get bucket key)` to resolve to an Entry map `{:bucket :key :value :revision :created :operation}`, so that I get the Revision alongside the value and can do compare-and-set without a second API.
7. As a developer, I want `get` on an absent key (never written, deleted, or purged) to resolve to `nil`, so that I can branch on absence with `if-let` instead of wrapping my most common read in try/catch.
8. As a developer, I want an Entry whose stored value is nil to be distinguishable from an absent key (`{:value nil …}` vs `nil`), so that storing nil is not ambiguous.
9. As a developer, I want `(get bucket key {:revision n})` to fetch the Entry at an exact past Revision — including delete/purge markers, with their `:operation` visible — so that I can do history archaeology cheaply.
10. As a developer, I want `(put bucket key value)` to resolve to the new Revision as a bare number, so that I can immediately use it as the expected Revision of a follow-up `update`.
11. As a developer, I want `(create bucket key value)` to succeed only when the key is absent, so that I can implement first-writer-wins initialization and locks.
12. As a developer, I want `(update bucket key value revision)` to succeed only when my expected Revision is still latest, so that concurrent writers cannot silently clobber each other.
13. As a developer, I want a lost compare-and-set race — stale `update` or `create` on an existing key — to reject with the single canonical Error `:type :wrong-revision` carrying the `:key`, so that I dispatch on KV vocabulary without knowing KV is stream-backed.
14. As a developer, I want `(delete bucket key)` to write a Tombstone (key reads as absent, history retained), so that deletion is observable to history readers and watchers.
15. As a developer, I want `(purge bucket key)` to erase the key's history leaving a single purge marker, so that I can reclaim space and remove stale values for good.
16. As a developer, I want `delete` and `purge` to accept an optional `:revision` guard, so that I only remove what I believe I am removing.
17. As a developer, I want `(history bucket key)` to resolve to a vector of Entries oldest→newest including Tombstones and purge markers, so that I can reconstruct how a key changed.
18. As a developer, I want `(keys bucket)` with an optional subject-style filter to resolve to a vector of key strings (deleted/purged keys excluded), so that I can enumerate a Bucket's live contents.
19. As a developer, I want `(watch bucket handler opts?)` following the house convention (handler positional, opts trailing) with each matching Entry pushed to my Handler, so that watching feels exactly like a core subscription.
20. As a developer, I want a single `:deliver` option — `:latest` (default), `:history`, or `:updates` — so that invalid flag combinations are unrepresentable.
21. As a cache builder, I want the watch handle to carry an `:initialized` Promise that resolves when the initial replay completes, so that I can build my cache and only then serve reads.
22. As a cache builder, I want `:ignore-deletes?` on watch, so that I can choose between cache-maintenance and event-log semantics.
23. As a developer, I want `(stop watch-handle)` to end a Watch idempotently, so that teardown is safe to repeat.
24. As a developer, I want a watch decode failure routed to the watch's `:on-error` if set, else the connection's `:on-status` as an `:error` event (the established sink semantics), so that async failures behave identically across the whole library.
25. As a developer, I want `:keys` filtering on watch (one or many subject-style patterns), so that I observe only the keys I care about.
26. As an operator, I want `bucket-names` (vector of strings), `list-buckets` (vector of status maps), and `bucket-status` (one normalized status map), so that I can inspect KV topology portably.
27. As an operator, I want `delete-bucket`, so that I can decommission a Bucket portably.
28. As an operator, I want bucket config expressed as a closed kebab map (`:bucket` required, plus `:description :history :ttl-ms :max-value-size :max-bucket-size :storage :replicas :compression?`), with `:unknown-config-key` / `:missing-required-key` validation, so that typos fail fast instead of silently defaulting.
29. As a developer, I want client-side key-syntax validation raising the validation `:type :invalid-key` (and `:invalid-name` for malformed Bucket names), so that programmer errors are caught before any wire call.
30. As a browser-app author who uses only core pub/sub, I want my bundle to ship zero KV bytes despite `@nats-io/kv` being an unconditional dependency, so that I never pay for what I don't require.
31. As a CLJS consumer, I want `@nats-io/kv` auto-installed and lockstep-pinned with nats-core, so that I cannot create a duplicate-client version skew by hand-installing.
32. As a library consumer reading errors in production, I want all new vocabulary (`:bucket-not-found`, `:wrong-revision`, `:invalid-key`) documented in the glossary and shipped as minor-bump additions per ADR 0009, so that upgrades are predictable.
33. As a contributor, I want the KV surface documented in the README (status, roadmap, usage), so that Phase 3 is discoverable on release.

## Implementation Decisions

- **Wrap natively (ADR 0003 discipline)**: the JVM leg wraps jnats `KeyValue`/`KeyValueManagement`; the CLJS leg wraps `@nats-io/kv`'s `Kvm`/`KV`. No portable reimplementation of KV semantics over the library's own JetStream layer; the impl never reaches around the native KV client to raw stream calls.
- **Dependency (ADR 0016)**: `@nats-io/kv` declared unconditionally in `:npm-deps`, lockstep-pinned with nats-core `3.3.1`; the KV JS import confined to the KV-specific CLJS impl namespace so core-only bundles stay KV-free. The existing core-bundle-check gains an `@nats-io/kv` marker assertion (no new build).
- **Handle model (ADR 0017 twin)**: `(kv conn)` → KV context, verified at entry with the same `:jetstream-not-enabled` semantics. Bucket CRUD takes the context; `open-bucket`/`create-bucket` resolve to a Bucket handle; all entry ops take the handle. `open-bucket` verifies existence → `:bucket-not-found`.
- **Codec**: per-Bucket only — connection default, `:codec` override at open/create; no per-op override. Decode failure in a one-shot `get` rejects with `:codec-error`; in a watch delivery it routes per ADR 0007 sink semantics.
- **Entry**: plain map `{:bucket :key :value :revision :created :operation}`; the value key is `:value` (KV domain language; `:data` stays reserved for Messages). `get` miss → `nil`. `:delta` included only where the natives populate it meaningfully (watch/history) — verify during implementation.
- **Writes**: `put`/`create`/`update` resolve to the bare Revision number; `delete`/`purge` resolve to nil; `delete`/`purge` accept an optional `:revision` guard. CAS failure is the single new canonical Error `:type :wrong-revision` (carrying `:key`) for both stale `update` and `create`-on-existing — ADR 0023 (KV speaks KV vocabulary, not its stream substrate).
- **Reads**: `get` takes an optional `:revision` for pinned reads, delivering marker Entries rather than hiding them (normalize both natives to this); `history` → fully-realized vector oldest→newest including Tombstones/purge markers (server-bounded ≤ 64 per key); `keys` → fully-realized vector of strings with optional subject-style filter (precedent: stream-names/consumer-names).
- **Watch**: Handler delivery (push, like core subscriptions — not the pull/refill model); signature `(watch bucket handler)` / `(watch bucket handler opts)` per house convention. Opts: `:deliver :latest|:history|:updates` (one closed option replacing the natives' flag set), `:keys` (one or many patterns), `:ignore-deletes?`, `:on-error`. Resolves to a watch handle carrying an `:initialized` Promise; `stop` ends it, fire-and-forget and idempotent (ADR 0012 spirit). Meta-only/headers-only mode is not exposed.
- **Bucket management**: `create-bucket`, `open-bucket`, `delete-bucket`, `bucket-names`, `list-buckets`, `bucket-status`. Closed config keys: `:bucket` (required), `:description`, `:history`, `:ttl-ms`, `:max-value-size`, `:max-bucket-size`, `:storage` (`:file`/`:memory`), `:replicas`, `:compression?`. Status maps reuse config key names where they overlap, plus observed counters; exact fields pinned against what both natives supply (shape parity, not cadence parity — ADR 0006 spirit).
- **Verify-then-include-or-ticket**: `update-bucket` and `purge-deletes` ship only if both native clients expose them at the pinned versions; otherwise each is dropped with a follow-up ticket.
- **Validation vocabulary**: new `:invalid-key` (key charset/shape, validated client-side before any wire call, carrying `:key`); malformed Bucket names reuse `:invalid-name`. Raised through the operation's own channel per ADR 0015.
- **Namespace conventions**: the facade excludes `get`, `keys`, `update` from clojure.core (the jetstream `next` precedent — the namespace-aliased call is the public name). Facade + per-leg impl namespaces under the KV area's impl segment; the KV protocol lives alongside the existing protocols.
- **No blocking layer**: no `blocking.kv` (Phase 2 precedent; Promises are the currency; ADR 0008's pull-subscription rationale does not apply).
- **Release**: lands as 0.4.0 with README status/roadmap updates; all vocabulary additions are minor-bump per ADR 0009. The README's Phase 3 roadmap line is reworded (jetstream-level direct get moved to its own ticket).
- Glossary terms (CONTEXT.md, already updated): Bucket, KV context, Bucket handle, Entry, Revision, Watch, Tombstone; Error/Validation-error entries extended. ADR 0023 records the substrate-vocabulary decision.

## Testing Decisions

Tests assert external behavior at the public facade — Entry shapes, resolved/rejected Promise values, canonical `:type`s — never native-client internals or wire details.

- **Primary seam — the portable facade against a real server**: one new portable `.cljc` KV test namespace runs identically on the JVM and Node against the shared ws-enabled, JetStream-enabled `nats-server` (no mocks; browser leg CI-only per ADR 0010). One `deftest` per behavior, `#?`-forked plumbing (blocking deref on `:clj`, `async` + promesa on `:cljs`), reusing the existing `with-conn`/test-support scaffolding. A distinct Bucket per test (the distinct-subject convention, lifted to Buckets) so the shared server never cross-feeds. Coverage: context verify-at-entry; open/create/delete buckets and `:bucket-not-found`; the five write verbs, bare-revision returns, and `:revision` guards; a genuine two-writer `:wrong-revision` race; `get` latest/missing/stored-nil/revision-pinned; `history` ordering with Tombstones; `keys` with and without filter; watch `:deliver` modes, `:initialized`, `:ignore-deletes?`, `stop` idempotence, and `:on-error` decode routing; per-bucket codec override.
- **Deep-module seam — pure impl helpers without a server** (the established deep-module pattern in the JetStream tests): bucket-config round-trip and `:unknown-config-key`/`:missing-required-key` guards; `:invalid-key` syntax validation; watch `:deliver` option validation; the native-failure → canonical-`:type` classifier (`:bucket-not-found`, `:wrong-revision`) on both legs.
- **Bundle isolation** rides the existing bundle-check task/CI step — extended marker scan, asserting a core-only bundle ships zero KV bytes.
- Both local legs (JVM + Node) plus clj-kondo lint must pass before commit; the known-bad-input discipline applies (watch a new guard go red before trusting its green).

## Out of Scope

- JetStream-level direct get (`get-message`, `:no-message-found`) — dropped from Phase 3; tracked as nts-01ktshvh6k6n.
- A `blocking.kv` convenience layer.
- Watch meta-only/headers-only mode (values stripped).
- Bucket topology config: `:placement`, `:republish`, `:mirror`/`:sources` — additive minor-bump material later.
- Per-operation codec overrides on entry ops (per-Bucket only).
- A jetstream-only bundle check asserting zero KV bytes (speculative until requested).
- Object Store (Phase 4) and the async adapters (Phase 5).

## Further Notes

- Two items are verify-first against jnats 2.25.3 and `@nats-io/kv` 3.3.1 before surfacing: `update-bucket` and `purge-deletes`. If either is missing on a leg, drop it from Phase 3 and open a follow-up ticket rather than reaching around the native KV client.
- Revision-pinned `get` on a marker revision must be normalized to "deliver the marker Entry" on both legs; each native's exact behavior here is unverified and must be pinned by a test.
- `:delta` semantics (whether it is meaningful outside watch/history deliveries) likewise get settled empirically, not inferred.
- The design conversation's full decision trail lives in the updated CONTEXT.md KV section and ADR 0023.

## Notes

**2026-06-11T02:54:14.826978924Z**

Phase 3 shipped as 0.4.0: nats-cljc.kv portable facade over jnats KeyValue and @nats-io/kv — KV context verified at entry, Bucket lifecycle + operator surface, five write verbs with Revision CAS (:wrong-revision), get/history/keys including revision-pinned marker reads, Watch with :deliver/:keys/:ignore-deletes?/:on-error and :initialized, per-Bucket Codec, zero KV bytes in core-only bundles. update-bucket dropped (nats.js lacks bucket-config update, won't-do follow-up nts-01ktt30j2ty5); jetstream-level direct get moved out and shipped separately as get-message (nts-01ktshvh6k6n). Released to Clojars as 0.4.0.
