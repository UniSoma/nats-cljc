---
id: nts-01ktdcwwhd76
title: 'Phase 2: JetStream (nats-cljc.jetstream)'
status: closed
type: epic
priority: 1
mode: afk
created: '2026-06-06T02:41:21.453576294Z'
updated: '2026-06-10T00:06:24.416282880Z'
closed: '2026-06-10T00:06:24.416282880Z'
tags:
- jetstream
- phase-2
acceptance:
- title: Obtain a JetStream context from a Connection; it rejects with :jetstream-not-enabled when JS is disabled, identically on JVM and Node
  done: true
- title: Stream and Consumer CRUD (create/update/delete/purge/info/list) work via portable closed kebab-keyword config maps with keyword enums, ms-in-key durations, and ISO-8601 timestamps
  done: true
- title: Acked publish returns a PubAck {:stream :seq :duplicate :domain}; :msg-id dedup and :expect optimistic-concurrency (:wrong-last-sequence) work; reserved Nats-* headers are rejected as :reserved-header
  done: true
- title: Pull delivery via next (Promise<msg-or-nil>), fetch (Promise<vector>), and consume (promise-return handler that measurably gates pulls); consume returns a drainable handle
  done: true
- title: ack/nak(+delay)/term/working are sync-nil and idempotent; double-ack returns Promise<bool>; delivered messages are pure data {:subject :data :headers :js {…}}
  done: true
- title: New operational and validation :types normalize identically on both legs; server-rejected configs are operational :jetstream-api-error; side-band consume errors reach a per-consume :on-error
  done: true
- title: OrderedConsumer replays a Stream gap-free with no acks
  done: true
- title: ClojureScript gets @nats-io/jetstream unconditionally and lockstep-pinned; a core-only browser bundle ships zero JetStream bytes
  done: true
- title: Suite passes on JVM + Node against a JetStream-enabled nats-server >= 2.12 on the anon :4222 server, with unique-stream-per-test isolation
  done: true
---

## Description

Phase 2 delivers JetStream under the portable `nats-cljc.jetstream` namespace, mirroring the shape of `nats-cljc.core`. The design was locked decision-by-decision; the rationale lives in ADRs 0016–0020 (and amendments to ADR 0002 + the README roadmap). This PRD is the spec of record for breaking the work into tickets.

## Problem Statement

Today nats-cljc gives me portable **core** NATS — pub/sub, request/reply, queue groups, codecs, lifecycle/status, errors — but delivery is fire-and-forget. If my consumer is down or crashes mid-process, the message is gone: there is no durable Stream, no replay, no at-least-once delivery, no acknowledgement, and no backpressured pull. I want the same write-once-run-everywhere experience (JVM, browser, Node) for JetStream, so I can build reliable, persistent messaging in Clojure *and* ClojureScript from one codebase — without it forcing an async dependency on me.

## Solution

A new `nats-cljc.jetstream` namespace that mirrors the portable shape of `nats-cljc.core`. From a **Connection** I obtain a single **JetStream context** with one call, then through it:

- Manage **Streams** and **Consumers** — create / update / delete / purge / info / list — from portable kebab-keyword config maps.
- **Acked publish** — publish into a Stream and await a **PubAck** `{:stream :seq :duplicate :domain}`, with opts for server-side dedup (`:msg-id`) and optimistic-concurrency (`:expect`).
- **Pull** messages three ways: one at a time (`next`), in a bounded batch (`fetch`), or continuously with backpressure (`consume`).
- Acknowledge delivered messages with **`ack` / `nak` / `term` / `working`**, and **`double-ack`** when losing an ack to a network blip is unacceptable.
- Read a stream with no acks via an **Ordered consumer**.

Delivered JetStream messages are plain data — `{:subject :data :headers :js {…}}` — with a normalized `:js` metadata map. Everything is one `.cljc` API, identical in shape on every platform.

## User Stories

1. As a portable developer, I want to obtain a JetStream context from a Connection with one call, so that I reach every JetStream operation through a single handle on all three platforms.
2. As a developer, I want obtaining the context to fail fast with `:jetstream-not-enabled` when the server/account has JetStream disabled, so that I learn of the misconfiguration at the handle rather than on my first publish — identically on the JVM and ClojureScript.
3. As a Stream author, I want to create a Stream from a portable kebab-keyword config map, so that I declare subjects/retention/storage/limits the same way on every platform.
4. As a Stream author, I want to update an existing Stream's config, so that I can change retention or limits without recreating it.
5. As a Stream author, I want to delete a Stream, so that I can tear down resources I no longer need.
6. As a Stream author, I want to purge a Stream, so that I can drop its messages while keeping the Stream definition.
7. As an operator, I want to read a Stream's info as a normalized map, so that I can inspect message counts, sequences, and config the same way on every platform.
8. As an operator, I want to list Streams (and their names), so that I can discover what exists on the server.
9. As a Consumer author, I want to create a durable Consumer from a portable config map (ack policy, deliver policy, ack-wait, max-deliver, filter subjects), so that I define delivery semantics portably.
10. As a Consumer author, I want to delete a Consumer, so that I can remove a delivery cursor I no longer need.
11. As an operator, I want to read a Consumer's info and list a Stream's Consumers, so that I can inspect delivery progress (delivered, ack-floor, pending) portably.
12. As a publisher, I want an Acked publish that resolves to a PubAck `{:stream :seq :duplicate :domain}`, so that I know my message was durably stored and at what sequence.
13. As a publisher, I want to set `:msg-id` on an Acked publish, so that the server deduplicates retries within its dedup window (the PubAck's `:duplicate` tells me when it did).
14. As a publisher, I want to set `:expect {:last-seq … :last-msg-id … :stream … :last-subject-seq …}`, so that I get optimistic-concurrency / dedup guarantees, with a mismatch surfaced as `:wrong-last-sequence`.
15. As a publisher, I want to set `:timeout-ms` on an Acked publish, so that a missing PubAck rejects rather than hanging.
16. As a publisher, I want to override the codec per Acked publish with `:codec`, so that a polyglot subject can carry JSON while my default stays EDN.
17. As a publisher, I want a reserved `Nats-*` header in my `:headers` to be rejected pre-flight as `:reserved-header`, so that I can't accidentally corrupt the dedup/expect mechanism.
18. As a worker, I want to `consume` a Consumer with a handler that may return a promise, so that the next message is delivered only when I am ready — backpressure with no core.async/missionary dependency.
19. As a worker, I want `consume` to return a drainable/unsubscribable handle, so that I can stop or drain delivery cleanly, exactly as with a core Subscription.
20. As a batch job, I want `fetch` to return a `Promise` of a bounded vector of messages, so that I can drain up to N messages and stop.
21. As a developer, I want `next` to return a `Promise` of a single message or `nil`, so that I can poll one message with a timeout.
22. As a worker, I want to tune the pull with `:batch` / `:threshold` (a message count) / `:expires-ms` / `:idle-heartbeat-ms` / `:max-bytes`, so that I control buffer size and refill cadence portably.
23. As a worker, I never want a `:slow-consumer` from pull, so that I rely on the pull gating itself (a slow handler simply slows the pull) rather than a drop-and-signal threshold.
24. As a worker, I want consume-time conditions (`:heartbeats-missed`, `:consumer-deleted`, `:exceeded-limits`) routed to a per-consume `:on-error`, so that I handle runtime delivery failures the same way as a core subscription's `:on-error`.
25. As a worker, I want a delivered JetStream message to be plain data `{:subject :data :headers :js {…}}`, so that I can inspect, log, and destructure it like a core message.
26. As a worker, I want the `:js` metadata map to carry `{:stream :consumer :stream-seq :delivery-seq :delivered :pending :redelivered :timestamp :domain}` with an ISO-8601 timestamp, so that I can reason about delivery position and redelivery portably.
27. As a worker, I want `ack` to mark a message processed (stop redelivery), returning nil synchronously, so that acking is simple and non-blocking.
28. As a worker, I want `nak` (optionally `:delay-ms`) to request redelivery, so that I can retry transient failures, optionally after a backoff.
29. As a worker, I want `term` (optionally `:reason`) to abandon a message so it is never redelivered, so that I can drop poison messages.
30. As a worker, I want `working` to postpone redelivery while I am still processing, so that a long task does not trip the ack-wait timer.
31. As a worker, I want `ack`/`nak`/`term`/`working` to be idempotent and never throw, so that a double-ack is harmless.
32. As a worker, I want `double-ack` to return a `Promise<bool>`, so that I can wait for the server to confirm my ack when at-least-once is not enough.
33. As a worker calling `(reply conn js-msg …)` by mistake, I want `:no-reply-subject`, so that I cannot publish garbage to the ack subject (the ack address lives under `:js`, not as a top-level `:reply`).
34. As a reader, I want an Ordered consumer that replays a Stream gap-free with no acks, so that I can do a simple single-client read without managing acknowledgements.
35. As a portable developer, I want JetStream errors normalized to canonical `:type`s (`:stream-not-found`, `:consumer-not-found`, `:wrong-last-sequence`, `:jetstream-api-error`, …) identical on both legs, so that I write one error-handling path via `(:type (ex-data e))`.
36. As a developer, I want a server-rejected config to surface as operational `:jetstream-api-error` carrying `{:code :description}`, so that I get the server's reason even though it could not be caught pre-flight.
37. As a developer, I want pre-flight caller-misuse caught as validation `:type`s (`:invalid-name`, `:unknown-config-key`, `:reserved-header`), so that typos and malformed names fail fast before any native call.
38. As a ClojureScript developer, I want JetStream installed automatically (no extra `npm install`) and version-pinned by the library, so that I never hit the duplicate-nats-core hazard.
39. As a core-only ClojureScript developer, I want a JetStream-free browser bundle, so that I do not ship JetStream bytes I never use.
40. As a maintainer, I want the suite to prove these behaviors on the JVM and Node against a JetStream-enabled server, so that portability is verified, not assumed.

## Implementation Decisions

**Dependency floor (ADR 0016).** `@nats-io/jetstream` is an **unconditional** dependency declared in `src/deps.cljs` `:npm-deps`, lockstep-pinned to the nats-core version by the library (so the library owns the exact-version pin and the duplicate-nats-core hazard cannot arise from consumer choice). "Force no dependency" means *non-NATS* dependency; the NATS family is one logical product. The JetStream JS import is confined to JetStream-specific namespaces (never the shared `nats_cljc.impl.js` core ns), so a core-only consumer's bundle excludes it. Root `package.json` gains the dep for the dev/test legs.

**Entry point (ADR 0017).** A single `(jetstream conn) → Promise<JetStream context>` holds **both** the data plane and the management plane; it hangs off the existing `Connection` via a new internal `JetStream` protocol in `protocol.cljc` that each platform's `Connection` record implements. The handle is **verified at entry**: a JS-info round-trip on *both* legs (added in the JVM off-thread wrap, native on CLJS) so `:jetstream-not-enabled` surfaces at the handle uniformly. Removing the JVM round-trip would silently break that symmetry.

**Surface scope.** Ships: Stream CRUD (create/update/delete/purge/info/list+names), Consumer CRUD (create/delete/info/list+names), Acked publish, the pull triad (`next`/`fetch`/`consume`), the ack family (`ack`/`nak`/`term`/`working`/`double-ack`), and the Ordered consumer. Out: push consumers, atomic-batch publish, direct-get/getMessage, KV, Object Store.

**Config & info representation.** Portable **closed** kebab-keyword maps; keyword enums (`:storage :file|:memory`, `:retention :limits|:interest|:work-queue`, `:ack-policy …`, `:deliver-policy …`); durations as **integer milliseconds with the unit in the key** (`:max-age-ms`, `:ack-wait-ms`, `:idle-heartbeat-ms`), translated to `Duration` (JVM) / Nanos (CLJS). An unrecognized/misspelled key is a validation error (`:unknown-config-key`) — no passthrough. `StreamInfo`/`ConsumerInfo`/`PubAck` are returned as **curated, normalized** kebab maps (not raw native passthrough). Timestamps are **ISO-8601 strings** everywhere (Info and message metadata).

**Acked publish.** `(publish js-ctx subject data opts) → Promise<PubAck>`, PubAck `{:stream :seq :duplicate :domain}`. Opts: `:msg-id`, `:expect {:last-seq :last-msg-id :stream :last-subject-seq}`, `:timeout-ms`, `:codec` (only `:data` is codec'd). `:msg-id`/`:expect` are the sanctioned way to set reserved `Nats-*` headers; a reserved key in user `:headers` is `:reserved-header`. On the JVM, prefer `publishAsync` (native `CompletableFuture`); fall back to off-thread sync `publish` if its in-flight cap misbehaves (impl-time grounding).

**Pull delivery & backpressure (ADR 0018).** Pull reuses the **ADR 0007 promise-return handler** — the runtime waits for the handler's returned promise before delivering the next message and refilling — *not* a channel/missionary adapter (that ADR 0002 premise predated ADR 0007 and is amended in place; the README roadmap is updated; core.async/missionary adapters remain Phase 3). `consume` = continuous handler → drainable handle; `fetch` = `Promise<vector>` (bounded); `next` = `Promise<msg-or-nil>`. Knobs `:batch`/`:threshold` (a **count**; JVM converts to percent)/`:expires-ms`/`:idle-heartbeat-ms`/`:max-bytes`. **No `:slow-consumer`/`:max-pending` in pull** (the client gates its own pulls). Consume-time side-band conditions normalize to operational `:type`s and route to a per-consume `:on-error` only.

**Delivered message & acks (ADR 0019).** Delivered JetStream messages are **pure data** `{:subject :data :headers :js {…}}`; `:js` = `{:stream :consumer :stream-seq :delivery-seq :delivered :pending :redelivered :timestamp :domain :ack-subject}` (`:redelivered` = delivered > 1). A per-leg lift (a `js-msg->raw` counterpart to `msg->raw`) reads native metadata and captures the ack subject, then discards the native object. Acks are **sugar over publish** of the version-independent protocol payloads (`+ACK`/`-NAK`/`-NAK{delay}`/`+WPI`/`+TERM`) to the captured ack subject — *not* native `.ack()` methods — so the ack path is one code path identical on both legs, idempotent-for-free, and the message stays pure data. `double-ack` is sugar over `request` to the ack subject. The ack address lives under `:js`, never as top-level `:reply`. The function is named `double-ack`, not `ack-sync` (ours is async).

**Error model (ADR 0020).** New operational `:type`s (shared err_code table, identical numbers both legs): `:jetstream-not-enabled` (10039), `:stream-not-found` (10059), `:consumer-not-found` (10014), `:wrong-last-sequence` (10071/10164), `:jetstream-api-error` (catch-all `{:code :description}`), plus side-band `:heartbeats-missed`/`:consumer-deleted`/`:exceeded-limits` (routed like `:slow-consumer`). **Server-rejected configs are operational, not validation** — they are detected after the native call, so they fail ADR 0015's "before any native call" line. New validation `:type`s (ADR 0015 open set): `:invalid-name`, `:unknown-config-key`, `:reserved-header`, plus the normalized nats.js `InvalidArgument`/`InvalidOperation` family. `:no-message-found` (10037) is deferred with direct-get. ADR 0006 carries a cross-ref to ADR 0020.

**Deep modules to extract (pure, isolated-testable):** (1) config translation + closed-key validation; (2) JetStream error normalization (err_code → `:type` table + side-band classification); (3) ack payload construction (msg+opts → wire bytes); (4) refill decision (buffered/threshold/batch → pull amount). **Per-leg/orchestration modules:** (5) the `nats-cljc.jetstream` facade; (6) the `JetStream` protocol + per-leg context; (7) the per-leg pull adapter; (8) the per-leg `js-msg->raw` lift.

## Testing Decisions

- **A good test asserts external behavior, not implementation** — round-trips and observable outcomes (create a Stream → publish → consume → ack → assert), error `:type`s, config map fidelity, backpressure timing — never private functions or internal state.
- **Unit tests (isolated, pure)** for the four deep modules: config kebab↔native round-trips and closed-key rejection; the err_code → `:type` table and side-band classification; ack-payload bytes for each verb (incl. `-NAK{delay}`); the refill arithmetic. These run with no server.
- **Integration `.cljc` tests on the JVM and Node** for the facade behaviors: context entry + `:jetstream-not-enabled`; Stream/Consumer CRUD + info/list round-trips; Acked publish + PubAck; `:msg-id` dedup (`:duplicate` true on retry); `:expect` optimistic-concurrency → `:wrong-last-sequence`; `next`/`fetch`/`consume` delivery; ack/nak(+delay)/term/working + redelivery behavior; idempotent double-ack returning `Promise<bool>`; backpressure (a slow promise-returning handler measurably gates pulls); Ordered consumer gap-free replay; error conditions (`:stream-not-found`, `:consumer-not-found`, server-rejected config → `:jetstream-api-error`); reserved-header and unknown-config-key guards.
- **Prior art:** the Phase-1 portable `.cljc` round-trip suite (one source, run on both legs) and the connection/error tests are the model for the integration layer; the deep-module units are plain `deftest`s.
- **Harness:** enable JetStream on the existing anon `:4222` server (a `jetstream {}` block + temp/gitignored `store_dir` in `ci/nats.conf`) — not a 5th server. Pin `nats-server` **≥ 2.12** (documented in `running-tests.md`; local 2.14.1 satisfies). Per-test isolation via **unique Stream names + explicit teardown**, defaulting to `:storage :memory` so streams vanish on restart even if a crashed test skips teardown. Browser leg stays CI-only (ADR 0010).

## Out of Scope

- **Push consumers** — pull-only for Phase 2 (CLJS removed legacy `subscribe`; JVM `consume(handler)` has no backpressure).
- **Atomic-batch publish.**
- **`getMessage` / direct-get** (and therefore the `:no-message-found` `:type`) — a Phase-3 prerequisite for KV, not Phase 2.
- **KV (`nats-cljc.kv`) and Object Store (`nats-cljc.object`)** — Phase 3 (the JVM client already bundles them; the CLJS deps follow the same unconditional pattern when they land).
- **core.async / missionary adapters** and **`request-many`** — Phase 3, layered on the same handle/handler.

## Further Notes

- Amendments made while grilling: ADR 0002's pull consequence is corrected in place (→ ADR 0018); the README roadmap's Phase-2 line is updated; ADR 0006 gains a cross-ref (→ ADR 0020). CONTEXT.md gained the glossary terms *JetStream, Stream, Consumer, Ordered consumer, JetStream context, Acked publish, PubAck, Ack, Double-ack* and extended the *Error* and *Validation error* sets.
- Impl-time known-unknowns to ground: jnats `publishAsync` in-flight cap (decides default publish path); the JVM JS-info round-trip used for verify-at-entry. The previously-flagged "JVM already-acked idempotency" question is dissolved by acks-over-publish (a redundant ack is a harmless publish the server ignores), and the `$JS.ACK.*` token-format/version risk is avoided by capturing the ack subject verbatim rather than parsing it.
- Versioning (ADR 0009): adding these normalized vocabulary members is a minor bump.

## Notes

**2026-06-10T00:06:24.416282880Z**

Phase 2 JetStream is complete: nats-cljc.jetstream ships the full pull-based surface portably on JVM and Node — verified-at-entry (jetstream conn) with :jetstream-not-enabled parity (ADR 0017), Stream and Consumer CRUD incl. update/purge/list/names via closed kebab config maps with keyword enums, ms-in-key durations, and canonical cross-leg timestamps, Acked publish with PubAck/:msg-id dedup/:expect/:reserved-header guard, the pull triad (next/fetch/consume) with promise-return backpressure and a drainable handle (ADR 0018), acks-over-publish incl. double-ack (ADR 0019), the normalized JetStream error model with side-band conditions routed to per-consume :on-error plus handler/decode throws (ADR 0020), the gap-free no-ack Ordered consumer, and the bb bundle:check + CI guard proving a core-only CLJS bundle ships zero @nats-io/jetstream bytes (ADR 0016). All 14 children closed; suite green on both local legs (171 JVM tests / 487 assertions, 142 Node tests / 399 assertions) against the JetStream-enabled :4222 server with unique-stream-per-test :memory isolation.
