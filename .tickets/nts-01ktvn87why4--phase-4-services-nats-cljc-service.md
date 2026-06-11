---
id: nts-01ktvn87why4
title: 'Phase 4: services (nats-cljc.service)'
status: open
type: epic
priority: 2
mode: afk
created: '2026-06-11T15:36:44.177203849Z'
updated: '2026-06-11T15:36:49.785503418Z'
tags:
- services
- prd
- phase-4
links:
- nts-01ktvn3fy51e
---

## Description

## Problem Statement

A consumer of `nats-cljc` can today connect, publish/subscribe, request-reply, run JetStream, and use KV — all under one portable `.cljc` API. But to expose a *discoverable, instrumented* request-reply service (the NATS "services"/"micro" pattern: a queue-subscribed handler set with auto-responders on `$SRV.PING|INFO|STATS.*`), they must hand-roll it over core request-reply on every platform, re-implementing endpoint stats, error-reply conventions, and discovery — and do it twice, once per native client, because jnats and `@nats-io/services` diverge sharply in shape. There is no portable way to host a Service or to discover Services others host.

## Solution

Add `nats-cljc.service`: a portable facade over the services framework in both wrapped clients. A consumer hosts a Service with one declarative async `create`, supplies ordinary Handlers for endpoints, and replies with explicit verbs that mirror `core/reply`. They query running Services with three Discovery functions on a Connection. The surface deliberately breaks the KV/JetStream "context" shape — services is pure core request-reply with no server feature to verify, so there is no Service context and nothing is checked at entry (ADR 0024). Application errors a Service returns are reply payloads, not normalized transport Errors (ADR 0025). The CLJS package `@nats-io/services` joins the unconditional NATS family, flooring the nats-io trio at `3.4.0` (ADR 0026).

## User Stories

1. As a service author, I want to create a Service with `(service/create conn config)` returning a Promise that resolves to a running Service, so that hosting follows the same one-shot async shape as the rest of the library (ADR 0002).
2. As a service author, I want to declare my endpoints as data in the create config (`:endpoints [{:name :subject :handler …}]`), so that my service definition is a value I can compose and inspect.
3. As a service author, I want an endpoint's `:subject` to default to its `:name` when omitted, so that the common case stays terse.
4. As a service author, I want each endpoint handler to be an ordinary ADR-0007 Handler (serial per endpoint, may return a promise for backpressure, must not block), so that I reuse the delivery model I already know from core subscriptions.
5. As a service author, I want to reply to a request with `(service/respond conn msg data)`, so that replying reads the same as `core/reply` and threads the connection the same way.
6. As a service author, I want to reply with a structured error via `(service/respond-error conn msg code description data?)`, so that callers and stats see a first-class error response.
7. As a service author, I want a handler that throws or returns a rejected promise to auto-reply a service error (code 500, description from the exception) and be counted in that endpoint's error stats, so that a bug never leaves a caller hanging until timeout.
8. As a service author, I want to override the codec on a single `respond` (ADR 0011), so that I can answer a polyglot client in the codec it used.
9. As a service author, I want the Service to bind one codec at create (the connection default unless overridden), so that request decoding and response encoding are consistent for that Service.
10. As a service author, I want `(service/stop svc)` to drain in-flight requests before tearing down and return a Promise, so that a graceful shutdown never drops a reply mid-request.
11. As a service author, I want a `:stopped` promise on the Service handle that resolves when the Service stops for any reason, so that I can react to shutdown (paralleling the Watch handle's `:initialized`).
12. As a service author, I want a service with zero endpoints to be legal (still answering `$SRV.*`), so that a discovery-only or placeholder service is expressible.
13. As a service author, I want `create` to reject a config missing `:name` or `:version` with a `:missing-required-key` validation error carrying the offending `:key`, so that I catch the mistake immediately and identically on both legs.
14. As a service author, I want a malformed service or endpoint *name* to raise `:invalid-name`, so that subject-token names are validated before any wire call, reusing the existing infrastructure-name rule.
15. As a service author, I want a non-semver `:version` to raise `:invalid-version` carrying the offending `:version`, validated portably before the native call, so that the failure shape is identical across legs from day one.
16. As a service author, I want two endpoints declaring the same `:name` to raise `:duplicate-endpoint` carrying the offending `:name`, so that the per-endpoint stats key collision is caught up front.
17. As a service client, I want `(service/ping conn opts?)` to resolve a Promise of a vector of identity maps for running Services, so that I can discover who is up.
18. As a service client, I want `(service/info conn opts?)` to resolve a vector of info maps (identity + `:description` + `:endpoints`), so that I can learn what each Service offers.
19. As a service client, I want `(service/stats conn opts?)` to resolve a vector of stats maps (identity + `:started` + per-endpoint counters), so that I can observe request/error counts and processing time.
20. As a service client, I want to narrow any discovery call by `:name` and `:id`, so that I can target a specific Service or instance.
21. As a service client, I want to bound a discovery fan-out by `:max-results` and `:timeout-ms`, so that the gather terminates predictably when I don't know how many Services exist.
22. As a service client, I want discovery results normalized to kebab-case EDN with the wire `type` discriminator dropped, durations as `:processing-time-ns`/`:average-processing-time-ns`, and `:started` as the canonical timestamp string, so that the shape matches KV/JetStream info/stats and is byte-identical across legs.
23. As a service client, I want to invoke an endpoint with an ordinary `core/request`, so that calling a service needs no new verb.
24. As a service client, I want `core/request` to resolve normally with the reply Message even when the Service answered with an error, so that an application error is data I branch on, not a thrown transport failure.
25. As a service client, I want `(service/error msg)` to return `nil` or `{:code … :description …}` from a reply Message, so that I opt into reading a service error without core pub/sub knowing about service headers.
26. As a service client, I want `:no-responders` to remain a normalized Error when nobody hosts the subject, so that I learn no Service is up through the standard error channel.
27. As a CLJS consumer, I want `@nats-io/services` installed automatically and pinned in lockstep with the rest of the nats-io trio, so that I never hand-pick a version that duplicates `nats-core` in my tree.
28. As a core-only CLJS consumer, I want zero service bytes in my browser bundle unless I require the service facade, so that the unconditional dependency costs me nothing.
29. As a JVM consumer, I want services available with no extra dependency (jnats carries `io.nats.service` in-jar), so that JVM/CLJS parity holds.
30. As a maintainer, I want the `3.3.1 → 3.4.0` trio bump proven green on JVM + Node before it lands, so that a behavior-breaking floor move is caught by us, not a consumer.

## Implementation Decisions

- **New namespace `nats-cljc.service`** with the per-leg impl structure of ADR 0005: `src/nats_cljc/service.cljc` (facade) + `src/nats_cljc/service/impl/{jvm.clj,js.cljs}` and any shared `.cljc` impl helpers, mirroring `kv`/`jetstream`.
- **No Service context, no entry verification** (ADR 0024). Two independent entry points off `Connection`: `create` (server side) and `ping`/`info`/`stats` (client side, Discovery).
- **Facade surface (server side):** `(create conn config) → Promise<Service>`; `config` = `{:name :version :description :metadata :codec :endpoints}`, each endpoint `{:name :subject :handler :queue-group :metadata}` with `:subject` defaulting to `:name`. No `Group` noun — a consumer composes grouped subjects directly. Declarative-only (no imperative add-endpoint in v1). `(stop svc) → Promise` drains; the handle carries a `:stopped` promise resolving to nil. No `reset` in v1.
- **Reply verbs:** `(respond conn msg data opts?)` and `(respond-error conn msg code description data? opts?)`, conn threaded as in `core/reply`, routed through the native service message so native per-endpoint stats stay correct. `code` is an integer. A thrown/rejected handler auto-error-replies (500) and is counted natively.
- **Facade surface (client side):** `(ping conn opts?)`, `(info conn opts?)`, `(stats conn opts?)` each `→ Promise<vector>`; `opts` = `{:name :id :max-results :timeout-ms}`. Bounded `$SRV.*` fan-out drained from the native `QueuedIterator` (JS) / `List` (JVM) into an EDN vector. No Discovery handle; no local introspection of a hosted Service (self-inspection is a wire request narrowed by `:name`/`:id`).
- **Handler delivery** follows ADR 0007 (push, serial, promise-return backpressure). JVM falls out of the dispatcher blocking on the `CompletionStage`. **JS serialization is a verification gate** (see Testing Decisions): if the native callback does not `await` the returned promise, the realization drives the endpoint async iterator instead, or the contract is narrowed for services and documented.
- **Error model** (ADR 0025): service application errors are reply payloads, not canonical `Error` `:type`s. `core/request` is unchanged and must not learn about `Nats-Service-Error` headers. `(service/error msg)` reads them.
- **Normalization:** kebab EDN, drop the wire `type` discriminator, `:processing-time-ns`/`:average-processing-time-ns` integers in nanoseconds, `:started` the canonical UTC-millis timestamp string (same form as KV `:created`). The per-endpoint custom `:data` blob passes through as parsed JSON→EDN, not via the connection codec.
- **Validation** (ADR 0015, strict from day one): reuse `:missing-required-key` and `:invalid-name`; add `:invalid-version` (carries `:version`) and `:duplicate-endpoint` (carries `:name`), both portable pre-flights that throw identically on each leg. Endpoint `:subject` syntax stays native/server-enforced (not a pre-flight). Empty/absent `:endpoints` is legal.
- **Dependency** (ADR 0026): `@nats-io/services` declared unconditionally in `src/deps.cljs` `:npm-deps`, pinned in lockstep; added to root `package.json` for the test legs. The whole nats-io trio (`nats-core`, `@nats-io/jetstream`, `@nats-io/kv`) lifts to `3.4.0` in one change. The JS import is confined to `nats_cljc.service.impl.js`; a new `:services` entry in the shadow-cljs `:core-bundle-check` / `:externs-check` guards keeps core-only bundles service-free.

## Testing Decisions

- **A good test exercises external behavior through the public `nats-cljc.service` facade only** — never an impl namespace or a native object — against a real `nats-server` with no mocks, asserting the portable contract (return shapes, normalized EDN, error/validation `:type`s) identically on every leg.
- **Seam:** one portable `test/nats_cljc/service_test.cljc`, the highest existing seam, mirroring `kv_test.cljc` and `jetstream_test.cljc`. Runs on JVM (TCP) + Node (ws) locally, browser CI-only (ADR 0010), using `test_support` helpers for connection setup.
- **Server:** the existing anonymous `:4222` server suffices — services is pure core request-reply, needing no JetStream or special feature. No new `ci/` server config.
- **Coverage:** create/host a Service and call its endpoints with `core/request`; success reply and `respond-error`; handler-throws → auto-500 + error count; `:stopped` resolution and drain-on-`stop`; the four validation cases (`:missing-required-key`, `:invalid-name`, `:invalid-version`, `:duplicate-endpoint`); discovery `ping`/`info`/`stats` shape, narrowing, and fan-out bounding; normalized stats fields (`:processing-time-ns`, `:started`); `(service/error msg)` reads/absences; `:no-responders` when no Service hosts a subject.
- **Verification gates as explicit tests, watched red-on-known-bad before trusting green** (AGENTS.md): (1) a slow async handler must demonstrably delay the next request on Node (serialization), and `:processing-time-ns` must reflect the awaited duration; (2) the semver pre-flight's accept-set must match both natives on borderline versions (`"1.0"`, `"1.2.3-rc1+build"`).
- **Prior art:** `kv_test.cljc` (codec binding, normalized entry/stats shapes, validation `:type` assertions, watch `:initialized` promise) and `jetstream_test.cljc` (context acquisition, ack/handler patterns).

## Out of Scope

- The native `Group` subject-prefix namespace as a portable noun (a consumer composes subjects directly).
- Imperative `add-endpoint`/`add-group` after create — config is declarative-only in v1.
- `reset()` (zeroing stats) on a hosted Service.
- A dedicated `service/request` verb that rejects on a service-error reply — `(service/error msg)` covers reading errors; revisit if a reject-on-error ergonomic proves wanted.
- The JS async-iterator (pull) handler style — push Handler only (no JVM analog).
- Local in-process introspection accessors on the Service handle — discovery covers self-inspection.
- semver/duplicate as *non-portable* native throws — v1 is strict, so these are in scope as portable pre-flights, not deferred.

## Further Notes

- The reply-verb decision (conn threaded) **overrides** the services-client research note (`docs/services-client-research.md`, the self-contained-`respond` suggestion), reconciled against `core/reply`'s established `(conn msg data)` shape.
- Follow-up ticket `nts-01ktvn3fy51e` tracks surveying what else the `3.4.0` floor unlocks, independent of shipping services.
- The decision record: ADR 0024 (no context / no entry verification), ADR 0025 (service errors are reply payloads), ADR 0026 (services joins the unconditional family, 3.4.0 floor); glossary terms Service, Endpoint, Discovery, and validation `:type`s `:invalid-version` / `:duplicate-endpoint` are in CONTEXT.md.
- Structure this as Phase 4, broken into tracer-bullet vertical slices via `knot:to-tickets`, mirroring how Phase 3 (KV) was sliced.
