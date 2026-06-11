---
id: nts-01ktvnzj8kwp
title: 'Tracer bullet: host a Service, handle a request, respond — invoked with core/request'
status: closed
type: feature
priority: 2
mode: afk
created: '2026-06-11T15:49:28.461590035Z'
updated: '2026-06-11T16:30:40.785007891Z'
closed: '2026-06-11T16:30:40.785007891Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: (create conn {:name … :version … :endpoints …}) resolves to a running Service on both legs, with no context and no entry verification
  done: true
- title: An endpoint's :subject defaults to its :name when omitted; an explicit :subject and :queue-group are honored
  done: true
- title: An endpoint handler is an ordinary ADR-0007 Handler and (respond conn msg data) answers the request; the caller's plain core/request resolves with the decoded reply
  done: true
- title: Request decode and response encode go through the connection's default codec
  done: true
- title: (stop svc) resolves and tears the Service down (enough for test teardown)
  done: true
- title: The :services core-bundle-check and externs-check guards land, verified red on a service-bearing bundle and green core-only
  done: true
- title: Portable service_test.cljc runs identically on JVM and Node against the shared server
  done: true
deps:
- nts-01ktvnz0v6pp
---

## Description

The Phase 4 tracer bullet: a new `nats-cljc.service` facade with the ADR 0005 per-leg impl structure (`src/nats_cljc/service.cljc` + `service/impl/{jvm.clj,js.cljs}`, mirroring kv/jetstream), wrapping `io.nats.service` (in-jar with jnats — no extra JVM dependency) and `@nats-io/services` on CLJS. There is no Service context and nothing is verified at entry (ADR 0024) — `create` hangs directly off the Connection.

`(create conn config)` → Promise<Service>; config `{:name :version :description :metadata :codec :endpoints}`, each endpoint `{:name :subject :handler :queue-group :metadata}` with `:subject` defaulting to `:name`. Declarative-only; no Group noun — a consumer composes grouped subjects directly. Endpoint handlers are ordinary ADR-0007 Handlers. `(respond conn msg data opts?)` replies, conn threaded as in `core/reply`, routed through the native service message so native per-endpoint stats stay correct. The Service binds the connection's default codec at create (the `:codec` create override and per-respond override are the codec slice). `(stop svc)` → Promise exists for teardown (drain semantics and `:stopped` are the lifecycle slice). A client invokes an endpoint with plain `core/request` — no new verb.

The JS import is confined to the service CLJS impl namespace; a new `:services` entry in the shadow-cljs core-bundle-check / externs-check guards keeps core-only bundles service-free — watched red on a service-bearing bundle before trusting green (AGENTS.md discipline).

Tests: new portable `test/nats_cljc/service_test.cljc` — the highest seam, mirroring `kv_test.cljc` — facade-only, no mocks, against the existing anonymous :4222 server (services is pure core request-reply; no new ci/ server config). JVM (TCP) + Node (ws) locally, browser CI-only (ADR 0010).

## Notes

**2026-06-11T16:30:40.785007891Z**

Shipped nats-cljc.service facade + per-leg impls (ADR 0005/0024/0026). create defaults endpoint :subject to :name and binds the connection's default codec; the ADR-0007 handler receives the decoded msg carrying the native service message under ::native; respond encodes and routes through that native message for correct per-endpoint stats; stop tears the Service down. JVM wraps io.nats.service (in-jar, no extra dep), off-thread void stop(); CLJS wraps @nats-io/services confined to service.impl.js, Service wrapped in a JsService record (nats.js doesn't export ServiceImpl). Added Service + ServiceLifecycle to the protocol, nats-cljc.service to :externs-check, and a '$srv' marker to bundle:check — watched RED on a service-bearing bundle then GREEN core-only. Verified lint clean, externs green, service_test.cljc green on JVM (217 tests) + Node (188 tests). Next: JS handler backpressure is callback-style (the Node serialization gate slice addresses it).
