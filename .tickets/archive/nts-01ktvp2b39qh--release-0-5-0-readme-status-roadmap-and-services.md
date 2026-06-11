---
id: nts-01ktvp2b39qh
title: 'Release 0.5.0: README status, roadmap, and services usage docs'
status: closed
type: task
priority: 2
mode: afk
created: '2026-06-11T15:50:59.391720262Z'
updated: '2026-06-11T18:52:09.510572208Z'
closed: '2026-06-11T18:52:09.510572208Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: README status and roadmap reflect Phase 4 shipped; a services usage section covers create, respond/respond-error, service/error, stop, and discovery
  done: true
- title: CHANGELOG documents the full 0.5.0 surface including the nats-io 3.4.0 floor
  done: true
- title: The glossary covers all shipped services vocabulary
  done: true
- title: 0.5.0 released to Clojars
  done: false
deps:
- nts-01ktvnzz2ejg
- nts-01ktvp0cttme
- nts-01ktvp0pb18n
- nts-01ktvp17f0r7
- nts-01ktvp1hsxc7
- nts-01ktvp1ynk2f
---

## Description

The Phase 4 release slice, mirroring the 0.4.0 precedent (nts-01ktsqptj7fd): README status and roadmap updated for services, plus a usage section for `nats-cljc.service` covering hosting (`create`, endpoints as data), replying (`respond`, `respond-error`), reading errors (`service/error`), lifecycle (`stop`, `:stopped`), and discovery (`ping`/`info`/`stats`). CHANGELOG cut with the full 0.5.0 surface, including the nats-io 3.4.0 floor (ADR 0026). Glossary/CONTEXT.md verified in sync for the shipped vocabulary — Service, Endpoint, Discovery, `:invalid-version`, `:duplicate-endpoint` — all minor-bump additions per ADR 0009. Released to Clojars as 0.5.0.

## Notes

**2026-06-11T18:52:09.510572208Z**

0.5.0 release prep, LOCAL ONLY. The 'released to Clojars' AC is intentionally deferred to the user per the slice scope (do all local work, stop before any outward-facing step). The other three ACs are done. Bumped version to 0.5.0 in build.clj + nats-cljc.core; cut CHANGELOG [0.5.0] covering the full nats-cljc.service surface and the nats-io 3.4.0 floor (ADR 0026) with compare links; README status/roadmap mark Phase 4 shipped, install coordinate bumped, added a Services usage section (create, endpoints-as-data, respond/respond-error, error, stop/:stopped, ping/info/stats). Glossary, deps.cljs 3.4.0 pin, ^:no-doc impl nss, and :services bundle/externs guards were already in place. Verified: lint clean; cljdoc.edn verify exit 0; version guard watched red-on-bad then green on 0.5.0; JVM 232 + Node 203 tests, 0 failures; bundle:check services-free; externs:check clean advanced compile. Committed a598f1b. Tag+push+deploy left for the user.
