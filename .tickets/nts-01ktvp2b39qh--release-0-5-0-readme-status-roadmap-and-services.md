---
id: nts-01ktvp2b39qh
title: 'Release 0.5.0: README status, roadmap, and services usage docs'
status: open
type: task
priority: 2
mode: afk
created: '2026-06-11T15:50:59.391720262Z'
updated: '2026-06-11T15:50:59.391720262Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: README status and roadmap reflect Phase 4 shipped; a services usage section covers create, respond/respond-error, service/error, stop, and discovery
  done: false
- title: CHANGELOG documents the full 0.5.0 surface including the nats-io 3.4.0 floor
  done: false
- title: The glossary covers all shipped services vocabulary
  done: false
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
