---
id: nts-01ktsqptj7fd
title: 'Release 0.4.0: README status, roadmap, and KV usage docs'
status: open
type: task
priority: 1
mode: hitl
created: '2026-06-10T21:41:10.309449300Z'
updated: '2026-06-10T21:41:10.309449300Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: README status, usage, and roadmap reflect the shipped KV surface; the Phase 3 roadmap line no longer promises jetstream-level direct get
  done: false
- title: All new error and validation vocabulary appears in the glossary, matching what shipped
  done: false
- title: 0.4.0 released
  done: false
deps:
- nts-01ktsqn9mc11
- nts-01ktsqp3v5vc
- nts-01ktsqp89yz9
- nts-01ktsqnpqbtp
- nts-01ktsqpd4y93
links:
- nts-01ktshvh6k6n
---

## Description

Phase 3 ships as 0.4.0 — all new vocabulary (`:bucket-not-found`, `:wrong-revision`, `:invalid-key`) is minor-bump additive per ADR 0009, so upgrades stay predictable.

Update the README: status header, a KV usage section, and the roadmap — rewording the Phase 3 roadmap line whose jetstream-level direct get moved to its own ticket (linked). Confirm the glossary (CONTEXT.md) and ADR 0023 cover every term actually shipped — including any verify-first ops that were dropped — then cut the release.
