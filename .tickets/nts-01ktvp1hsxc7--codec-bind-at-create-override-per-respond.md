---
id: nts-01ktvp1hsxc7
title: 'Codec: bind at create, override per respond'
status: open
type: feature
priority: 2
mode: afk
created: '2026-06-11T15:50:33.527082532Z'
updated: '2026-06-11T15:50:33.527082532Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: :codec in the create config overrides the connection default for both request decode and response encode on every endpoint
  done: false
- title: A per-call codec override on respond encodes that single reply in the overriding codec
  done: false
- title: A per-call codec override on respond-error does the same for the error reply's data
  done: false
- title: Portable facade tests pass on both legs
  done: false
deps:
- nts-01ktvnzz2ejg
---

## Description

The Service binds one codec at create — the connection default unless `:codec` in the create config overrides it — so request decoding and response encoding are consistent for that Service across all its endpoints. A single `respond` or `respond-error` may override the codec per call (ADR 0011, the established per-call override convention), so a service author can answer a polyglot client in the codec it used.

Tests: create a Service with a `:codec` differing from the connection default and assert both request decode and response encode honor it; then answer one request through a per-call override on each reply verb and assert that one reply (including an error reply's `data`) is encoded in the overriding codec.
