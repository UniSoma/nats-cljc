---
id: nts-01kstx9pbqe5
title: 'Codecs: built-ins, per-call override, custom protocol'
status: open
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:22:24.119127883Z'
updated: '2026-05-29T22:34:54.990175609Z'
acceptance:
- title: Each built-in codec (`:transit` `:json` `:edn` `:string` `:bytes`) round-trips a representative value on all three platforms
  done: false
- title: A per-call `:codec` option overrides the connection default on publish, subscribe, and request
  done: false
- title: A custom codec implementing the encode/decode protocol works wherever a keyword codec is accepted
  done: false
- title: An encode or decode failure surfaces `ex-info` `:type :codec-error`
  done: false
deps:
- nts-01kstx8ysgv5
---

## Description

Pluggable payload encoding. Built-ins `:transit` (default), `:json`, `:edn`, `:string`, `:bytes`, selectable as the connection default (`:codec`) or overridden per call on publish/subscribe/request. Custom codecs implement the `encode`/`decode` protocol and are accepted anywhere a codec keyword is. An encode or decode failure surfaces as `ex-info` `:type :codec-error`.

ADRs: 0004 (codec-centric, connection-default + per-call override, `:transit` default), 0006 (normalized errors).
