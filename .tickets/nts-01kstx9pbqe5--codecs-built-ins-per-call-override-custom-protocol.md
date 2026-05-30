---
id: nts-01kstx9pbqe5
title: 'Codecs: built-ins, per-call override, custom protocol'
status: open
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:22:24.119127883Z'
updated: '2026-05-30T19:11:17.632532970Z'
acceptance:
- title: A per-call `:codec` option overrides the connection default on publish, subscribe, and request
  done: false
- title: A custom codec implementing the encode/decode protocol works wherever a keyword codec is accepted
  done: false
- title: An encode or decode failure surfaces `ex-info` `:type :codec-error`
  done: false
- title: Each built-in codec (`:edn` `:string` `:bytes`) round-trips a representative value on all three platforms; `:edn` is the connection default
  done: false
- title: Opt-in codecs `:transit` and `:json` round-trip once their namespace is required (with the third-party dep present); referencing an unloaded codec keyword surfaces an actionable error
  done: false
deps:
- nts-01kstx8ysgv5
---

## Description

Pluggable payload encoding. Built-in dependency-free codecs `:edn` (default), `:string`, `:bytes`, selectable as the connection default (`:codec`) or overridden per call on publish/subscribe/request. `:transit` and `:json` are opt-in codecs in their own namespaces (`nats-cljc.codec.transit`, `nats-cljc.codec.json`) that a consumer requires after adding the third-party dependency — never forced on consumers (ADR 0004). Custom codecs implement the `encode`/`decode` protocol and are accepted anywhere a codec keyword is, via the same registry the opt-in codecs use. An encode or decode failure surfaces as `ex-info` `:type :codec-error`.

ADRs: 0004 (codec-centric, EDN default, Transit/JSON opt-in), 0006 (normalized errors).
