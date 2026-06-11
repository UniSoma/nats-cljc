---
id: nts-01ktvp1hsxc7
title: 'Codec: bind at create, override per respond'
status: closed
type: feature
priority: 2
mode: afk
created: '2026-06-11T15:50:33.527082532Z'
updated: '2026-06-11T18:00:30.659922668Z'
closed: '2026-06-11T18:00:30.659922668Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: :codec in the create config overrides the connection default for both request decode and response encode on every endpoint
  done: true
- title: A per-call codec override on respond encodes that single reply in the overriding codec
  done: true
- title: A per-call codec override on respond-error does the same for the error reply's data
  done: true
- title: Portable facade tests pass on both legs
  done: true
deps:
- nts-01ktvnzz2ejg
---

## Description

The Service binds one codec at create — the connection default unless `:codec` in the create config overrides it — so request decoding and response encoding are consistent for that Service across all its endpoints. A single `respond` or `respond-error` may override the codec per call (ADR 0011, the established per-call override convention), so a service author can answer a polyglot client in the codec it used.

Tests: create a Service with a `:codec` differing from the connection default and assert both request decode and response encode honor it; then answer one request through a per-call override on each reply verb and assert that one reply (including an error reply's `data`) is encoded in the overriding codec.

## Notes

**2026-06-11T18:00:30.659922668Z**

Service binds one codec at create — connection default unless :codec in the create config overrides it (resolved once into a Prepared via codec/prepare inside the validate chain stage, so an unresolvable :codec rejects the create promise pre-flight). The bound codec rides the handler-delivered msg under a private ::codec key (set in decode-request, alongside ::native), so respond/respond-error encode the reply with the Service's codec, not the bare connection default. Added private reply-codec helper with precedence (:codec opts) > msg ::codec > (:codec conn); a per-call :codec on respond or respond-error overrides for that single reply (ADR 0011). Facade-only change in src/nats_cljc/service.cljc; both impls already destructure named config keys so the extra :codec passes through harmlessly. Verified by new portable facade test codec-binds-at-create-and-overrides-per-respond (conn default :edn, Service :codec :string, :string/:edn round-trip discriminator). Watched red-before-green twice on JVM: reverting respond's encode to the connection default, and dropping the :codec opts precedence. Gates green: bb lint 0/0, bb test JVM 791 / Node 692 assertions 0 failures, bb bundle:check + externs:check 0 warnings.
