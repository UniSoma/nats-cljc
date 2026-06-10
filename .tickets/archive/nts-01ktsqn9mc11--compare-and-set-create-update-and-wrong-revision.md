---
id: nts-01ktsqn9mc11
title: 'Compare-and-set: create, update, and :wrong-revision'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:20.231678015Z'
updated: '2026-06-10T23:27:00.873858233Z'
closed: '2026-06-10T23:27:00.873858233Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: create resolves to the new Revision on an absent key and rejects with :wrong-revision carrying :key when the key already exists
  done: true
- title: update with the latest Revision resolves to the new Revision; update with a stale Revision rejects with :wrong-revision carrying :key
  done: true
- title: A genuine two-writer race test shows exactly one winner and one :wrong-revision loser
  done: true
- title: The native-failure to :wrong-revision classifier is covered as a deep-module seam on both legs
  done: true
- title: Portable facade tests pass on both legs
  done: true
deps:
- nts-01ktsqmtyszc
---

## Description

First-writer-wins and revision-guarded writes on the Bucket handle. `(create bucket key value)` succeeds only when the key is absent, enabling first-writer-wins initialization and locks. `(update bucket key value revision)` succeeds only when the expected Revision is still latest, so concurrent writers cannot silently clobber each other. Both resolve to the new Revision as a bare number.

A lost compare-and-set race — a stale update or a create on an existing key — rejects with the single canonical Error `:type :wrong-revision` carrying the `:key`, per ADR 0023: callers dispatch on KV vocabulary without knowing KV is stream-backed. The native-failure-to-canonical-type classifier is a deep-module seam covered without a server on both legs.

## Notes

**2026-06-10T23:27:00.873858233Z**

Shipped KV compare-and-set on the Bucket handle: (create bucket key value) for first-writer-wins and (update bucket key value revision) for revision-guarded writes, both resolving to the new Revision as a bare number. A lost race rejects with the canonical :wrong-revision carrying the contested :key — the substrate's wrong-last-sequence codes (10071/10164) route through a new portable classifier (kv.impl.error/cas-error-data), with per-leg seams (cas-off-thread on the JVM, with-cas-error on CLJS) feeding it the key; non-race failures keep their Bucket-verb faces. Covered by deep-module classifier units (no server) plus live facade tests on both legs, including a genuine two-writer race asserting exactly one winner and one :wrong-revision loser, and a clobber-proof check that a lost write leaves the guarded value untouched. Lint clean; full suite green on JVM (195 tests) and Node (166 tests). Commit 810eeaf.
