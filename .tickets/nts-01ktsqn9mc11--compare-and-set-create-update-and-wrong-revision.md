---
id: nts-01ktsqn9mc11
title: 'Compare-and-set: create, update, and :wrong-revision'
status: open
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:20.231678015Z'
updated: '2026-06-10T21:40:20.231678015Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: create resolves to the new Revision on an absent key and rejects with :wrong-revision carrying :key when the key already exists
  done: false
- title: update with the latest Revision resolves to the new Revision; update with a stale Revision rejects with :wrong-revision carrying :key
  done: false
- title: A genuine two-writer race test shows exactly one winner and one :wrong-revision loser
  done: false
- title: The native-failure to :wrong-revision classifier is covered as a deep-module seam on both legs
  done: false
- title: Portable facade tests pass on both legs
  done: false
deps:
- nts-01ktsqmtyszc
---

## Description

First-writer-wins and revision-guarded writes on the Bucket handle. `(create bucket key value)` succeeds only when the key is absent, enabling first-writer-wins initialization and locks. `(update bucket key value revision)` succeeds only when the expected Revision is still latest, so concurrent writers cannot silently clobber each other. Both resolve to the new Revision as a bare number.

A lost compare-and-set race — a stale update or a create on an existing key — rejects with the single canonical Error `:type :wrong-revision` carrying the `:key`, per ADR 0023: callers dispatch on KV vocabulary without knowing KV is stream-backed. The native-failure-to-canonical-type classifier is a deep-module seam covered without a server on both legs.
