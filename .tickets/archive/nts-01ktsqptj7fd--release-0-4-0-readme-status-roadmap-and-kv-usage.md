---
id: nts-01ktsqptj7fd
title: 'Release 0.4.0: README status, roadmap, and KV usage docs'
status: closed
type: task
priority: 1
mode: hitl
created: '2026-06-10T21:41:10.309449300Z'
updated: '2026-06-11T02:52:16.842251779Z'
closed: '2026-06-11T02:52:16.842251779Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: README status, usage, and roadmap reflect the shipped KV surface; the Phase 3 roadmap line no longer promises jetstream-level direct get
  done: true
- title: All new error and validation vocabulary appears in the glossary, matching what shipped
  done: true
- title: 0.4.0 released
  done: true
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

## Notes

**2026-06-11T01:41:20.981654244Z**

Release 0.4.0 prepared locally: README (status 0.4.0, install coord, new KV usage section, Phase 3 roadmap line reworded — no longer promises jetstream-level direct get, notes get-message shipped), CHANGELOG cut as [0.4.0] - 2026-06-11 with the full KV surface + the direct-get entry, CONTEXT.md glossary gained the missing :invalid-deliver validation type (the only vocabulary gap — :bucket-not-found, :wrong-revision, :invalid-key were already present), version bumped in build.clj + core.cljc. Lint clean; JVM leg 210/692 green; Node leg green 2 of 3 runs — one flaky failure in purge-deletes-removes-tombstoned-history-bucket-wide (kv_test.cljc:958, marker survived purge-deletes once). cljdoc.edn verified; commit e16e17d 'Release 0.4.0' tagged v0.4.0 locally; jar built, pom tag correct. Remaining: git push && git push --tags, then clojure -T:build deploy (CLOJARS_USERNAME/PASSWORD not in this shell), then verify cljdoc.

**2026-06-11T02:45:24.888665931Z**

v0.4.0 tag moved from e16e17d to beef3d1 so the release includes the tombstone-only :keys watch fixes (06be755..beef3d1); jar rebuilt at the new tag, pom scm tag verified. CHANGELOG and glossary already cover :invalid-keys (synced in 06be755). ACs 1-2 flipped done. Remaining: git push && git push --tags, clojure -T:build deploy (needs CLOJARS creds), verify cljdoc, flip AC 3, close.

**2026-06-11T02:45:53.738997892Z**

Correction to the previous note: the empty-:keys/glossary-sync commit is 06be55f (not 06be755); the fix range is 06be55f..beef3d1.

**2026-06-11T02:52:16.842251779Z**

0.4.0 released: README (status, KV usage section, Phase 3 roadmap line reworded — no jetstream-level direct get promise), CHANGELOG cut with the full KV surface, glossary covers all shipped vocabulary (:bucket-not-found, :wrong-revision, :invalid-key, :invalid-deliver, :invalid-keys). v0.4.0 tag moved to beef3d1 to include the tombstone-only :keys watch fixes; jar rebuilt at the tag. CI green, pushed, deployed to Clojars.
