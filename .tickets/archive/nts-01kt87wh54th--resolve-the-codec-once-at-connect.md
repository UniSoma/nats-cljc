---
id: nts-01kt87wh54th
title: Resolve the Codec once at connect
status: closed
type: chore
priority: 3
mode: afk
created: '2026-06-04T02:37:34.756507804Z'
updated: '2026-06-05T01:06:24.816622690Z'
closed: '2026-06-05T01:06:24.816622690Z'
tags:
- review
- efficiency
- codec
acceptance:
- title: The connection default codec is resolved once at connect; steady-state encode and decode perform no registry deref
  done: true
- title: A per-call `:codec` override still resolves correctly
  done: true
- title: Existing codec round-trip tests stay green on JVM and Node; clj-kondo clean
  done: true
---

## Description

The connection stores the codec keyword, never a resolved `ICodec`, so `codec/encode`/`codec/decode` call `resolve-codec` (an `@registry` deref + map lookup) on every publish, request, reply, and delivered message — pure repeated work on the hot path, since the connection's default codec cannot change for its lifetime.

Resolve the default codec once at connect and store the resolved record alongside the keyword (ADR 0011); keep the keyword resolution path only for per-call `:codec` overrides.

## Notes

**2026-06-05T01:06:24.816622690Z**

Resolved the connection default codec once at connect (ADR 0011). New codec/Prepared record holds the resolved ICodec (:impl) + stable id (:edn or :custom); codec/prepare builds it. resolve-codec and codec-id grew a leading Prepared branch (cond), so the hot path returns :impl with no @registry deref and a failure still names the keyword, not the record. core/connect now wraps impl/connect in an impl/then that stores (codec/prepare (:codec conn)) on the connection — so a bad default codec rejects the connect promise instead of failing lazily on first publish. effective-codec, codec/encode, codec/decode and the impl layers are otherwise untouched; per-call :codec overrides stay raw keyword/instance refs resolving through the registry. Verified registry-bypass via with-redefs empty-registry round-trip. JVM 95 tests / Node 72 tests green; clj-kondo clean. ADR 0011 consequences note the eager-at-connect resolution and the connect-vs-first-publish failure shift.
