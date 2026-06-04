---
id: nts-01kt87wh54th
title: Resolve the Codec once at connect
status: open
type: chore
priority: 3
mode: afk
created: '2026-06-04T02:37:34.756507804Z'
updated: '2026-06-04T02:53:25.156585690Z'
tags:
- review
- efficiency
- codec
acceptance:
- title: The connection default codec is resolved once at connect; steady-state encode and decode perform no registry deref
  done: false
- title: A per-call `:codec` override still resolves correctly
  done: false
- title: Existing codec round-trip tests stay green on JVM and Node; clj-kondo clean
  done: false
---

## Description

The connection stores the codec keyword, never a resolved `ICodec`, so `codec/encode`/`codec/decode` call `resolve-codec` (an `@registry` deref + map lookup) on every publish, request, reply, and delivered message — pure repeated work on the hot path, since the connection's default codec cannot change for its lifetime.

Resolve the default codec once at connect and store the resolved record alongside the keyword (ADR 0011); keep the keyword resolution path only for per-call `:codec` overrides.
