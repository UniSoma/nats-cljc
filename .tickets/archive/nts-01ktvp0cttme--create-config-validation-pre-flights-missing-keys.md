---
id: nts-01ktvp0cttme
title: 'Create-config validation pre-flights: missing keys, names, semver, duplicate endpoints'
status: closed
type: feature
priority: 2
mode: afk
created: '2026-06-11T15:49:55.668516541Z'
updated: '2026-06-11T16:54:44.235234062Z'
closed: '2026-06-11T16:54:44.235234062Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: create with :name or :version absent rejects with :missing-required-key carrying the offending :key, identically on both legs
  done: true
- title: A malformed service or endpoint :name raises :invalid-name before any wire call
  done: true
- title: A non-semver :version raises :invalid-version carrying the offending :version
  done: true
- title: Two endpoints sharing a :name raise :duplicate-endpoint carrying the :name
  done: true
- title: The semver accept-set is pinned against both natives on the borderline versions 1.0 and 1.2.3-rc1+build, with the guard watched red on a known-bad input
  done: true
- title: A config with empty or absent :endpoints passes validation and create resolves
  done: true
deps:
- nts-01ktvnzj8kwp
---

## Description

ADR 0015 strict-from-day-one validation, all portable pre-flights thrown identically on each leg before any native or wire call:

- A config missing `:name` or `:version` rejects with `:missing-required-key` carrying the offending `:key` (reused vocabulary).
- A malformed service or endpoint name raises `:invalid-name`, reusing the existing infrastructure-name rule.
- A non-semver `:version` raises the new `:invalid-version` carrying the offending `:version`.
- Two endpoints declaring the same `:name` raise the new `:duplicate-endpoint` carrying the offending `:name` (the per-endpoint stats key collision, caught up front).

Endpoint `:subject` syntax stays native/server-enforced — not a pre-flight. Empty or absent `:endpoints` is legal and must not trip validation (its $SRV.* discoverability is asserted in the discovery slice).

Verification gate from the epic's testing decisions, watched red on known-bad inputs: the semver pre-flight's accept-set must match both natives on borderline versions ("1.0", "1.2.3-rc1+build"), pinned by test against jnats and @nats-io/services at the 3.4.0 floor.

Seam: deep-module tests (no server) for the validation cases, mirroring the KV config-guard precedent, plus the facade rejection shape.

## Notes

**2026-06-11T16:54:44.235234062Z**

Added strict create-config pre-flights (ADR 0015/0024) in new portable deep module nats-cljc.service.impl.config, wired into facade create via resolved->then->bind so a sync throw rejects the promise identically on both legs. Reuses :missing-required-key (absent :name/:version) and :invalid-name; adds :invalid-version and :duplicate-endpoint. Semver accept-set pinned on '1.0' (reject) / '1.2.3-rc1+build' (accept), verified live against jnats validateSemVer AND @nats-io/services parseSemVer at the 3.4.0 floor; red-before-green watched by swapping re-matches->re-find. Verified: bb lint clean, bb test green JVM(223)+Node(194), bundle:check Services-free, externs:check exit 0.
