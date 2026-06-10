---
id: nts-01ktsqmf2j98
title: KV context and Bucket lifecycle (kv, create-bucket, open-bucket, delete-bucket)
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:39:53.042487591Z'
updated: '2026-06-10T21:59:46.552740541Z'
closed: '2026-06-10T21:59:46.552740541Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: (kv conn) resolves to a KV context on a JetStream-enabled server and rejects with :jetstream-not-enabled when JetStream is unavailable, identically on both legs
  done: true
- title: create-bucket resolves to a usable Bucket handle; unknown/missing config keys reject with :unknown-config-key / :missing-required-key, malformed Bucket names with :invalid-name (deep-module seam, no server)
  done: true
- title: open-bucket resolves to a Bucket handle for an existing Bucket and rejects with :bucket-not-found for a missing one
  done: true
- title: delete-bucket removes the Bucket; a subsequent open-bucket rejects with :bucket-not-found
  done: true
- title: '@nats-io/kv is auto-installed and lockstep-pinned with nats-core 3.3.1'
  done: true
- title: The core-bundle-check asserts a core-only bundle ships zero @nats-io/kv bytes
  done: true
- title: Portable facade tests run identically on JVM and Node against the shared server, one distinct Bucket per test
  done: true
---

## Description

The Phase 3 tracer bullet: a new portable KV facade wrapping each platform's native KV client (jnats KeyValue/KeyValueManagement on the JVM, @nats-io/kv Kvm/KV on Node and the browser), per ADR 0003 — no reimplementation of KV semantics over the library's own JetStream layer, and the impl never reaches around the native KV client to raw stream calls.

`(kv conn)` returns a KV context verified at entry, rejecting with `:jetstream-not-enabled` when the account lacks JetStream — the ADR 0017 twin of the jetstream context. `create-bucket` takes the context and a closed kebab-case config map (`:bucket` required, plus `:description :history :ttl-ms :max-value-size :max-bucket-size :storage :replicas :compression?`) and resolves to a Bucket handle; unknown or missing keys reject with `:unknown-config-key` / `:missing-required-key`, and malformed Bucket names with the validation `:type :invalid-name` (ADR 0015 channel). `open-bucket` verifies the Bucket exists and rejects with `:bucket-not-found` per ADR 0023 — KV vocabulary, never the stream substrate. `delete-bucket` decommissions a Bucket portably.

Dependency wiring per ADR 0016: `@nats-io/kv` declared unconditionally in npm-deps, lockstep-pinned with nats-core 3.3.1, with the KV JS import confined to the KV-specific CLJS impl namespace so core-only bundles stay KV-free. The existing core-bundle-check gains an `@nats-io/kv` marker assertion (no new build).

Facade and per-leg impl namespaces follow the JetStream precedent (impl segment under the KV area; the KV protocol alongside the existing protocols).

## Notes

**2026-06-10T21:59:46.552740541Z**

Phase 3 tracer bullet shipped: nats-cljc.kv portably wraps each platform's native KV client (jnats KeyValueManagement on the JVM, @nats-io/kv Kvm on CLJS) per ADR 0003. (kv conn) is the ADR 0017 twin of the jetstream context — verified at entry by a forced $JS.API.INFO round-trip on both legs (the CLJS Kvm constructs without one), rejecting :jetstream-not-enabled. create-bucket takes the closed kebab config (:bucket required, plus description/history/ttl-ms/max-value-size/max-bucket-size/storage/replicas/compression?) validated pre-flight (:unknown-config-key / :missing-required-key / :invalid-name, name pinned to both natives' [-\w]+); open-bucket forces an existence round-trip (getStatus on the JVM, status() after nats.js' verify-free bind) and delete-bucket decommissions — a missing Bucket rejects :bucket-not-found via the KV-faced 10059 re-mapping (ADR 0023, nats-cljc.kv.impl.error). @nats-io/kv 3.3.1 lockstep-pinned in deps.cljs/package.json; bundle:check and the CI guard gained the $KV. marker (verified red on a KV-bearing bundle, green core-only); externs-check covers nats-cljc.kv. Portable kv_test.cljc green on JVM (180/509) and Node (151/418), lint clean.
