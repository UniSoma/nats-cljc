---
id: nts-01ktsqnpqbtp
title: 'Operator surface: bucket-names, list-buckets, bucket-status'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:33.637811238Z'
updated: '2026-06-10T23:16:12.176425137Z'
closed: '2026-06-10T23:16:12.176425137Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: bucket-names resolves to a vector of strings naming every Bucket
  done: true
- title: list-buckets resolves to a vector of normalized status maps
  done: true
- title: bucket-status resolves to one normalized status map reusing config key names where they overlap; a missing Bucket rejects with :bucket-not-found
  done: true
- title: Status map shape is identical across both legs, pinned by test
  done: true
- title: Portable facade tests pass on both legs
  done: true
deps:
- nts-01ktsqmf2j98
---

## Description

Portable KV topology inspection on the KV context. `bucket-names` resolves to a vector of Bucket name strings; `list-buckets` resolves to a vector of normalized status maps; `bucket-status` resolves to one normalized status map and rejects with `:bucket-not-found` for a missing Bucket.

Status maps reuse the bucket-config key names where they overlap, plus observed counters; pin the exact field set against what both natives supply — shape parity, not cadence parity (ADR 0006 spirit). Precedent for the fully-realized vector returns: stream-names / list-streams.

## Notes

**2026-06-10T23:16:12.176425137Z**

Shipped the KV operator surface on the KV context: bucket-names resolves to a vector of Bucket name strings (jnats getBucketNames; on CLJS derived from draining Kvm.list, which has no names endpoint), list-buckets to a vector of normalized status maps, and bucket-status to one such map — bucket-config key names reused where they overlap (:bucket :description :history :ttl-ms :max-value-size :max-bucket-size :storage :replicas :compression?) plus observed :values/:bytes counters, with :description normalized to nil when unset and :storage routed back through the shared wire table. A missing Bucket rejects :bucket-not-found (jnats getStatus 10059; nats.js open+status round-trip), a malformed name pre-flight :invalid-name. Shape pinned field-by-field by a both-legs test (exact key set via dissoc :bytes equality); full suite green on JVM and Node, clj-kondo clean.
