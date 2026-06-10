---
id: nts-01ktsqnpqbtp
title: 'Operator surface: bucket-names, list-buckets, bucket-status'
status: open
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:33.637811238Z'
updated: '2026-06-10T21:40:33.637811238Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: bucket-names resolves to a vector of strings naming every Bucket
  done: false
- title: list-buckets resolves to a vector of normalized status maps
  done: false
- title: bucket-status resolves to one normalized status map reusing config key names where they overlap; a missing Bucket rejects with :bucket-not-found
  done: false
- title: Status map shape is identical across both legs, pinned by test
  done: false
- title: Portable facade tests pass on both legs
  done: false
deps:
- nts-01ktsqmf2j98
---

## Description

Portable KV topology inspection on the KV context. `bucket-names` resolves to a vector of Bucket name strings; `list-buckets` resolves to a vector of normalized status maps; `bucket-status` resolves to one normalized status map and rejects with `:bucket-not-found` for a missing Bucket.

Status maps reuse the bucket-config key names where they overlap, plus observed counters; pin the exact field set against what both natives supply — shape parity, not cadence parity (ADR 0006 spirit). Precedent for the fully-realized vector returns: stream-names / list-streams.
