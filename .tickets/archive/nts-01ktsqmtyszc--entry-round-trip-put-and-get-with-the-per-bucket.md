---
id: nts-01ktsqmtyszc
title: 'Entry round trip: put and get with the per-Bucket Codec'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:05.204822756Z'
updated: '2026-06-10T23:01:59.238062166Z'
closed: '2026-06-10T23:01:59.238062166Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: put resolves to the new Revision as a bare number
  done: true
- title: get resolves to an Entry map {:bucket :key :value :revision :created :operation} with :value decoded by the Bucket's Codec
  done: true
- title: get on an absent key resolves to nil; a stored nil value resolves to an Entry with :value nil — the two are distinguishable
  done: true
- title: A :codec override at open/create governs all reads and writes through that Bucket handle; the connection default applies otherwise
  done: true
- title: Decode failure in a one-shot get rejects with :codec-error
  done: true
- title: Malformed keys reject client-side with validation :type :invalid-key carrying :key, before any wire call (key-syntax rules covered as a deep-module seam)
  done: true
- title: Portable facade tests cover all of the above on both legs
  done: true
deps:
- nts-01ktsqmf2j98
---

## Description

The first entry operations on a Bucket handle. `(put bucket key value)` resolves to the new Revision as a bare number, immediately usable as the expected Revision of a follow-up compare-and-set. `(get bucket key)` resolves to an Entry — a plain map `{:bucket :key :value :revision :created :operation}` — whose `:value` is decoded through the Bucket's one Codec. A get on an absent key resolves to nil so callers branch with if-let; a stored nil value stays distinguishable (`{:value nil …}` vs nil).

The Bucket handle binds one Codec: the connection's default unless `:codec` is overridden at open/create (per-Bucket only — no per-op override). Decode failure in a one-shot get rejects with `:codec-error`.

Client-side key-syntax validation raises the validation `:type :invalid-key` carrying `:key` before any wire call (ADR 0015 channel), on every entry op that takes a key.

The facade excludes `get` (and later `keys`/`update`) from clojure.core — the jetstream `next` precedent: the namespace-aliased call is the public name.

## Notes

**2026-06-10T23:01:59.238062166Z**

Shipped the first entry operations on a Bucket handle: (put bucket key value) resolves to the new Revision as a bare number; (get bucket key) resolves to the Entry map {:bucket :key :value :revision :created :operation} with :value decoded through the Bucket's one Codec, or nil on an absent key (a stored nil stays a distinguishable {:value nil ...} Entry; nats.js' tombstone-surfacing get is normalized to the absent-is-nil contract). create-bucket/open-bucket gained an opts arity whose :codec override is resolved once (codec/prepare) and bound onto the handle, governing all reads and writes through it; the connection default applies otherwise, and decode failure in a one-shot get rejects :codec-error. Key syntax ([-/_=.a-zA-Z0-9], no leading/trailing dot, no wildcards) is a deep-module seam in kv.impl.bucket (validate-key), raised pre-flight as :invalid-key carrying :key on both verbs. New BucketEntries protocol (-kv-put/-kv-get) extended onto JvmBucket/JsBucket with canonical-timestamp :created and operation keyword normalization. Portable facade tests plus deep-module units; full suite green on JVM and Node, clj-kondo clean.
