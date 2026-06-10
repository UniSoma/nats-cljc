---
id: nts-01ktsqmtyszc
title: 'Entry round trip: put and get with the per-Bucket Codec'
status: open
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:05.204822756Z'
updated: '2026-06-10T21:40:05.204822756Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: put resolves to the new Revision as a bare number
  done: false
- title: get resolves to an Entry map {:bucket :key :value :revision :created :operation} with :value decoded by the Bucket's Codec
  done: false
- title: get on an absent key resolves to nil; a stored nil value resolves to an Entry with :value nil — the two are distinguishable
  done: false
- title: A :codec override at open/create governs all reads and writes through that Bucket handle; the connection default applies otherwise
  done: false
- title: Decode failure in a one-shot get rejects with :codec-error
  done: false
- title: Malformed keys reject client-side with validation :type :invalid-key carrying :key, before any wire call (key-syntax rules covered as a deep-module seam)
  done: false
- title: Portable facade tests cover all of the above on both legs
  done: false
deps:
- nts-01ktsqmf2j98
---

## Description

The first entry operations on a Bucket handle. `(put bucket key value)` resolves to the new Revision as a bare number, immediately usable as the expected Revision of a follow-up compare-and-set. `(get bucket key)` resolves to an Entry — a plain map `{:bucket :key :value :revision :created :operation}` — whose `:value` is decoded through the Bucket's one Codec. A get on an absent key resolves to nil so callers branch with if-let; a stored nil value stays distinguishable (`{:value nil …}` vs nil).

The Bucket handle binds one Codec: the connection's default unless `:codec` is overridden at open/create (per-Bucket only — no per-op override). Decode failure in a one-shot get rejects with `:codec-error`.

Client-side key-syntax validation raises the validation `:type :invalid-key` carrying `:key` before any wire call (ADR 0015 channel), on every entry op that takes a key.

The facade excludes `get` (and later `keys`/`update`) from clojure.core — the jetstream `next` precedent: the namespace-aliased call is the public name.
