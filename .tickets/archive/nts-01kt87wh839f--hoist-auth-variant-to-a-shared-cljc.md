---
id: nts-01kt87wh839f
title: Hoist `auth-variant` to a shared `.cljc`
status: closed
type: chore
priority: 3
mode: afk
created: '2026-06-04T02:37:34.851688029Z'
updated: '2026-06-05T01:41:06.248118359Z'
closed: '2026-06-05T01:41:06.248118359Z'
tags:
- review
- reuse
- auth
acceptance:
- title: '`auth-variant` lives in one shared `.cljc` namespace and both legs call it, with no verbatim duplication'
  done: true
- title: A single shared test pins the variant classification including the `:seed`-precedence ordering
  done: true
- title: Auth behavior is unchanged on both legs; clj-kondo clean; suite green on JVM and Node
  done: true
---

## Description

`auth-variant` — the pure `{:keys [token user nkey jwt creds]}` -> tag classifier whose cond ordering encodes the `:seed`-precedence rule — is duplicated verbatim in both impl legs, as is the `with-auth` `case` skeleton's five branch keys. With no compiler or test linking them, the two copies can drift so the same `{:auth ...}` map silently selects different credentials on JVM vs JS — an undetectable parity break in a security-sensitive path.

Move `auth-variant` (and the shared branch-key contract) into a shared `.cljc` seam, following the `nats-cljc.error`/`codec` precedent, with one cross-leg test.

## Notes

**2026-06-05T01:41:06.248118359Z**

Hoisted auth-variant into a shared nats-cljc.auth .cljc seam (ADR 0005), matching the error/codec precedent. Removed the byte-identical defn- from impl/jvm.clj and impl/js.cljs; both with-auth cases now dispatch on (auth/auth-variant auth) via a :require. New cross-leg test/nats_cljc/auth_test.cljc pins each shape's classification plus the :seed-precedence ordering (jwt+nkey+seed -> :jwt) so the legs can't drift. clj-kondo clean; suite green JVM (97 tests) and Node (74 tests).
