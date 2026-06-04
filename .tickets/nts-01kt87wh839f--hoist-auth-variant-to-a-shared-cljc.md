---
id: nts-01kt87wh839f
title: Hoist `auth-variant` to a shared `.cljc`
status: open
type: chore
priority: 3
mode: afk
created: '2026-06-04T02:37:34.851688029Z'
updated: '2026-06-04T02:53:25.242064870Z'
tags:
- review
- reuse
- auth
acceptance:
- title: '`auth-variant` lives in one shared `.cljc` namespace and both legs call it, with no verbatim duplication'
  done: false
- title: A single shared test pins the variant classification including the `:seed`-precedence ordering
  done: false
- title: Auth behavior is unchanged on both legs; clj-kondo clean; suite green on JVM and Node
  done: false
---

## Description

`auth-variant` — the pure `{:keys [token user nkey jwt creds]}` -> tag classifier whose cond ordering encodes the `:seed`-precedence rule — is duplicated verbatim in both impl legs, as is the `with-auth` `case` skeleton's five branch keys. With no compiler or test linking them, the two copies can drift so the same `{:auth ...}` map silently selects different credentials on JVM vs JS — an undetectable parity break in a security-sensitive path.

Move `auth-variant` (and the shared branch-key contract) into a shared `.cljc` seam, following the `nats-cljc.error`/`codec` precedent, with one cross-leg test.
