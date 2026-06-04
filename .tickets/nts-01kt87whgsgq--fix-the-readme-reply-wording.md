---
id: nts-01kt87whgsgq
title: Fix the README `:reply` wording
status: open
type: chore
priority: 4
mode: afk
created: '2026-06-04T02:37:35.129165163Z'
updated: '2026-06-04T02:53:25.527576608Z'
tags:
- review
- docs
acceptance:
- title: README states that `:reply` is always present (nil when absent) and only `:headers` is conditional
  done: false
- title: No code change; matches CONTEXT.md and the delivered-message test
  done: false
---

## Description

README says `:headers`/`:reply` appear only when set, but `decode-msg` includes `:reply` unconditionally (nil when absent) and gates only `:headers` behind `cond->`; CONTEXT.md and the delivered-message test confirm the always-present `:reply` is intentional.

Update the README so it stops lumping `:reply` with `:headers` — `:reply` is always present (nil when absent); only `:headers` appears only when set. README-only.
