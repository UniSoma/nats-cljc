---
id: nts-01kt87whgsgq
title: Fix the README `:reply` wording
status: closed
type: chore
priority: 4
mode: afk
created: '2026-06-04T02:37:35.129165163Z'
updated: '2026-06-05T00:54:32.361453775Z'
closed: '2026-06-05T00:54:32.361453775Z'
tags:
- review
- docs
acceptance:
- title: README states that `:reply` is always present (nil when absent) and only `:headers` is conditional
  done: true
- title: No code change; matches CONTEXT.md and the delivered-message test
  done: true
---

## Description

README says `:headers`/`:reply` appear only when set, but `decode-msg` includes `:reply` unconditionally (nil when absent) and gates only `:headers` behind `cond->`; CONTEXT.md and the delivered-message test confirm the always-present `:reply` is intentional.

Update the README so it stops lumping `:reply` with `:headers` — `:reply` is always present (nil when absent); only `:headers` appears only when set. README-only.

## Notes

**2026-06-05T00:54:32.361453775Z**

README-only: reworded the Messages section so :reply is no longer lumped with :headers. Line 109 now states :reply is always present (nil when the sender expects no reply) and only :headers appears when set; the example map's :reply comment updated to match. Confirmed against decode-msg (core.cljc:113-117), which always emits :reply and gates only :headers behind cond->, plus CONTEXT.md's message map.
