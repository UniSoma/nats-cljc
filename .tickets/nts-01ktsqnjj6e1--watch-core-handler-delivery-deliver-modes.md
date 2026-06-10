---
id: nts-01ktsqnjj6e1
title: 'Watch core: handler delivery, :deliver modes, :initialized, stop'
status: open
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:29.377753998Z'
updated: '2026-06-10T21:40:29.377753998Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: watch pushes each matching Entry to the Handler, decoded through the Bucket's Codec
  done: false
- title: :deliver :latest (default) replays current values then streams updates; :history replays full history; :updates streams only new changes — each mode verified by test
  done: false
- title: An invalid :deliver value rejects as a validation error (deep-module seam)
  done: false
- title: The watch handle's :initialized Promise resolves when the initial replay completes
  done: false
- title: stop ends delivery and is idempotent — a second stop is a safe no-op
  done: false
- title: Portable facade tests pass on both legs
  done: false
deps:
- nts-01ktsqmtyszc
---

## Description

Watching a Bucket feels exactly like a core subscription. `(watch bucket handler)` / `(watch bucket handler opts)` per the house convention — handler positional, opts trailing — with each matching Entry pushed to the Handler (ADR 0007 delivery; push, not the pull/refill model), decoded through the Bucket's Codec.

One closed `:deliver` option replaces the natives' flag set: `:latest` (default) replays current values then streams updates, `:history` replays full history first, `:updates` streams only new changes — invalid flag combinations are unrepresentable, and an invalid value rejects as a validation error (deep-module seam).

The call resolves to a watch handle carrying an `:initialized` Promise that resolves when the initial replay completes, so cache builders can populate first and serve reads after. `(stop watch-handle)` ends the Watch, fire-and-forget and idempotent (ADR 0012 spirit). Meta-only/headers-only mode is not exposed.

`:delta` on watch deliveries: include only if verified meaningful on both natives.
