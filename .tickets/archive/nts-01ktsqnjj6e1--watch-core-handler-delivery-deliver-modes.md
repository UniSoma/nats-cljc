---
id: nts-01ktsqnjj6e1
title: 'Watch core: handler delivery, :deliver modes, :initialized, stop'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:29.377753998Z'
updated: '2026-06-11T00:27:42.159807302Z'
closed: '2026-06-11T00:27:42.159807302Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: watch pushes each matching Entry to the Handler, decoded through the Bucket's Codec
  done: true
- title: :deliver :latest (default) replays current values then streams updates; :history replays full history; :updates streams only new changes — each mode verified by test
  done: true
- title: An invalid :deliver value rejects as a validation error (deep-module seam)
  done: true
- title: The watch handle's :initialized Promise resolves when the initial replay completes
  done: true
- title: stop ends delivery and is idempotent — a second stop is a safe no-op
  done: true
- title: Portable facade tests pass on both legs
  done: true
deps:
- nts-01ktsqmtyszc
---

## Description

Watching a Bucket feels exactly like a core subscription. `(watch bucket handler)` / `(watch bucket handler opts)` per the house convention — handler positional, opts trailing — with each matching Entry pushed to the Handler (ADR 0007 delivery; push, not the pull/refill model), decoded through the Bucket's Codec.

One closed `:deliver` option replaces the natives' flag set: `:latest` (default) replays current values then streams updates, `:history` replays full history first, `:updates` streams only new changes — invalid flag combinations are unrepresentable, and an invalid value rejects as a validation error (deep-module seam).

The call resolves to a watch handle carrying an `:initialized` Promise that resolves when the initial replay completes, so cache builders can populate first and serve reads after. `(stop watch-handle)` ends the Watch, fire-and-forget and idempotent (ADR 0012 spirit). Meta-only/headers-only mode is not exposed.

`:delta` on watch deliveries: include only if verified meaningful on both natives.

## Notes

**2026-06-11T00:27:42.159807302Z**

Shipped the watch core on both legs (commit c9464ce). (watch bucket handler opts?) pushes each matching Entry to the Handler decoded through the Bucket's Codec (Tombstones included, :delta verified meaningful on both natives and carried); one closed :deliver option — :latest default replay-then-stream, :history full-history replay, :updates new-changes-only — with an invalid value rejecting pre-flight as validation :type :invalid-deliver from the bucket deep module. The watch handle carries an :initialized Promise resolving when the initial replay completes: jnats endOfData natively on the JVM; derived on CLJS (@nats-io/kv 3.x dropped initializedFn) from the delta-0 caught-up entry, immediately for :updates, and via a post-watch status round-trip for the empty-replay edge. stop ends delivery, returns nil, and is idempotent via a CAS guard on both legs. Covered by a deep-module validate-deliver test plus four live facade tests (latest/history/updates/stop-idempotence, including empty-replay :initialized); the mode mapping watched go red on a deliberate break per leg. JVM (203 tests/640 assertions) and Node (174/549) suites green, clj-kondo clean.
