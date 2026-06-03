---
id: nts-01kt78y774j0
title: Blocking unsubscribe [sub max] parity (auto-after-N in the pull model)
status: closed
type: feature
priority: 3
mode: afk
created: '2026-06-03T17:36:44.252981133Z'
updated: '2026-06-03T20:47:23.590686434Z'
closed: '2026-06-03T20:47:23.590686434Z'
tags:
- blocking
acceptance:
- title: (unsubscribe sub max) exists on the blocking layer; max validated with the canonical :invalid-max type (parity with core)
  done: true
- title: 'After N lifetime messages arrive, the buffer is poisoned: take-message returns nil (end-of-stream) and active? is false once drained — no hang'
  done: true
- title: messages/reduce over the sub terminates on its own at N
  done: true
- title: 'JVM test: subscribe -> (unsubscribe sub N) -> publish >N -> consumer sees exactly N then end-of-stream'
  done: true
- title: The 'not yet supported' caveat is removed from the blocking unsubscribe docstring
  done: true
links:
- nts-01kstxb0758h
- nts-01kt5jvdy5s1
---

## Description

The blocking layer's `unsubscribe` (`nats-cljc.blocking.core`) exposes only `[sub]`; the async core's `core/unsubscribe` exposes `[sub max]` (auto-unsubscribe after N lifetime messages, shipped in nts-01kt5jvdy5s1). This breaks parity in the direction that matters: CONTEXT.md's "Unsubscribe" glossary term defines `max` as part of the verb, and ADR 0008's "swap a single require" promise fails for `(unsubscribe sub 100)` (ArityException on the blocking layer, where it worked on async).

Surfaced by a code review of the blocking layer (review.md) and confirmed in a grill-with-docs session on 2026-06-03. Disposition: deliberate fast-follow — the gap is documented in the blocking `unsubscribe` docstring, this ticket implements the parity.

## Design

No core change needed — implementable entirely in `blocking/core.clj`.

**The hang trap:** a bare `([sub max] (core/unsubscribe (:inner sub) max))` stops the server at N but never flips the blocking handle's `ended` or poisons the buffer, so the (N+1)th `take-message` parks forever.

**Correct machinery:**
- Count arrivals in the `subscribe` `enqueue` handler (a lifetime counter atom). When the count reaches N, `end-sub!` (poison the buffer) so `take-message`/`messages` terminate at end-of-stream.
- Delegate the server-side stop to native `core/unsubscribe(:inner) max` so the server stops sending at exactly N.
- Validate `max` with the canonical `:invalid-max` contract (positive int <= 2147483647), parity with core (which already validates before the native call).

**Semantics to pin:** "received N" = arrived into the buffer (consistent with the async "arrived at the sub"), NOT taken by the consumer. If the sub has already received >= N at call time, it ends now. Reaching N is the normal non-error path to the ended state (ADR 0012).

On implementation, remove the "not yet supported" caveat from the blocking `unsubscribe` docstring.

## Notes

**2026-06-03T20:47:23.590686434Z**

Blocking unsubscribe reached parity with core: (unsubscribe sub max) auto-unsubscribes after N lifetime arrivals in the pull model. A lifetime counter atom {:received :max} on PullSubscription tracks arrivals; the enqueuing handler ends the sub gracefully at the Nth arrival via fire-once (CAS-guarded) end-after-max! that appends the end-of-stream sentinel AFTER the buffered N (flush-then-poison, unlike abrupt unsubscribe which drops in-flight). Server stop delegated to native core/unsubscribe(:inner) max. max validated as positive int <= 2147483647 with canonical :invalid-max ex-info, parity with core. Arming :max and reading :received in one swap closes the race so exactly one of the call/handler ends the sub. 'Not yet supported' caveat removed from docstring. JVM 83/183 green, Node 62/118 green, lint clean.
