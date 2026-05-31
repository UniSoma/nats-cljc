---
id: nts-01kstxatbw6k
title: Error-model completion across both clients
status: open
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:23:00.986432509Z'
updated: '2026-05-31T21:25:21.078845390Z'
acceptance:
- title: Each canonical error `:type` is reproduced and asserted with identical shape on both `io.nats:jnats` and `@nats-io/nats-core`
  done: false
- title: A handler that throws is caught and routed to `:on-status :error` (or the subscription `:on-error`) without killing the subscription
  done: false
- title: A decode failure on a subscription is routed to the per-sub `:on-error` / status `:error` sink as `:codec-error`
  done: false
- title: '`:slow-consumer` is surfaced as a status event and `:max-pending` is honored'
  done: false
deps:
- nts-01kstx9hs32y
- nts-01kstx9pbqe5
- nts-01kstzmd6d2v
- nts-01kstzmd96ms
---

## Description

Complete the canonical error normalization across both native clients so portable code always reads `(:type (ex-data e))` rather than branching on host exception types. Normalize the remaining members of the canonical set not covered by earlier slices: `:connect-failed`, `:connection-closed`, `:permissions-violation`, `:max-payload-exceeded`, `:protocol-error`, `:drained` (the request `:timeout`/`:no-responders` and the `:codec-error` arrive with their own slices).

Route async failures to their sinks: a throwing handler and a decode failure reach the connection `:on-status :error` sink and/or a per-subscription `:on-error`; surface `:slow-consumer` and honor `:max-pending`.

ADRs: 0006 (normalized errors — one-shots reject, async failures hit `:on-status :error` or per-sub `:on-error`), 0007 (delivery; slow consumer).

## Notes

**2026-05-31T17:44:09.782678455Z**

Design note for AC#4 (:slow-consumer surfaced + :max-pending honored), from investigation while reviewing nts-01kstxa6v2mm.

Today's subscribe dispatch (impl/jvm.clj + impl/js.cljs -subscribe) eager-drains both native clients to run the promise-return backpressure chain: JVM onMessage returns immediately after swap!; JS uses a {:callback} sub invoked synchronously per message. Consequence: under a *sustained-slow* (or hung) handler, the undelivered backlog grows UNBOUNDED in our in-process promise/CompletableFuture chain, and the native slow-consumer detection NEVER trips because we keep the clients' own queues empty. So neither :max-pending nor :slow-consumer is satisfiable by adding a knob on the current code.

Native mechanisms exist but are gated on the consumption model:
- JVM (jnats 2.25.3): Consumer.setPendingLimits(maxMsgs,maxBytes) / getPendingMessageCount (= dispatcher incoming MessageQueue depth) / getDroppedCount; over-limit path (NatsConnection ~1806-1839) drops + markSlow + ErrorListener.slowConsumerDetected(conn,consumer); default limit 524288 msgs. This only trips if the dispatcher queue fills, i.e. if onMessage BLOCKS. So the native path = rework onMessage to block on the handler's CompletionStage (deref) instead of eager-chaining.
- JS (nats-core 3.3.1): per-sub 'slow?:number' option fires SlowConsumerStatus{type,sub,pending} and getPending() works — but ONLY for the async-iterator model. setSlowNotificationFn throws 'callbacks don't support slow notifications' (protocol.js:141-142); getPending() is 'iterator-only' (core.d.ts:508-511). So the native path = rework -subscribe from {:callback} to async-iterator + (await (handler m)) in a detached loop; backlog then buffers in nats.js' QueuedIterator where slow/getPending apply.

Recommendation: honor :max-pending + surface :slow-consumer by reworking BOTH -subscribe to these native consumption models (a 'road 2'), NOT by bolting an our-layer pending counter onto the current eager-chain ('road 1'). Road 2 also deletes the manual tail-chaining and gets ordered/serial/one-at-a-time for free. Costs to weigh: JVM blocks one dispatcher thread per in-flight handler; JS needs the detached for-await loop wired into drain/close/unsubscribe; and :slow-consumer is per-subscription natively (jnats ErrorListener consumer arg; nats.js per-sub fn) whereas :on-status is connection-level, so the per-sub->status mapping/plumbing is a sub-decision.

The promise-return mechanism shipped in nts-01kstxa6v2mm is correct as-is for ordering/backpressure; this overflow/bounding work is correctly scoped here, not there. impl comments now carry a one-line caveat pointing at this ticket.

**2026-05-31T21:25:21.078845390Z**

Design decisions from grill-with-docs on docs/audits/phase1-premise-audit.md (2026-05-31):

1. SUPERSEDE, not revert. The promise-return contract in 4d5fc30 (ADR 0007) is correct/CI-green (nts-01kstxa6v2mm); its manual tail-chaining is replaced — not backed out — by this ticket's rework. No interim window without backpressure.

2. ROAD 2 confirmed (native-consumption rework), not road 1 (our-layer counter). JVM: onMessage blocks the dispatcher thread on the handler's CompletionStage so jnats' pending-limits (512K/64MB default) + slowConsumerDetected engage. CLJS: drop the {:callback} sub for the async-iterator + detached (await (handler m)) loop, where nats-core's slow?/getPending apply. Accepted costs: JVM pins one dispatcher thread per in-flight slow handler; CLJS must wire the detached for-await loop into drain/close/unsubscribe teardown. Road 2 also deletes the manual tail-chaining and yields ordered/serial/one-at-a-time for free.

3. :max-pending = SIGNAL-portable, DROP-native (grounded to source). nats-core QueuedIterator.push (queued_iterator.js:66) is unbounded — no drop; SlowNotifier.maybeNotify (protocol.js:79) only fires the notify fn. jnats setPendingLimits DROPS over-limit + signals. So the portable guarantee is the :slow-consumer SIGNAL on both legs at the :max-pending threshold; the hard drop is native-only (JVM drops at cap and at 512K/64MB default; CLJS does not auto-drop — consumer reacts to the signal). No JS pull-and-discard shedding this phase. ADR 0007 wording softened accordingly.

4. :slow-consumer routes to the per-sub :on-error ONLY (not connection-level :on-status). It is inherently per-subscription; the handler closes over its sub so identity is implicit, and :on-status stays a bare connection-level {:type ...}. :slow-consumer moved from the status :type set to the ERROR :type set (CONTEXT.md + ADR 0006 updated). This refines AC#4.

5. ADR 0006 kept as the target (NOT softened); the async-error sink ships WHOLE here. AC#2 (throwing handler -> :on-error), AC#3 (decode failure -> :codec-error sink), AC#4 (:slow-consumer + :max-pending), and the road-2 -subscribe rework are ONE indivisible change — same plumbing, same rewritten -subscribe. Code is temporarily behind ADR 0006 (A10: async failures swallowed) until this lands; that's a ticket-tracked gap, not an ADR overstatement.

6. :auth-invalid added to the canonical error :type set (CONTEXT.md + ADR 0006) — client-side credential validation before dial, distinct from :connect-failed; rejects the connect promise. Resolves audit H4 drift.
