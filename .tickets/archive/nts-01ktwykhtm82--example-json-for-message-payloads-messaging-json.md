---
id: nts-01ktwykhtm82
title: 'Example: JSON for Message Payloads (messaging.json-payloads)'
status: closed
type: task
priority: 2
mode: hitl
created: '2026-06-12T03:39:26.420166887Z'
updated: '2026-06-23T19:33:24.782613555Z'
closed: '2026-06-23T18:47:55.197118524Z'
parent: nts-01ktwyk19r7p
tags:
- examples
---

## Description

Port https://natsbyexample.com/examples/messaging/json/go to nats-cljc.

File: examples/examples/messaging/json_payloads.cljc (stub scaffolded; implement -main).
Surface: codec.json vs the EDN default
Run: `bb example:jvm messaging.json-payloads` / `bb example:node messaging.json-payloads` against the local ci/nats.conf server.

Done when: runs to completion on both legs, prints the upstream narrative as it goes, and cleans up its streams/buckets/consumers (idempotent re-runs). Log friction as ticket notes with gap:/wart:/doc:/win: prefixes (see umbrella nts-01ktwyk19r7p).

## Notes

**2026-06-23T18:19:45.057968682Z**

Hard-won mechanism findings (verify-don't-infer; confirmed by instrumenting onMessage, not reasoning about jnats). The example must sequence its two paths, and the only reliable "this path is done" signal is the handler/:on-error firing OR the connection drain — NOT flush+sub-drain:
- flush confirms only that the SERVER received what we sent. It does NOT wait for our own published messages to loop back and reach our handlers — that leg is async (reader thread -> dispatcher queue -> onMessage), uncoordinated with flush. (Retracts an earlier note's "MSG frames land before PONG so they're pending" reasoning: they land in the reader, not yet the dispatcher's processed set.)
- sub-drain (JvmSubscription -drain -> .drain sub, jvm.clj:137) settles on jnats' pending-message COUNT; it does not await onMessage completion, let alone the route-error!->:on-error path (jvm.clj:206-214). Instrumented: with publish/flush/drain-strict the invalid message never enters onMessage at all (sub torn down first), so its :on-error 'dropped' line vanishes — 15/15 fail. A 300ms sleep before drain makes both messages dispatch and :on-error fire, proving a timing race, not a logic error.
- connection drain (JvmConnection -drain -> .drain client, jvm.clj:291) DOES await in-flight delivery before close, so handler-delivered lines arrive reliably (12/12). run-example uses this.
- self-labeling lines + fire-both-paths does NOT work either: the two subscriptions deliver on separate dispatcher threads and their printlns interleave mid-line. Logical labels don't fix physical concurrent writes; the paths must run one at a time.

Not a bug: flush and drain each behave per NATS semantics; the fault is composing them as a self-publish round-trip barrier they never promise. gap candidate worth a separate ticket: should sub-level drain await in-flight onMessage (incl. :on-error dispatch)? It doesn't today.

**2026-06-23T18:35:12.016513544Z**

SHIPPED (current state). examples/messaging/json_payloads.cljc runs on both legs, exact-ordered output 10/10 JVM + 6/6 Node, clj-kondo clean. Working-tree only on main.

Two labeled blocks demonstrating two malformed-payload strategies:
- strict-path (strict.foo): connection :json codec auto-decodes; a bad payload (published with a per-call {:codec :string} override) fails decode and routes to subscribe :on-error as :codec-error — handler never sees garbage, message dropped (ADR 0006).
- salvage-path (salvage.foo): {:codec :bytes} + inline codec/decode :json, catch :codec-error -> codec/bytes->str raw fallback (keeps the bad message). Matches every upstream sibling: Deno m.json()/m.string(), Rust prints b"not json", Python catches JSONDecodeError and degrades to untyped.

Sequencing: example runs the two paths in order with a single (p/delay 200) BETWEEN them — enough for the strict block to round-trip and print (incl. its :on-error line) before salvage starts, and to keep the two dispatcher threads from interleaving (see findings note). No explicit flush or per-sub drain in the body; salvage trails into run-example's connection drain. 200ms is ~100x the ~2ms localhost round-trip. Timing-heuristic, not deterministic — accepted for a pedagogical example (upstream Go/Deno/Rust siblings sleep too), and chosen over a p/create :on-error->promise bridge that was correct but obscured the codec teaching point.

win: the codec seam collapses upstream's manual Marshal/Unmarshal to a plain publish of a Clojure map; codec/encode surfaces the SAME value on the JSON vs EDN wire side-by-side.

**2026-06-23T18:47:55.197118524Z**

Ported messaging.json-payloads to nats-cljc; runs on both legs (JVM + Node), exact-ordered output 10/10 JVM + 6/6 Node, clj-kondo clean. Two labeled blocks: strict-path (:json codec auto-decode, bad payload -> :on-error :codec-error, dropped per ADR 0006) and salvage-path (:bytes + inline codec/decode :json, catch -> raw bytes->str fallback, the upstream Deno/Rust/Python move). Paths sequenced with a single (p/delay 200) between them — keeps the two dispatcher threads from interleaving and lets the strict :on-error print before salvage starts; salvage trails into run-example's connection drain. See the two retained notes for the shipped shape and the drain/flush mechanism findings (incl. a sub-drain gap candidate).

**2026-06-23T19:33:24.782613555Z**

Finding #1 drain — Mode 2 + severity VERIFIED via probe (both legs, ci/nats.conf), probes reverted. Slow handler (200ms), publish 5, drain conn once handler#1 in-flight.

MODE 2 (buffered-message fate): NOT discarded. On Node the detached consume loop keeps draining nats.js's CLIENT-SIDE iterator buffer after drain() resolves — final started=5/completed=5 — even when close() is called immediately after drain. So no delivery loss to the handler. Mode 2 = (a), not (b). The fix is NOT 'drain the loop queue'.

MODE 1 confirmed on Node: drain() resolves EARLY (snapshot at drain-resolve: started=1 completed=0) — does NOT await the in-flight handler. JVM drain IS a barrier (drain-resolve: started=5 completed=5).

SEVERITY (the real defect, sharper than the original framing): a handler that USES the connection in its async tail (publish a reply, ack) has that work SILENTLY DROPPED on Node. Second probe: responder publishes to a sink observed on a separate conn. Node sink=0 (both with and without close, deterministic x3); JVM sink=5. Mechanism: handler tail runs after drain has closed/is draining the conn → publish is best-effort → nil (ADR 0014). So it's not message-delivery loss; it's the loss of conn-dependent side effects of in-flight handlers — exactly the graceful-shutdown guarantee drain exists to provide. Strong case for FIX over document (Q3).
