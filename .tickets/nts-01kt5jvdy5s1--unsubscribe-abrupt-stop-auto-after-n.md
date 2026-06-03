---
id: nts-01kt5jvdy5s1
title: Unsubscribe (abrupt stop + auto-after-N)
status: open
type: feature
priority: 1
mode: afk
created: '2026-06-03T01:51:29.733323655Z'
updated: '2026-06-03T01:51:29.733323655Z'
tags:
- subscription
- api
acceptance:
- title: (unsubscribe sub) stops delivery abruptly (handler sees no further msgs; buffered/in-flight dropped, not delivered) and returns nil synchronously
  done: false
- title: (unsubscribe sub max) auto-unsubscribes after max lifetime messages; non-positive-int max throws ex-info {:type :invalid-max}
  done: false
- title: 'Idempotent no-op: unsubscribe after a prior unsubscribe, after drain(sub), and after connection close all return nil without throwing (JVM swallows IllegalStateException) — ADR 0012'
  done: false
- title: -unsubscribe added to Sub protocol; JVM routes via dispatcher AND dissocs the slow-consumer registry entry; JS via native sub; facade owns arities + validation
  done: false
- title: README/CONTEXT/ADR-0012 stay accurate; suite green on JVM + Node (browser CI-only per ADR 0010)
  done: false
---

## Description

Add the `unsubscribe` op — the abrupt sibling to the already-shipped graceful `drain(sub)`. Design resolved in a grilling session (2026-06-03); see CONTEXT.md (Unsubscribe glossary entry) and ADR 0012. The name was reserved across ADR 0005/0008, protocol.cljc, README.md, and jvm.clj:258 but never implemented; this slice implements it.

## Design

Boundary (ADR 0012 / CONTEXT.md): unsubscribe = abrupt, sync, returns nil, DROPS not-yet-delivered messages; drain = graceful + awaitable. Matches both natives.

Protocol: add `-unsubscribe` to the `Sub` protocol as a single primitive `(-unsubscribe [sub max])` (max = nil for no-limit, else positive int). Public `core/unsubscribe` owns the two arities + validation:
  ([sub]     -> (-unsubscribe sub nil))
  ([sub max] -> pos-int? else throw {:type :invalid-max}; (-unsubscribe sub max))

JVM (jnats): subs are dispatcher-owned, so route through the dispatcher, NEVER sub.unsubscribe(): (.unsubscribe dispatcher sub) / (.unsubscribe dispatcher sub max). Wrap in try/catch IllegalStateException -> nil (idempotent no-op, ADR 0012). Also (swap! registry dissoc dispatcher) like -drain, so an unsubscribed sub leaks no slow-consumer sink (see jvm.clj:258 comment).

JS (nats-core): (.unsubscribe sub) / (.unsubscribe sub max); already a no-op when closed; returns nil.

max semantics (native-confirmed, identical both clients): lifetime total from subscription start; if already >= max, stops now; never recalls already-delivered messages. No subscribe-time :max option (asymmetric across clients; the unsubscribe arity covers it race-free since max is a lifetime total).

Blocking layer: ADR 0008 already re-exports unsubscribe unchanged (sync) — wire it in when this lands.
