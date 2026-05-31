---
id: nts-01kstzmdepdq
title: Reconnect + server-driven status
status: in_progress
type: feature
priority: 1
mode: afk
created: '2026-05-29T23:03:12.596952673Z'
updated: '2026-05-31T14:12:49.663345730Z'
parent: nts-01kstxa377qb
acceptance:
- title: ''':reconnect {:max :wait-ms :jitter-ms}'' drives reconnection after the server drops'
  done: true
- title: The :reconnecting and :reconnected status events fire and are asserted on all three platforms
  done: false
- title: The :lame-duck and :servers-changed server-driven events are normalized and delivered to :on-status
  done: true
- title: Verified on JVM, browser-headless, and Node against a real ws:// nats-server (CI cycles the server to trigger reconnection)
  done: false
deps:
- nts-01kstzmd6d2v
---

## Description

Split from nts-01kstxa377qb. Consumes the lifecycle/status seam to deliver the reconnect and server-driven lifecycle events. `:reconnect {:max :wait-ms :jitter-ms}` configures and drives reconnection after the server drops; the `:reconnecting` / `:reconnected` events fire. Also normalizes the remaining server-driven types `:lame-duck` and `:servers-changed`. (`:slow-consumer` and `:error` event production stay with the delivery / error-model slices, which already claim them.) ADRs 0001 (reconnect), 0006 (normalized status).

## Notes

**2026-05-31T14:12:49.663345730Z**

Reconnect + server-driven status shipped via TDD (4 vertical slices), consuming the lifecycle/status seam from nts-01kstzmd6d2v.

:reconnect option — {:max :wait-ms :jitter-ms} plumbed through a with-reconnect seam in both impls: jnats .maxReconnects/.reconnectWait(Duration)/.reconnectJitter(Duration); nats-core :maxReconnectAttempts/:reconnectTimeWait/:reconnectJitter. Absent keys leave each client's own defaults. (AC1)

Status :type additions (ADR 0009 minor-bump, locked vocabulary): :reconnected (jnats RECONNECTED / nats.js "reconnect"), :lame-duck (LAME_DUCK / "ldm"), :servers-changed (DISCOVERED_SERVERS / "update"). :reconnecting maps natively on nats.js ("reconnecting", self-gated to real attempts) but jnats has NO reconnecting event, so it is synthesized in the listener after :disconnected when reconnection is enabled (:max != 0), matching nats.js' signal. (AC2)

Reconnect trigger reuses the lifecycle slice's force-drop! (public forceReconnect/reconnect) — a real link drop that drives a full disconnect->reconnecting->reconnect cycle client-side, no server-cycling infra, portable to the browser. The reconnect tests configure :reconnect and assert :reconnecting + :reconnected reach :on-status.

Server-driven :lame-duck / :servers-changed have no portable client trigger (a lame-duck needs a server signal; a server-list change needs a cluster), so they are asserted at the REAL normalization seam: a new public impl/deliver-status! (the exact fn the live listener/pump call) is driven with the native event jnats/nats.js would emit, checking the canonical {:type ...} reaches :on-status. (AC3)

Verified locally green: JVM 18 tests/27 assertions, Node 18/27, clj-kondo 0/0, browser target compiles 0 warnings. Browser-headless leg is CI-only (ADR 0010) — same .cljc, all CLJS APIs used (.reconnect, plain JS status objects) are browser-safe. AC2 (all-three-platforms) and AC4 (browser-headless verification) left unchecked pending CI browser green.
