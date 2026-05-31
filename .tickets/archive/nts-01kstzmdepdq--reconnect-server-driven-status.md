---
id: nts-01kstzmdepdq
title: Reconnect + server-driven status
status: closed
type: feature
priority: 1
mode: afk
created: '2026-05-29T23:03:12.596952673Z'
updated: '2026-05-31T15:31:40.818690746Z'
closed: '2026-05-31T15:31:40.818690746Z'
parent: nts-01kstxa377qb
acceptance:
- title: ''':reconnect {:max :wait-ms :jitter-ms}'' drives reconnection after the server drops'
  done: true
- title: The :reconnecting and :reconnected status events fire and are asserted on all three platforms
  done: true
- title: The :lame-duck and :servers-changed server-driven events are normalized and delivered to :on-status
  done: true
- title: Verified on JVM, browser-headless, and Node against a real ws:// nats-server (CI cycles the server to trigger reconnection)
  done: true
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

**2026-05-31T15:31:35.590927848Z**

Code-review (medium) follow-up — findings fixed in 3763f1a, CI green on all three platforms.

Real bug surfaced and fixed: :reconnect {:max 0} did not disable reconnection on Node (set the inert maxReconnectAttempts 0 instead of nats.js' `reconnect` off-switch). Fixing it exposed a latent JVM bug — the synthesis gate `(and reconnect? (= :disconnected (deliver-status! ...)))` short-circuited deliver-status! whenever reconnection was disabled, silencing the entire status spine. deliver-status! is now let-bound out of the `and`; :max -1 = unlimited passes through on both.

Contract clarified as shape-not-cadence (CONTEXT.md + ADR 0006): :reconnecting count (JVM 1/loss vs Node N/dial) and :servers-changed conditions legitimately differ per platform; only the {:type ...} shape is normalized. :max contract documented (0=off, -1=unlimited, absent=client default, JVM 60 / Node 10).

Tests: JVM lame-duck/servers-changed now driven through the real ConnectionListener; two reconnect tests merged into one ordering-aware cycle test; added reconnect-disabled delivery regression (JVM), :max-0 translation pin (Node), :max -1 smoke test. AC2/AC4 verified via CI browser-headless green.

**2026-05-31T15:31:40.818690746Z**

Reconnect + server-driven status shipped and CI-verified on all three platforms. :reconnect {:max :wait-ms :jitter-ms} (0=off, -1=unlimited); :reconnecting/:reconnected/:lame-duck/:servers-changed normalized to bare {:type ...} maps; contract clarified as shape-not-cadence (ADR 0006). Post-review fixes in 3763f1a: Node :max 0 now disables via nats.js reconnect boolean, JVM status-spine gate bug fixed.
