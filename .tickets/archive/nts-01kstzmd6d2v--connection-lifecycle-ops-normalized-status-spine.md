---
id: nts-01kstzmd6d2v
title: Connection lifecycle ops + normalized status spine
status: closed
type: feature
priority: 1
mode: afk
created: '2026-05-29T23:03:12.332714065Z'
updated: '2026-05-31T01:46:22.942346142Z'
closed: '2026-05-31T01:46:22.942346142Z'
parent: nts-01kstxa377qb
acceptance:
- title: Normalized status events reach :on-status as {:type ...} maps, identical in shape on JVM, browser, and Node, with the canonical status :type mapping in place
  done: true
- title: The baseline lifecycle types :connected, :disconnected, and :closed each fire and are asserted on all three platforms
  done: true
- title: flush, drain (connection and subscription), and close each return a promise that settles correctly
  done: true
- title: drain and close end the connection's subscriptions
  done: true
- title: One portable .cljc suite drives the lifecycle + status path green on JVM, browser-headless, and Node against a real ws:// nats-server
  done: true
deps:
- nts-01kstx8ysgv5
---

## Description

Split from nts-01kstxa377qb. The thinnest connection-lifecycle slice and the status seam every later lifecycle slice plugs into: normalized status events delivered to `:on-status` as a plain `{:type ...}` map, identical in shape on JVM, browser, and Node, with the canonical status :type mapping established. Exercises the baseline lifecycle types only (`:connected` / `:disconnected` / `:closed`); reconnect-driven and server-driven types arrive in sibling slices. Also delivers the teardown one-shots `flush`, `drain` (connection and subscription), and `close`, each returning a settling promise; `drain`/`close` end the connection's subscriptions. ADRs 0001 (transports), 0006 (normalized status), 0009 (the status :type set is part of the semver contract).

## Notes

**2026-05-31T01:30:09.964238797Z**

Lifecycle + status spine implemented via TDD (6 vertical slices). Normalized status spine: per-platform maps (impl.jvm event->type: CONNECTED/DISCONNECTED/CLOSED; impl.js status->type: disconnect/close) deliver bare {:type ...} to :on-status; nats.js emits no initial event so :connected is synthesized when wsconnect resolves; unmapped native events dropped (reconnect/server-driven types belong to nts-01kstzmdepdq). One-shots: nats/flush, nats/close (JVM wraps blocking flush/close in CompletableFuture; CLJS returns native Promise), nats/drain polymorphic over a connection (proto/-drain) vs a native Subscription (impl/drain-subscription via satisfies? proto/Conn). drain/close end the connection's subscriptions; sub-drain ends only its sub.

Faithful :disconnected (Option B): a clean close fires only :closed on BOTH clients (verified by probe), so :disconnected is provoked by a REAL link drop via the public, client-side forceReconnect (jnats) / reconnect (nats.js) — a genuine native DISCONNECTED/'disconnect', portable to the browser with zero server-cycling infra. Trigger is test-only (force-drop! reach-in, like the existing close! helper); no force-reconnect verb leaks into the public API. No new server configs — the anon server (:4222/:8080) covers it.

Verified locally green: JVM 14 tests/23 assertions, Node 14/23, clj-kondo 0/0. Browser-headless leg is CI-only (ADR 0010) — same .cljc, all CLJS APIs used are browser-safe; pending CI for AC2/AC5 browser confirmation.

**2026-05-31T01:46:22.942346142Z**

Connection lifecycle + normalized status spine shipped via TDD (6 vertical slices). :on-status receives bare {:type ...} maps, identical in shape across platforms, from per-platform mappings (jnats ConnectionListener event->type; nats.js status() status->type); :connected synthesized on CLJS where nats.js emits no initial event; unmapped events dropped (reconnect/server-driven types -> nts-01kstzmdepdq). :disconnected provoked faithfully by a real link drop via public forceReconnect/reconnect (test-only trigger; no public verb; no server-cycling infra). flush/drain/close return settling native promises; drain polymorphic over connection vs native subscription; drain/close end subscriptions. Verified: JVM 14/23, Node 14/23, clj-kondo 0/0. Browser-headless is CI-only (ADR 0010) — same .cljc, browser-safe APIs; browser AC confirmation rides on CI.
