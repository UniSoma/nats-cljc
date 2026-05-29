---
id: nts-01kstzmd6d2v
title: Connection lifecycle ops + normalized status spine
status: open
type: feature
priority: 1
mode: afk
created: '2026-05-29T23:03:12.332714065Z'
updated: '2026-05-29T23:03:12.332714065Z'
parent: nts-01kstxa377qb
acceptance:
- title: Normalized status events reach :on-status as {:type ...} maps, identical in shape on JVM, browser, and Node, with the canonical status :type mapping in place
  done: false
- title: The baseline lifecycle types :connected, :disconnected, and :closed each fire and are asserted on all three platforms
  done: false
- title: flush, drain (connection and subscription), and close each return a promise that settles correctly
  done: false
- title: drain and close end the connection's subscriptions
  done: false
- title: One portable .cljc suite drives the lifecycle + status path green on JVM, browser-headless, and Node against a real ws:// nats-server
  done: false
deps:
- nts-01kstx8ysgv5
---

## Description

Split from nts-01kstxa377qb. The thinnest connection-lifecycle slice and the status seam every later lifecycle slice plugs into: normalized status events delivered to `:on-status` as a plain `{:type ...}` map, identical in shape on JVM, browser, and Node, with the canonical status :type mapping established. Exercises the baseline lifecycle types only (`:connected` / `:disconnected` / `:closed`); reconnect-driven and server-driven types arrive in sibling slices. Also delivers the teardown one-shots `flush`, `drain` (connection and subscription), and `close`, each returning a settling promise; `drain`/`close` end the connection's subscriptions. ADRs 0001 (transports), 0006 (normalized status), 0009 (the status :type set is part of the semver contract).
