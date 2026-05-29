---
id: nts-01kstzmdepdq
title: Reconnect + server-driven status
status: open
type: feature
priority: 1
mode: afk
created: '2026-05-29T23:03:12.596952673Z'
updated: '2026-05-29T23:03:12.596952673Z'
parent: nts-01kstxa377qb
acceptance:
- title: ''':reconnect {:max :wait-ms :jitter-ms}'' drives reconnection after the server drops'
  done: false
- title: The :reconnecting and :reconnected status events fire and are asserted on all three platforms
  done: false
- title: The :lame-duck and :servers-changed server-driven events are normalized and delivered to :on-status
  done: false
- title: Verified on JVM, browser-headless, and Node against a real ws:// nats-server (CI cycles the server to trigger reconnection)
  done: false
deps:
- nts-01kstzmd6d2v
---

## Description

Split from nts-01kstxa377qb. Consumes the lifecycle/status seam to deliver the reconnect and server-driven lifecycle events. `:reconnect {:max :wait-ms :jitter-ms}` configures and drives reconnection after the server drops; the `:reconnecting` / `:reconnected` events fire. Also normalizes the remaining server-driven types `:lame-duck` and `:servers-changed`. (`:slow-consumer` and `:error` event production stay with the delivery / error-model slices, which already claim them.) ADRs 0001 (reconnect), 0006 (normalized status).
