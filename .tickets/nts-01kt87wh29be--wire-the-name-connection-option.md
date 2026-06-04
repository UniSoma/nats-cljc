---
id: nts-01kt87wh29be
title: Wire the `:name` Connection option
status: open
type: feature
priority: 3
mode: afk
created: '2026-06-04T02:37:34.665298326Z'
updated: '2026-06-04T02:53:25.064495197Z'
tags:
- review
- connect
acceptance:
- title: '`(connect {:servers [...] :name "orders-service"})` sets the connection name on both legs, visible in server-side connection info'
  done: false
- title: Omitting `:name` leaves the native default unchanged
  done: false
- title: clj-kondo clean; suite green on JVM and Node
  done: false
---

## Description

README lists `:name "orders-service"` as a connection option, but neither leg's connect destructures it — both bind only `[servers codec auth on-status reconnect]` — so the documented option is a silent no-op and the connection name never reaches the server.

Destructure `:name` and wire it to jnats' `.connectionName` and nats.js' `name` option on both legs. Verify the name appears in server-side connection info / monitoring.
