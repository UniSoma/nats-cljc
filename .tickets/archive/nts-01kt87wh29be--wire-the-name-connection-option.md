---
id: nts-01kt87wh29be
title: Wire the `:name` Connection option
status: closed
type: feature
priority: 3
mode: afk
created: '2026-06-04T02:37:34.665298326Z'
updated: '2026-06-04T20:27:42.445619086Z'
closed: '2026-06-04T20:27:42.445619086Z'
tags:
- review
- connect
acceptance:
- title: '`(connect {:servers [...] :name "orders-service"})` sets the connection name on both legs, visible in server-side connection info'
  done: true
- title: Omitting `:name` leaves the native default unchanged
  done: true
- title: clj-kondo clean; suite green on JVM and Node
  done: true
---

## Description

README lists `:name "orders-service"` as a connection option, but neither leg's connect destructures it — both bind only `[servers codec auth on-status reconnect]` — so the documented option is a silent no-op and the connection name never reaches the server.

Destructure `:name` and wire it to jnats' `.connectionName` and nats.js' `name` option on both legs. Verify the name appears in server-side connection info / monitoring.

## Notes

**2026-06-04T20:27:42.445619086Z**

Wired :name on both legs: destructured it in connect and threaded it to jnats' (.connectionName name) (JVM) and nats.js' (assoc :name name) (CLJS), each guarded by (cond-> name ...) so omitting :name leaves the native default. Enabled http_port 8222 on the anonymous server (ci/nats.conf) so the suite can read /connz. Two new core_test.cljc deftests, both legs: connect-name-reaches-the-server asserts the name shows up in the server's /connz monitoring (RED-verified — without the wiring the connz entry carries no name field), and connect-without-name-keeps-the-native-default asserts the native option stays nil/unset. clj-kondo clean; full suites green on JVM (94 tests) and Node (71 tests).
