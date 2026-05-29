---
id: nts-01kstxa377qb
title: Connection lifecycle & normalized status
status: open
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:22:37.286340516Z'
updated: '2026-05-29T22:22:37.286340516Z'
tags:
- needs-triage
acceptance:
- title: Each `:auth` shape connects against an appropriately configured server (`:token` and `:user`/`:pass` at minimum; nkey/jwt/creds where the server supports them)
  done: false
- title: '`:reconnect` options drive reconnection and the `:reconnecting` / `:reconnected` status events fire'
  done: false
- title: '`flush`, `drain` (connection and subscription), and `close` each return a promise that settles correctly; `drain` and `close` end the connection subscriptions'
  done: false
- title: Normalized `:on-status` events use the canonical `:type` set, identical in shape on all three platforms
  done: false
deps:
- nts-01kstx8ysgv5
---

## Description

Full connection lifecycle and the normalized status surface. `:auth` accepts `{:token ...}` / `{:user ... :pass ...}` / `{:nkey ... :seed ...}` / `{:jwt ... :seed ...}` / `{:creds "<string content>"}` (string content, not a file path — the browser has no filesystem). `:reconnect {:max :wait-ms :jitter-ms}` configures reconnection. `flush` / `drain` (connection and subscription) / `close` return promises. Lifecycle and async events are normalized to the canonical status `:type` set and delivered to `:on-status` identically on every platform.

Canonical status `:type`s: `:connected` `:disconnected` `:reconnecting` `:reconnected` `:closed` `:error` `:slow-consumer` `:lame-duck` `:servers-changed`.

ADRs: 0001 (transports/reconnect), 0006 (normalized status/errors), 0009 (the status `:type` set is part of the semver contract).
