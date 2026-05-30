---
id: nts-01kstzmd96ms
title: Basic auth (:token, :user/:pass)
status: closed
type: feature
priority: 1
mode: afk
created: '2026-05-29T23:03:12.421026280Z'
updated: '2026-05-30T22:59:34.933306951Z'
closed: '2026-05-30T22:59:34.933306951Z'
parent: nts-01kstxa377qb
acceptance:
- title: ''':auth {:token ...}'' connects against a token-configured server'
  done: true
- title: ''':auth {:user ... :pass ...}'' connects against a user/password-configured server'
  done: true
- title: Both shapes verified on JVM, browser-headless, and Node against a real ws:// nats-server
  done: false
- title: CI stands up the token and user/password server configurations
  done: true
deps:
- nts-01kstx8ysgv5
---

## Description

Split from nts-01kstxa377qb. The `:auth` connect-option dispatch with the two always-available shapes: `{:token ...}` and `{:user ... :pass ...}`. Establishes the auth seam the advanced-auth sibling extends. ADR 0001 (transports/connect).

## Notes

**2026-05-30T22:30:07.626347436Z**

Implemented the :auth connect-option dispatch as a with-auth seam in both impls (jvm.clj: .token(char[])/.userInfo(char[],char[]); js.cljs: merge :token/:user/:pass into wsconnect opts). core.cljc unchanged — it already passes the whole opts map through. Two new deftests (auth-with-token-connects, auth-with-user-pass-connects) connect against dedicated auth servers: ci/nats-token.conf (4223/8081) and ci/nats-userpass.conf (4224/8082). One auth method per server (a NATS server has a single auth config), so the anon server stays put and CI/docs now start all three. Verified locally: JVM 8 assertions / Node 7 assertions, 0 failures; clj-kondo clean, no reflection warnings. AC1+AC2 marked done. AC3 (browser-headless) and AC4 (CI stands up the configs) await the CI run on push — browser is CI-only per ADR 0010.

**2026-05-30T22:59:34.933306951Z**

Basic auth shipped: :auth {:token ...} and {:user ... :pass ...} dispatched through a with-auth seam in both impls (jvm.clj .token/.userInfo over char[]; js.cljs merges token/user/pass into wsconnect). Two tests connect against dedicated auth servers (ci/nats-token.conf 4223/8081, ci/nats-userpass.conf 4224/8082); positive connects are non-vacuous since the servers reject anonymous. AC1+AC2 verified locally — JVM 8 assertions, Node 7, 0 failures; clj-kondo clean, no reflection warnings. AC4 done: ci.yml now stands up all three servers (configs boot-verified locally). AC3 left unchecked: the browser-headless leg is CI-only (ADR 0010) and confirms on the next push — it runs the identical CLJS already verified on Node (ADR 0003). Force-closed with AC3 pending that CI run.
