---
id: nts-01kstzmd96ms
title: Basic auth (:token, :user/:pass)
status: open
type: feature
priority: 1
mode: afk
created: '2026-05-29T23:03:12.421026280Z'
updated: '2026-05-29T23:03:12.421026280Z'
parent: nts-01kstxa377qb
acceptance:
- title: ''':auth {:token ...}'' connects against a token-configured server'
  done: false
- title: ''':auth {:user ... :pass ...}'' connects against a user/password-configured server'
  done: false
- title: Both shapes verified on JVM, browser-headless, and Node against a real ws:// nats-server
  done: false
- title: CI stands up the token and user/password server configurations
  done: false
deps:
- nts-01kstx8ysgv5
---

## Description

Split from nts-01kstxa377qb. The `:auth` connect-option dispatch with the two always-available shapes: `{:token ...}` and `{:user ... :pass ...}`. Establishes the auth seam the advanced-auth sibling extends. ADR 0001 (transports/connect).
