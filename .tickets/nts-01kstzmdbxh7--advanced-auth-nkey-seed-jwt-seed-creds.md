---
id: nts-01kstzmdbxh7
title: Advanced auth (:nkey/:seed, :jwt/:seed, :creds)
status: open
type: feature
priority: 1
mode: afk
created: '2026-05-29T23:03:12.508031084Z'
updated: '2026-05-29T23:03:12.508031084Z'
parent: nts-01kstxa377qb
acceptance:
- title: ''':auth {:nkey ... :seed ...}'' connects against an nkey-configured server'
  done: false
- title: ''':auth {:jwt ... :seed ...}'' connects against a jwt-configured server'
  done: false
- title: ''':auth {:creds "<string content>"}'' connects using credentials passed as string content, not a file path'
  done: false
- title: Each shape verified on JVM, browser-headless, and Node against an appropriately configured real ws:// nats-server
  done: false
- title: CI stands up the nkey, jwt, and creds server configurations
  done: false
deps:
- nts-01kstzmd96ms
---

## Description

Split from nts-01kstxa377qb. The harder `:auth` shapes built on the basic-auth seam: `{:nkey ... :seed ...}`, `{:jwt ... :seed ...}`, and `{:creds "<string content>"}`. `:creds` takes credential string content, NOT a file path — the browser has no filesystem. Isolating these contains the non-trivial CI server-config work (this is where the original ticket's 'where the server supports them' hedge lived). ADR 0001.
