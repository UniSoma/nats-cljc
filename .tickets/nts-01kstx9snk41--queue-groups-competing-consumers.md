---
id: nts-01kstx9snk41
title: Queue groups (competing consumers)
status: open
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:22:27.507161086Z'
updated: '2026-05-29T22:22:27.507161086Z'
tags:
- needs-triage
acceptance:
- title: Multiple subscriptions sharing a `:queue` group each receive a disjoint share — each message delivered to exactly one member
  done: false
- title: Verified on JVM, browser, and Node against a real server
  done: false
- title: A non-queue subscription on the same subject still receives every message
  done: false
deps:
- nts-01kstx8ysgv5
---

## Description

Competing consumers. `(nats/subscribe conn subject handler {:queue "workers"})` joins a named queue group; the server load-balances each matching message so it reaches exactly one member of the group.

CONTEXT: Queue group. ADR 0007 (delivery).
