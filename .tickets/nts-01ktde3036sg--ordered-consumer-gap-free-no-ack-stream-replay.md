---
id: nts-01ktde3036sg
title: 'Ordered consumer: gap-free, no-ack stream replay'
status: open
type: feature
priority: 2
mode: afk
created: '2026-06-06T03:02:10.273856573Z'
updated: '2026-06-06T03:02:10.273856573Z'
parent: nts-01ktdcwwhd76
tags:
- jetstream
- phase-2
- ordered
acceptance:
- title: An ordered consumer replays all published messages of a Stream in sequence order, gap-free, with no acks, on both legs
  done: false
- title: It is ephemeral (leaves no durable consumer behind) and reuses the pull delivery surface (next/fetch/consume)
  done: false
- title: Portable integration test passes on JVM + Node
  done: false
deps:
- nts-01ktde2zr4f0
---

## Description

An Ordered consumer for single-client, gap-free replay reusing the pull triad (next/fetch/consume). (ordered-consumer js-ctx stream opts) yields a pull handle that replays the Stream in order taking NO acknowledgements; it is ephemeral, server-managed, and automatically recreated if a sequence gap appears. Covers user story 34.
