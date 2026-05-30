---
id: nts-01kstx8ysgv5
title: 'Tracer bullet: connect -> publish -> subscribe round-trip'
status: open
type: feature
priority: 0
mode: afk
created: '2026-05-29T22:21:59.983903495Z'
updated: '2026-05-30T19:53:02.446156077Z'
acceptance:
- title: '`(nats/connect {:servers ...})` resolves to a `Connection` on JVM, browser, and Node'
  done: false
- title: '`(nats/publish conn subject data)` returns `nil` and the message reaches subscribers'
  done: false
- title: One portable `.cljc` suite drives the round-trip green on JVM, browser-headless, and Node against a real `ws://` nats-server
  done: false
- title: Core protocol + `.cljc` facade + both platform impl records are in place under `nats-cljc.*`
  done: false
- title: '`(nats/subscribe conn subject handler)` returns a `Subscription` synchronously; the handler receives `{:subject :data}` with `:data` EDN-decoded'
  done: false
deps:
- nts-01kstx8jppcc
---

## Description

The thinnest end-to-end path through every layer: open a connection, publish a message, receive it on a subscription — identical `.cljc` code green on JVM (TCP), browser (WebSocket), and Node (WebSocket). `:edn` codec only.

This slice stands up the architecture every later slice extends: the core protocol, the `.cljc` facade (`nats-cljc.core`, aliased `nats`), both platform impl records (the `io.nats:jnats` wrapper on the JVM; `@nats-io/nats-core` `wsconnect` on ClojureScript), and the **portable `.cljc` test harness** that runs one suite on all three targets against a **real `ws://` nats-server** (no mocks).

`connect` returns `Promise<Connection>`; `publish` is fire-and-forget returning `nil`; `subscribe` returns a `Subscription` synchronously and delivers `{:subject :data}` maps to the handler, one call per message. Establishes the baseline `ex-info`-with-canonical-`:type` error shape (full normalization lands in the error-model slice).

ADRs: 0001 (non-blocking core; JVM=>TCP, CLJS=>WebSocket), 0002 (native-promise one-shots + callback handler), 0004 (codec-centric, `:edn` default), 0005 (protocol + platform record behind a `.cljc` facade, `nats-cljc.*`), 0007 (handler delivery baseline).
