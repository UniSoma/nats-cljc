---
id: nts-01kstx8ysgv5
title: 'Tracer bullet: connect -> publish -> subscribe round-trip'
status: closed
type: feature
priority: 0
mode: afk
created: '2026-05-29T22:21:59.983903495Z'
updated: '2026-05-30T20:51:48.881664312Z'
closed: '2026-05-30T20:23:18.382548480Z'
acceptance:
- title: '`(nats/connect {:servers ...})` resolves to a `Connection` on JVM, browser, and Node'
  done: true
- title: '`(nats/publish conn subject data)` returns `nil` and the message reaches subscribers'
  done: true
- title: One portable `.cljc` suite drives the round-trip green on JVM, browser-headless, and Node against a real `ws://` nats-server
  done: true
- title: Core protocol + `.cljc` facade + both platform impl records are in place under `nats-cljc.*`
  done: true
- title: '`(nats/subscribe conn subject handler)` returns a `Subscription` synchronously; the handler receives `{:subject :data}` with `:data` EDN-decoded'
  done: true
deps:
- nts-01kstx8jppcc
links:
- nts-01ksx9np0tpm
- nts-01ksxaghgkg0
---

## Description

The thinnest end-to-end path through every layer: open a connection, publish a message, receive it on a subscription — identical `.cljc` code green on JVM (TCP), browser (WebSocket), and Node (WebSocket). `:edn` codec only.

This slice stands up the architecture every later slice extends: the core protocol, the `.cljc` facade (`nats-cljc.core`, aliased `nats`), both platform impl records (the `io.nats:jnats` wrapper on the JVM; `@nats-io/nats-core` `wsconnect` on ClojureScript), and the **portable `.cljc` test harness** that runs one suite on all three targets against a **real `ws://` nats-server** (no mocks).

`connect` returns `Promise<Connection>`; `publish` is fire-and-forget returning `nil`; `subscribe` returns a `Subscription` synchronously and delivers `{:subject :data}` maps to the handler, one call per message. Establishes the baseline `ex-info`-with-canonical-`:type` error shape (full normalization lands in the error-model slice).

ADRs: 0001 (non-blocking core; JVM=>TCP, CLJS=>WebSocket), 0002 (native-promise one-shots + callback handler), 0004 (codec-centric, `:edn` default), 0005 (protocol + platform record behind a `.cljc` facade, `nats-cljc.*`), 0007 (handler delivery baseline).

## Notes

**2026-05-30T20:23:18.382548480Z**

Tracer bullet green on all three platforms against a real nats-server (TCP :4222 / ws :8080). Architecture under nats-cljc.*: protocol (Conn: -publish/-subscribe), codec (:edn only — pr-str + clojure.edn/cljs.reader to platform bytes), .cljc facade core (connect/publish/subscribe owning codec + {:subject :data} shape), and both impl records — impl.jvm (JvmConnection over jnats 2.25.3, connect wraps blocking Nats/connect in a CompletableFuture, subscribe via Dispatcher+MessageHandler) and impl.js (JsConnection over @nats-io/nats-core wsconnect, subscribe via {:callback} returning a Subscription synchronously). connect => native promise; publish => nil; subscribe => Subscription sync; handler gets EDN-decoded :data. One portable core_test.cljc: identical connect/publish/subscribe calls; only the transport URL and sync(deref)-vs-async(cljs.test async + promesa) await glue fork by reader conditional. Same-connection SUB-before-PUB ordering made the round-trip non-flaky without exposing flush (deferred to the connection-lifecycle slice). Results: JVM 2 tests/6 assertions, Node 2/5, browser-headless (karma) 2/2 — all 0 failures; clj-kondo 0/0; jar packages all 5 namespaces. Baseline ex-info :connect-failed error shape established (full normalization is the error-model slice). Note: local karma needs CHROME_BIN=<playwright chrome-headless-shell> (full Chrome-for-Testing trips a crashpad bug in this container); CI Chrome is unaffected, so karma.conf untouched.
