---
id: nts-01kt87wgm75d
title: Publish-during-drain parity across legs
status: closed
type: bug
priority: 2
mode: afk
created: '2026-06-04T02:37:34.215563795Z'
updated: '2026-06-05T16:47:24.054950911Z'
closed: '2026-06-05T16:47:24.054950911Z'
tags:
- review
- drain
acceptance:
- title: clj-kondo clean; suite green on JVM and Node
  done: true
- title: 'A publish issued while drain is in flight does not throw on both legs (prior bug: a raw DrainingConnectionError leaked on JS)'
  done: true
- title: A publish on a closed connection (drain-complete or close()) throws :connection-closed on both legs; publish never yields :drained
  done: true
- title: ADR 0014 records the publish-during-drain contract + cross-ref in ADR 0006
  done: true
- title: 'A publish-during-drain-window test covers both legs: sync publish does not throw; post-drain throws :connection-closed'
  done: true
---

## Description

A publish issued on a draining/closing connection diverges across legs, and on the JS leg can leak a raw native error.

jnats keeps getStatus CONNECTED through the drain window and exposes no draining signal at publish time, so a publish during the in-flight window succeeds and returns nil; once the connection is closed it throws IllegalStateException ("Closed") -> :connection-closed.

nats.js (@nats-io/nats-core 3.3.1) is noisier: a publish lands natively (no throw) in the synchronous tick after drain(), throws DrainingConnectionError in the microtask window after noMorePublishing flips, and ClosedConnectionError once closed. isDraining() latches true forever (even after close), so it is useless as a discriminator. The current JS -publish only catches ClosedConnectionError (-> op-state-error -> :drained when isDraining) and lets DrainingConnectionError fall through :else as a RAW native error with no canonical :type. The request path has the same latent leak. So portable shutdown code that publishes during drain behaves inconsistently across legs and can surface a non-normalized error on JS.

Decision (ADR 0014): converge on jnats' allow/best-effort behavior, keyed on the actual close state (NOT isDraining):
- publish on a draining (not-closed) connection -> nil (best-effort)
- publish on a closed connection (drain-complete or close()) -> :connection-closed
- publish NEVER yields :drained

This matches what the JVM already does -> JS-only fix, full parity, zero JVM production change. request keeps its :drained contract (it has a promise to reject, and jnats throws "Draining" for requests), so a drained-then-closed connection yields publish -> :connection-closed but request -> :drained -- an accepted asymmetry that is identical on both legs.

## Design

Refined via a grilling session. JS-only production change.

## src/nats_cljc/impl/js.cljs

1. -publish catch (cond): replace the ClosedConnectionError -> op-state-error branch with:
     (instance? nats-core/InvalidArgumentError e)  (throw (max-payload-error client subject bytes))
     (instance? nats-core/DrainingConnectionError e) nil            ;; best-effort during drain (match jnats)
     (instance? nats-core/ClosedConnectionError e)
       (throw (ex-info "Connection is closed" {:type :connection-closed :subject subject}))
     :else (throw e)
   nats-core/DrainingConnectionError is a top-level export (verified). publish no longer calls op-state-error.

2. -request catch: add AHEAD of the ClosedConnectionError branch:
     (instance? nats-core/DrainingConnectionError e)
       (ex-info "Connection is draining" {:type :drained :subject subject})
   (twin leak fix. Keep the existing ClosedConnectionError -> op-state-error branch: it still yields :drained
   via isDraining for the closed-after-drain case the existing request-during-drain test pins.)

3. op-state-error is now request-only: update its docstring (drop the "publish/" mention).

## src/nats_cljc/impl/jvm.clj
NO code change. jnats already gives in-window nil and closed -> :connection-closed for publish.
Only update op-state-error's publish-mentioning comment so it no longer implies publish maps via it.

## Untestable branches (state honestly)
Both DrainingConnectionError branches (publish->nil, request->:drained) sit in a sub-ms microtask window
(the gap between noMorePublishing=true and close()) that is NOT deterministically reachable -- the suite
uses real servers, no mocks. They are defensive normalization upholding ADR 0006's "no native error escapes"
invariant, not branches a test exercises.

## test/nats_cljc/core_test.cljc
New deftest publish-during-drain-window-returns-nil, mirroring request-during-drain-window-rejects-drained
(manual connect + gated handler + try/finally on JVM, promise chain on CLJS; NOT with-conn).
- nats/publish ALWAYS returns nil, so assert "does NOT throw", not (nil? ...).
- The in-window publish must be issued SYNCHRONOUSLY right after (drain conn) -- no delay/await between them --
  to land in the nil window on JS (any delay closes the conn -> :connection-closed). A gated handler holds the
  JVM window open (status CONNECTED); JS lands in the native sync-success tick.
Assertions, identical on both legs:
  a) publish sync after drain() -> does NOT throw                         (AC#1, in-window best-effort)
  b) complete gate, await drain, publish on the now-closed conn -> :connection-closed
     (Design-B boundary guard: a regression to isDraining->:drained would make this :drained)
AC#2's close() path stays covered by the existing connection-closed-normalized test (no duplication).

## Verify
clj-kondo --lint src test ; JVM: clojure -X:test ; Node: npx shadow-cljs compile node && node target/node-tests.js

## Docs (already done this session)
ADR docs/adr/0014-publish-during-drain-is-best-effort.md written; one-line cross-ref added to ADR 0006.
CONTEXT.md intentionally unchanged (per-operation delivery contract, not a glossary term).

## Notes

**2026-06-05T15:05:03.964220199Z**

Plan refined + handed off via a grilling session (2026-06-05). Premise corrected: the JS leak is a raw native DrainingConnectionError (no canonical :type), not a thrown :drained; jnats returns nil only in-window and :connection-closed once closed. Chosen contract = Design B (key on actual close state, not isDraining): JS-only fix, full cross-leg parity, zero JVM production change. Full implementation + test plan in the Design section. ADR 0014 + ADR 0006 cross-ref already written; CONTEXT.md intentionally unchanged.

**2026-06-05T16:47:24.054950911Z**

JS-only fix for publish-during-drain parity (Design B, ADR 0014). -publish now swallows a DrainingConnectionError to nil (best-effort, matching jnats) and maps ClosedConnectionError directly to :connection-closed — no longer via op-state-error, so publish never yields :drained. -request gained the twin DrainingConnectionError -> :drained branch (raw-leak fix), keeping its :drained contract via the existing ClosedConnectionError -> op-state-error path. JVM: docstring-only (jnats already gives in-window nil + closed -> :connection-closed). New deftest publish-during-drain-window-returns-nil covers both legs: sync publish after drain() does not throw; post-drain publish -> :connection-closed. clj-kondo clean; JVM 100/217 + Node 76/143 green.
