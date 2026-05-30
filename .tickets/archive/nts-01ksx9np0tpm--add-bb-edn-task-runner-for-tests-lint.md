---
id: nts-01ksx9np0tpm
title: Add bb.edn task runner for tests + lint
status: closed
type: task
priority: 3
mode: afk
created: '2026-05-30T20:37:08.761376798Z'
updated: '2026-05-30T22:59:25.429115258Z'
closed: '2026-05-30T22:59:25.429115258Z'
tags:
- tooling
- dx
acceptance:
- title: bb.edn defines test:jvm, test:node, test:browser, lint, and test (= jvm + node, the local legs); `bb tasks` lists them
  done: true
- title: The local legs (test:jvm, test:node, test, lint) pass locally with a ws-enabled nats-server running
  done: true
- title: test:browser is CI-facing — excluded from `test`, not expected in the standard dev container (browser is CI-only, ADR 0010), and does not hardcode CHROME_BIN (honors $CHROME_BIN, else karma default)
  done: true
links:
- nts-01kstx8ysgv5
---

## Description

Add a thin bb.edn **task runner** wrapping the existing test/lint commands. bb only orchestrates — JVM tests (`clojure -X:test`) and shadow-cljs still run on the real JVM; the tasks shell out.

Why: the Node flow is a 2-step compile+run with a non-obvious prerequisite (a ws-enabled nats-server). A task runner collapses these into discoverable verbs (`bb tasks`) and is the natural single source of truth if CI later calls the same tasks (see nts-01ksxaghgkg0).

Local dev runs JVM + Node only; the browser leg is CI-only (ADR 0010). So `test` (the everyday verb) is JVM + Node, and `test:browser` exists as a CI-facing / opt-in wrapper — handy for the CI job and for a dev who brings their own Chrome — but is not part of `test` and is not expected to run in the standard dev container.

Proposed tasks:
- `test:jvm`     -> `clojure -X:test`
- `test:node`    -> `npx shadow-cljs compile node` then `node target/node-tests.js`
- `test`         -> test:jvm + test:node (`:depends`) — the local legs
- `test:browser` -> `npx shadow-cljs compile browser` then `npx karma start --single-run` (CI-facing/opt-in; needs Chrome)
- `lint`         -> `clj-kondo --lint src test`
- optional `nats:up` -> start `nats-server -c ci/nats.conf` if :4222/:8080 are not already listening

Constraints / decisions:
- **Do not hardcode `CHROME_BIN`.** `test:browser` respects an existing `$CHROME_BIN`; otherwise defers to karma default Chrome lookup (the CI / normal-machine path). The container-specific Playwright headless-shell path stays a documented local override, never a committed constant.
- The local test tasks need the ws nats-server up — decide whether to auto-start via `nats:up`/a guard, or assume it is already running.

Out of scope (separate follow-up): rewiring `.github/workflows/ci.yml` to call these tasks — a deliberate single-source-of-truth change weighed alongside the CI-Chrome work in nts-01ksxaghgkg0.

Context: surfaced while closing the tracer-bullet slice (nts-01kstx8ysgv5). Browser-tests-in-CI-only is recorded in ADR 0010.

## Notes

**2026-05-30T22:38:30.775638112Z**

Added bb.edn task runner. Tasks: test:jvm (clojure -X:test), test:node (shadow-cljs compile node + node run), test:browser (shadow-cljs compile browser + karma --single-run; CI-facing, honors $CHROME_BIN, no hardcoded path), test (:depends [test:jvm test:node] — local legs only), lint (clj-kondo --lint src test). Each shells out; bb only orchestrates. Verified locally with all ws servers up: bb test green (JVM 8 assertions, Node 7), bb lint clean, bb tasks lists all. CI rewiring to call these tasks stays out of scope per the ticket.

Extra (requested on top of the ticket): a pre_start task that backs a single 'bb pre_start' in .aishell/config.yaml, replacing the inline 4-process string. It's idempotent — port-guards 7888/4222/4223/4224 and only starts what's down — and daemonizes via babashka.process (no shutdown hook, so children survive bb exit; verified by killing the token server, restarting it via bb pre_start, and confirming it outlived the bb process). config.yaml now reads: pre_start: "bb pre_start".

**2026-05-30T22:59:25.429115258Z**

bb.edn task runner shipped: test:jvm, test:node, test:browser, lint, and test (= jvm + node). Verified locally with ws servers up — bb test green (JVM 8 / Node 7 assertions), bb lint clean, bb tasks lists all. test:browser stays CI-facing: excluded from test, honors $CHROME_BIN, no hardcoded path. Extra (requested on top): an idempotent pre_start task (port-guards 7888/4222/4223/4224, daemonizes via babashka.process — detachment verified by restarting a killed server) now backs 'bb pre_start' in .aishell/config.yaml. A :silence-unsafe deps alias quiets the JDK23+ sun.misc.Unsafe deprecation warning on the local cljs compile, kept out of :cljs so CI's JDK21 npx runs are unaffected.
