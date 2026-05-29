---
id: nts-01kstx8jppcc
title: Project scaffold + green CI skeleton
status: open
type: chore
priority: 0
mode: afk
created: '2026-05-29T22:21:47.606541012Z'
updated: '2026-05-29T22:21:47.606541012Z'
tags:
- needs-triage
acceptance:
- title: 'JVM build: `deps.edn` resolves `io.nats:jnats` 2.x + promesa and the `tools.build` script produces a jar'
  done: false
- title: 'CLJS build: `shadow-cljs` compiles the `:browser` and `:node` test targets'
  done: false
- title: '`deps.cljs` declares the npm dependency `@nats-io/nats-core`'
  done: false
- title: CI matrix runs JVM + Node + browser-headless jobs, each starting a `nats-server` with the websocket listener enabled (`ws://`)
  done: false
- title: CI is green on a trivial build across all three jobs
  done: false
- title: '`LICENSE` is Apache-2.0 and the build coordinate is `io.github.UniSoma/nats-cljc`'
  done: false
---

## Description

Stand up the build, dependency, and CI foundation so an empty library compiles and tests green on all three platforms — the substrate every later slice builds on. No NATS behavior yet.

Deliver: `deps.edn` (JVM deps incl. `io.nats:jnats` 2.x + promesa), `shadow-cljs.edn` (CLJS build + test targets), `deps.cljs` declaring the npm dep `@nats-io/nats-core`, a `tools.build` build script, a GitHub Actions CI matrix running JVM / Node / browser-headless against a downloaded `nats-server` binary with the **websocket listener enabled** (`ws://`), and an Apache-2.0 `LICENSE`.

The Clojars coordinate `io.github.UniSoma/nats-cljc` goes in the build config as-is. The `io.github.<owner>` segment is case-sensitive and permanent once published — verify it matches the GitHub org login exactly before the first publish (not a blocker for scaffolding).

ADRs: 0001 (transports), 0003 (toolchain — jnats + @nats-io/nats-core via shadow-cljs), 0009 (Apache-2.0, coordinate, semver contract).
