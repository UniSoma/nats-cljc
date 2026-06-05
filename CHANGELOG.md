# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The normalized vocabularies are the public contract: adding a member is a minor
bump, renaming or removing one is a major bump (see ADR 0009).

## [Unreleased]

## [0.1.1] - 2026-06-05

### Fixed

- **cljdoc API documentation** now builds. cljdoc analyses a library by
  `require`-ing every namespace, and the opt-in `:json` and `:transit` codec
  namespaces pull dependencies (`org.clojure/data.json`, `com.cognitect/transit-*`)
  that are deliberately absent from the published pom (ADR 0004's clean forced
  footprint) — so loading them failed the `0.1.0` doc build. Both namespaces are now
  marked `^:no-doc`, which excludes them from analysis. The codecs are unchanged and
  remain documented via the README, ADR 0011, and `nats-cljc.codec`. No API or
  runtime change.

## [0.1.0] - 2026-06-05

First release: the Phase 1 core and Phase 1.5 blocking layer, tested on the JVM,
Node, and the browser.

### Added

- **Portable core API** (`nats-cljc.core`) — one `.cljc` surface that runs
  unchanged on the JVM, the browser, and Node: `connect`, `publish`, `subscribe`,
  `request`/`reply`, `unsubscribe` (with auto-unsubscribe), `flush`, `drain`,
  `close`, and a `subject` builder. One-shot operations return the platform-native
  promise (`CompletableFuture` on the JVM, `js/Promise` on ClojureScript).
- **Queue groups** — load-balanced subscriptions via `:queue`.
- **Headers** — portable, HTTP-style header normalization on publish and delivery.
- **Codecs** — `:edn` by default with zero extra runtime dependencies; opt-in
  `:json` and `:transit` codecs; custom codecs via `nats-cljc.codec/register!`.
- **Connection lifecycle & status events** — normalized `:on-status` notifications
  with a canonical `:type` set, identical in shape on every platform.
- **Normalized error model** — a canonical error `:type` set surfaced as `ex-info`,
  identical in shape on every platform, plus a separate validation-error category
  for caller misuse.
- **Authentication** — token, user/password, nkey, JWT, and creds.
- **Reconnect** — configurable automatic reconnection.
- **JVM-only blocking convenience layer** (`nats-cljc.blocking.core`) — the same
  verb names, synchronous, with a pull-based subscription model.

[Unreleased]: https://github.com/unisoma/nats-cljc/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/unisoma/nats-cljc/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/unisoma/nats-cljc/releases/tag/v0.1.0
