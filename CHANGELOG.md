# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The normalized vocabularies are the public contract: adding a member is a minor
bump, renaming or removing one is a major bump (see ADR 0009).

## [Unreleased]

## [0.4.0] - 2026-06-11

Phase 3: KV, tested on the JVM and Node. One new portable namespace,
`nats-cljc.kv`, speaking KV vocabulary throughout — Buckets, Entries,
Revisions — never its stream substrate (ADR 0023). All new vocabulary is
additive ⇒ minor bump (ADR 0009).

### Added

- **KV context** — `(kv conn)` resolves to the handle every Bucket-lifecycle
  operation flows through, verified at entry exactly like the JetStream
  context: a server or account without JetStream rejects with
  `:jetstream-not-enabled` at the handle, not on the first operation
  (ADR 0017).
- **Bucket lifecycle** — `create-bucket`, `open-bucket`, and `delete-bucket`,
  configured by the same portable closed kebab-keyword config maps as streams
  (`:bucket`, `:history`, `:ttl-ms`, `:storage`, `:replicas`, …; an
  unrecognized key is `:unknown-config-key`, an omitted `:bucket` is
  `:missing-required-key`). Opening verifies the Bucket exists, rejecting with
  the new operational `:type :bucket-not-found` — the KV face of
  `:stream-not-found` (ADR 0023). The resolved Bucket handle binds the
  Bucket's one Codec (the connection default, or an open/create-time `:codec`
  override) — a Bucket's values are homogeneous, never per-operation choices.
- **Operator surface** — `bucket-names`, `list-buckets`, and `bucket-status`,
  resolving fully-realized vectors and one normalized status map shape
  (config as applied plus the `:values`/`:bytes` counters), identical on every
  platform.
- **Entry round trip** — `put` and `get` through the Bucket's Codec: `put`
  resolves the new Revision as a bare number; `get` resolves an Entry — a
  plain map `{:bucket :key :value :revision :created :operation}` — or nil
  for an absent key (absence is a normal outcome to branch on, not an Error;
  a stored nil stays distinguishable as `{:value nil …}`). A malformed key is
  the new validation `:type :invalid-key`, pre-flight on every entry
  operation.
- **Compare-and-set** — `create` (write only when absent — first-writer-wins
  locks and initialization) and `update` (write only when the expected
  Revision is still latest). A lost race rejects with the new operational
  `:type :wrong-revision` carrying the contested `:key` — one canonical face
  for both verbs, KV vocabulary rather than the substrate's
  `:wrong-last-sequence` (ADR 0023).
- **Tombstones and history** — `delete` (a Tombstone: the key reads as absent
  while history retains the deletion, observable as an Entry with a delete
  `:operation`), `purge` (erase one key's history down to a purge marker),
  both with an optional `:revision` guard rejecting stale guards as
  `:wrong-revision`; `purge-deletes` (the Bucket-wide janitor: every
  Tombstoned key's retained history removed, marker included, immediately and
  deterministically on both legs); and `history` — the retained Entries of a
  key oldest-to-newest, markers visible, each Entry carrying `:delta`.
- **History archaeology** — `get` takes an optional `:revision` pinning the
  read to an exact past Revision (markers stay visible, never hidden), and
  `keys` enumerates a Bucket's live keys, optionally restricted by a
  subject-style filter.
- **Watch** — `watch` pushes each matching Entry to a handler under the core
  subscription contract (serial delivery; a returned promise suspends the
  next delivery — ADR 0007), with a closed `:deliver` mode (`:latest` default
  / `:history` / `:updates`; anything else is the new validation
  `:type :invalid-deliver`), `:keys` pattern filtering (one subject-style
  pattern or a vector — their union), `:ignore-deletes?` to suppress marker
  deliveries, and a per-watch `:on-error` sink with the core-subscription
  override semantics. The watch handle carries an `:initialized` promise —
  the "cache is warm" signal — and `stop` ends it, idempotently.
- **ClojureScript packaging** — `@nats-io/kv` is installed automatically and
  version-pinned in lockstep with the core client, exactly like
  `@nats-io/jetstream`; the KV import is confined to the KV impl namespace,
  so a bundle that never requires `nats-cljc.kv` ships zero KV bytes
  (ADR 0016).
- **JetStream direct get** — `nats-cljc.jetstream/get-message`: a one-shot,
  promise-returning read of a stored message straight off a Stream, by stream
  sequence (`{:seq n}`) or newest-on-subject (`{:last-by-subject subj}`),
  resolving a pure-data `{:subject :data :seq :timestamp}` (plus `:headers`
  when present). Rejects with the new operational `:type :no-message-found`
  (err 10037) when nothing matches; pre-flight, a malformed query rejects with
  the new validation `:type :invalid-query`. Vocabulary additions ⇒ minor bump
  (ADR 0009).

## [0.3.0] - 2026-06-10

Internal-namespace housekeeping: the API/implementation boundary is now visible
in the namespace names themselves. Breaking only for code that required
never-public (`^:no-doc`) internals.

### Changed

- **Internal namespaces moved under `impl`** — never-public (`^:no-doc`)
  namespaces now live under their area's `impl` segment so editor autocomplete
  distinguishes API from internals: `nats-cljc.{auth,error,protocol}` →
  `nats-cljc.impl.{auth,error,protocol}`, and
  `nats-cljc.jetstream.{acks,consumer,error,pub,pull,refill,stream}` →
  `nats-cljc.jetstream.impl.{…}`. No deprecation shims (ADR 0005, amended).
  The public surface — `nats-cljc.core`, `nats-cljc.codec`,
  `nats-cljc.jetstream`, `nats-cljc.blocking.core`, and the opt-in
  `nats-cljc.codec.<name>` namespaces — is unchanged.
- **Cross-facade plumbing moved out of `nats-cljc.core`** — `normalize-headers`,
  `trim-headers`, and `effective-codec` were public in `nats-cljc.core` only so
  the JetStream facade could reuse them; they now live in the new
  `nats-cljc.impl.msg`, so the public namespaces expose only API. This also
  removes the JetStream facade's code dependency on `nats-cljc.core`.

## [0.2.0] - 2026-06-10

Phase 2: JetStream, tested on the JVM and Node. One new portable namespace,
`nats-cljc.jetstream`, mirroring the shape of `nats-cljc.core`.

### Added

- **JetStream context** — `(jetstream conn)` returns one handle for the data
  and management planes, verified at entry: a server round-trip on both
  platforms so a server or account without JetStream fails fast with
  `:jetstream-not-enabled` at the handle, not on the first operation (ADR 0017).
- **Stream management** — `create-stream`, `update-stream`, `delete-stream`,
  `purge-stream`, `stream-info`, `list-streams`, and `stream-names`, configured
  by
  portable closed kebab-keyword maps: keyword enums (`:storage`, `:retention`),
  durations as integer milliseconds with the unit in the key (`:max-age-ms`),
  and a validation error (`:unknown-config-key`) for any unrecognized key
  instead of silent passthrough. Info maps come back normalized the same way,
  with ISO-8601 timestamps on every platform.
- **Consumer management** — `create-consumer`, `update-consumer`,
  `delete-consumer`, `consumer-info`, `list-consumers`, and `consumer-names`,
  with
  the same closed config-map contract (`:ack-policy`, `:deliver-policy`,
  `:ack-wait-ms`, `:filter-subjects`, …).
- **Acked publish** — `publish` against the JetStream context resolves to a
  PubAck `{:stream :seq :duplicate :domain}`. Supports `:msg-id` server-side
  dedup, `:expect` optimistic concurrency (a mismatch surfaces as
  `:wrong-last-sequence`), `:timeout-ms`, and per-publish `:codec`. A reserved
  `Nats-*` key in user `:headers` is rejected pre-flight as `:reserved-header`.
- **Pull delivery** — three verbs: `next` (promise of one message or nil),
  `fetch` (promise of a bounded vector), and `consume` (continuous delivery
  through the same promise-return handler contract as core subscriptions — the
  runtime waits for the handler's returned promise before delivering more, so
  a slow handler simply slows the pull; no `:slow-consumer`, no async-library
  dependency). `consume` returns a drainable, unsubscribable handle (ADR 0018).
- **Acknowledgements** — `ack`, `nak` (with optional `:delay-ms`), `term`,
  and `working` are synchronous, idempotent, and
  never throw; `double-ack` returns a promise of the server's confirmation for
  exactly-once-adjacent processing (ADR 0019).
- **Pure-data messages** — delivered JetStream messages are plain maps
  `{:subject :data :headers :js {…}}`; `:js` carries normalized delivery
  metadata (`:stream`, `:consumer`, `:stream-seq`, `:delivery-seq`,
  `:delivered`, `:pending`, `:redelivered`, `:timestamp`, `:domain`).
- **Ordered consumer** — `ordered-consumer` replays a Stream gap-free with no
  acks, for simple single-reader stream reads.
- **Normalized JetStream errors** — new operational `:type`s identical on
  every platform: `:jetstream-not-enabled`, `:stream-not-found`,
  `:consumer-not-found`, `:wrong-last-sequence`, and the catch-all
  `:jetstream-api-error` carrying the server's `{:code :description}`.
  Consume-time side-band conditions (`:heartbeats-missed`,
  `:consumer-deleted`, `:exceeded-limits`) route to a per-consume `:on-error`.
  New validation `:type`s for caller misuse: `:invalid-name`,
  `:unknown-config-key`, `:reserved-header`, `:no-ack-subject` (ADR 0020).
- **ClojureScript packaging** — `@nats-io/jetstream` is installed
  automatically and version-pinned in lockstep with the core client, so the
  duplicate-nats-core hazard cannot arise. The JetStream import is confined to
  JetStream namespaces: a core-only browser bundle ships zero JetStream bytes,
  enforced by a CI guard (ADR 0016).

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

[Unreleased]: https://github.com/unisoma/nats-cljc/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/unisoma/nats-cljc/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/unisoma/nats-cljc/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/unisoma/nats-cljc/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/unisoma/nats-cljc/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/unisoma/nats-cljc/releases/tag/v0.1.0
