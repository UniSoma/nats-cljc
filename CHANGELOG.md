# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The normalized vocabularies are the public contract: adding a member is a minor
bump, renaming or removing one is a major bump (see ADR 0009).

## [Unreleased]

## [0.5.0] - 2026-06-11

Phase 4: services, tested on the JVM and Node. One new portable namespace,
`nats-cljc.service`, for hosting discoverable, instrumented request-reply
Services and discovering them — pure convenience over core request-reply, so
there is NO service context and nothing is verified at entry (ADR 0024). All
new vocabulary is additive ⇒ minor bump (ADR 0009).

### Added

- **Service hosting** — `(create conn config)` resolves a platform-native
  promise to a running Service: a queue-subscribed handler per endpoint plus
  the framework's auto-responders on `$SRV.PING|INFO|STATS.*`. There is no
  service context and no server feature to round-trip against, so `create`
  hangs directly off the Connection and resolves as soon as the endpoints are
  subscribed (ADR 0024). The config is portable, closed data: `:name` and
  `:version` (both required), `:description`, `:metadata`, and `:endpoints` —
  a vector of endpoint maps `{:name :subject :handler :queue-group :metadata}`
  where `:subject` defaults to `:name`. Each `:handler` is an ordinary ADR-0007
  push handler invoked per request with `{:subject :data :reply}`, a returned
  promise applying per-endpoint backpressure (Node serializes per the same
  contract). There is no Group noun — a consumer composes a grouped subject
  directly (ADR 0024). A client invokes an endpoint with plain
  `nats-cljc.core/request`; there is no new caller verb.
- **Create-config validation** — pre-flight on `create`, before any native or
  wire call (ADR 0015): an omitted `:name`/`:version` rejects the promise with
  `:missing-required-key` carrying the offending `:key`, a malformed service or
  endpoint `:name` with `:invalid-name`, a non-semver `:version` with the new
  validation `:type :invalid-version` carrying the offending `:version`, and two
  endpoints sharing a `:name` with the new validation `:type
  :duplicate-endpoint` carrying the offending `:name` (the per-endpoint stats
  key must be unique). Empty or absent `:endpoints` is legal; endpoint
  `:subject` syntax stays native/server-enforced.
- **Replying** — `(respond conn msg data)` answers a request through its native
  service message so the owning endpoint's native stats stay correct (ADR 0024),
  the service analog of `core/reply`. `(respond-error conn msg code description
  data?)` replies with a first-class service error (ADR 0025): a SUCCESSFUL
  reply carrying an integer `code` and string `description` in the
  `Nats-Service-Error` / `Nats-Service-Error-Code` headers, not a transport
  failure — the caller's `core/request` resolves normally. It is terminal like a
  thrown handler (it ends the handler so the native framework counts the
  endpoint error), and a handler that throws or returns a rejected promise
  auto-replies the same shape with code 500.
- **Reading errors** — `(error msg)` reads the service error a reply carries
  (ADR 0025): `nil` on a normal success, or `{:code <int> :description
  <string>}` when the Service answered with an error. The opt-in reader of the
  error headers `core/request` leaves intact — a service error is data the
  caller branches on, never a thrown transport failure.
- **One codec per Service** — bound at `create` (the connection default unless a
  `:codec` registry keyword or `ICodec` instance overrides it there), used to
  decode every request and encode every reply across all endpoints; a single
  `respond` / `respond-error` may override it per call (ADR 0011). An
  unresolvable `:codec` rejects the create promise pre-flight.
- **Lifecycle** — `(stop svc)` resolves a native promise once the Service has
  stopped, DRAINING in-flight requests (a request being handled runs to
  completion and still receives its reply); after it settles a fresh request
  rejects with the canonical `:no-responders` (ADR 0006). Idempotent. The
  resolved Service handle carries a `:stopped` key — a native promise that
  resolves to nil once the Service stops for any reason, the react-to-shutdown
  signal paralleling the Watch handle's `:initialized` (ADR 0024).
- **Discovery** — `ping`, `info`, and `stats`, the client side of the surface
  hanging directly off the Connection (ADR 0024), each a bounded fan-out
  resolving a native promise of a VECTOR of normalized kebab-case EDN maps:
  `ping` the identity `{:name :id :version}` (plus `:metadata`), `info` adds
  `:description` and `:endpoints` (`{:name :subject}` plus `:queue-group` /
  `:metadata`), `stats` adds `:started` and per-endpoint counters
  (`:num-requests`, `:num-errors`, `:processing-time-ns`,
  `:average-processing-time-ns`, durations in integer nanoseconds). `opts`
  narrows by `:name` / `:id` and BOUNDS the fan-out with `:max-results` /
  `:timeout-ms`. There is no Discovery handle and no local introspection — a
  Service inspects itself with the same wire request, narrowed by its
  `:name`/`:id`.
- **ClojureScript packaging** — `@nats-io/services` joins the unconditional
  nats-io family (ADR 0026), declared in `deps.cljs` and pinned in lockstep
  with the core client. The services import is confined to the service impl
  namespace, so a bundle that never requires `nats-cljc.service` ships zero
  services bytes, enforced by the `:services` bundle/externs CI guards
  (ADR 0016).

### Changed

- **nats-io floor raised to 3.4.0** — `@nats-io/services@3.4.0` peer-requires
  `@nats-io/nats-core@3.4.0`, so `@nats-io/nats-core`, `@nats-io/jetstream`,
  and `@nats-io/kv` all move from `3.3.1` to `3.4.0` in lockstep (ADR 0026).
  The bump is a tested gate, not an assumption: the full suite passes on both
  local legs (JVM + Node) against the `3.4.0` trio. JVM consumers see no change
  — `io.nats:jnats` carries `io.nats.service` in-jar.

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
  pattern or a non-empty vector — their union; an empty vector is the new
  validation `:type :invalid-keys`), `:ignore-deletes?` to suppress marker
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

[Unreleased]: https://github.com/unisoma/nats-cljc/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/unisoma/nats-cljc/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/unisoma/nats-cljc/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/unisoma/nats-cljc/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/unisoma/nats-cljc/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/unisoma/nats-cljc/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/unisoma/nats-cljc/releases/tag/v0.1.0
