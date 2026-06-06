# JetStream context: a single async handle, verified at entry

The entry point to JetStream is one portable handle — `(jetstream conn) → Promise<JetStream context>` — that holds **both** the data plane (publish, pull) and the management plane (stream/consumer admin). It is async (a Promise, per ADR 0002), it hangs off the existing `Connection` via a new internal protocol the platform records implement (the surface-on-Connection pattern of ADR 0005), and **obtaining it verifies JetStream is enabled on the server/account** — so the handle is the single place `:jetstream-not-enabled` (err 10039) surfaces, identically on both legs.

Two things here are deliberate deviations a future reader would otherwise question.

## One handle, not the native two

Both native clients hand you *two* objects: jnats has `conn.jetStream()` (data) and `conn.jetStreamManagement()` (admin); nats.js has `jetstream(nc)` (data) and `jetstreamManager(nc)` (admin). nats-cljc collapses them into one **JetStream context** that internally holds both. The common application creates a stream *and* publishes *and* consumes, so it needs both planes anyway; the data/admin split is an implementation detail of the underlying clients, not something the consumer should juggle. The cost we accept: a publish-only consumer still gets the management surface (and pays the entry round-trip below) even though it never calls an admin verb.

## Verify at entry, even though the JVM needn't

The legs disagree on whether getting the handle round-trips. nats.js' `jetstreamManager(nc)` does an INFO round-trip by default and **rejects** if JetStream is not enabled (unless `checkAPI:false`). jnats' `jetStream()`/`jetStreamManagement()` are cheap *local constructions* — they don't talk to the server, so on the JVM `:jetstream-not-enabled` would not surface until the first real operation.

Left to native defaults, the same misconfiguration would reject `(jetstream conn)` on CLJS but the first `publish`/`create-stream` on the JVM — an asymmetry that breaks portable error handling: `(-> (jetstream conn) (.then …) (.catch no-js))` would catch it on one leg only. So nats-cljc **forces a JS-info round-trip on the JVM too** (inside the off-thread wrap), making the handle's promise the one uniform place 10039 is raised. Failing fast at handle creation with a clear `:jetstream-not-enabled` is also a better experience than a confusing failure on the first publish.

## What the verify reinterprets — and what it lets pass

The verify is a `$JS.API.INFO` request, so it can fail the three ways any request can (ADR 0006): no-responders, timeout, or a closed/draining connection. Only the **no-responder** failure means "JetStream is not enabled" — nobody answers on `$JS.API.*` because the server carries no JetStream — and only it is reinterpreted as `:jetstream-not-enabled`. A **timeout** (a healthy JetStream server that didn't answer the INFO in time — load, leader election) stays the core `:timeout`; a **closing/closed** connection stays `:connection-closed` / `:drained`. The handle rejects with whichever applies, identically on both legs. These are not JetStream-specific failures and must not be relabelled as the *permanent* `:jetstream-not-enabled`, or a caller who would retry a transient blip instead gives up for good.

The legs reach that split differently. nats.js separates the modes natively — its INFO request rejects with `JetStreamNotEnabled` for a no-responder and a bare `TimeoutError` / `ClosedConnectionError` / `DrainingConnectionError` otherwise — which we normalize by error name. jnats **cannot**: `getAccountStatistics()`'s blocking request funnels its `TimeoutException` *and* its no-responder `CancellationException` into one indistinguishable `IOException` (`NatsJetStreamImpl.responseRequired(null)` — same class, same message). So the JVM disambiguates with a second, round-trip-free signal — `ServerInfo.isJetStreamAvailable()`, the server's connect-time `jetstream` INFO flag: on that ambiguous `IOException`, server-advertises-JetStream ⇒ the non-answer was transient ⇒ `:timeout`; server-does-not ⇒ `:jetstream-not-enabled`. The closing `IOException` (jnats' `ensureNotClosing`, thrown by the cheap `jetStream()`/`jetStreamManagement()` construction *before* the round-trip) is told apart structurally — by which call threw, not by message text — and keyed on connection status (CLOSED → `:connection-closed`, else → `:drained`), reusing the `op-state-error` precedent. Account-level disablement is unaffected throughout: it returns err_code 10039 as a `JetStreamApiException`, normalized via the shared table (ADR 0020), never reaching the `IOException` arm.

## Considered options

- **Two handles mirroring the native split** (a cheap sync data handle + an async management handle). Rejected for the common case: most apps need both, and forcing the consumer to acquire and pass two handles leaks the underlying clients' structure. It *would* let a publish-only app skip the management round-trip — a real but narrow saving, not worth the everyday friction. The single handle remains internally free to lazily construct each native object.
- **Defer verification** (cheap entry on both legs via `checkAPI:false`; let 10039 surface on the first operation). Symmetric and round-trip-free, but it pushes the "JetStream isn't enabled" failure onto every first data/admin call instead of the handle, where it is least expected and hardest to attribute. Verify-at-entry trades one round-trip for a single, obvious failure site.
- **Native defaults** (CLJS verifies at entry, JVM defers). Rejected outright: it is the only option that makes *where* 10039 surfaces platform-dependent, defeating the portability the library exists for.

## Consequences

- The portable surface gains a new noun, **JetStream context**, and a new internal protocol (ADR 0005 style) that each platform's `Connection` record implements to vend it.
- Acquiring the context costs one server round-trip on *both* legs, including for publish-only consumers. Accepted: it is one-time per handle and buys a uniform, fail-fast `:jetstream-not-enabled`.
- `:jetstream-not-enabled` is contractually raised by the `(jetstream conn)` promise, not by individual operations — portable code catches it in one place.
- The JVM verification round-trip is an implementation obligation (a JS/account-info call in the off-thread wrap); removing it to "save a round-trip" would silently reintroduce the cross-leg asymmetry this decision exists to prevent.
