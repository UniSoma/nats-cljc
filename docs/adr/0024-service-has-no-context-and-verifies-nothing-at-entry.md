# Service has no context and verifies nothing at entry

The services surface (`nats-cljc.service`) deliberately breaks the shape KV and JetStream established. There is **no "service context"** handle, and creating a Service **verifies nothing** against the server. Two independent entry points hang off the `Connection`: `(service/create conn config) → Promise<Service>` (the server side — you host a Service) and the three Discovery functions `(service/ping|info|stats conn opts?) → Promise<vector>` (the client side — you query Services others host). A future reader who internalized ADR 0017 will expect a `(service conn)` context and look for the entry-verification that isn't there; this records why it is absent.

## Why no context

ADR 0017 gave the JetStream context two jobs, and KV's context inherited both: it **groups management operations**, and **obtaining it verifies a server feature is enabled** — the single place `:jetstream-not-enabled` / a missing Bucket surfaces uniformly across legs. That second job is the headline rationale of 0017.

Services has **neither driver**:

- **Nothing to verify.** Services is a pure convenience over core request-reply — a queue-subscribed handler plus auto-responders on `$SRV.PING|INFO|STATS.*`. There is no server feature, no new wire protocol, no account flag. A server either speaks core NATS (it does — you are connected) or it does not. There is no round-trip that could fail the way `$JS.API.INFO` fails for a JetStream-less server, so the entire reason 0017 forces an entry round-trip is absent. Inventing one would verify nothing and cost a round-trip for it.
- **The natives don't unify around a manager.** nats.js *appears* to, with `new Svcm(nc)` vending both `.add(config)` and `.client()` — but that is a thin local factory, not a verified handle. jnats has **no manager at all**: `Service.builder().connection(conn)…build()` for hosting, and a wholly independent `new Discovery(conn)` for querying. The data/admin split that justified collapsing two JetStream handles into one context simply does not exist here.

So a context would be cargo-culting the KV/JetStream silhouette onto a feature whose defining rationale (0017's verify-at-entry) does not apply — paying for a noun that groups nothing and, worse, implying a verification that never happens.

## What follows from the no-context shape

Several downstream choices are consequences of this decision and of existing ADRs, recorded here rather than in ADRs of their own:

- **Discovery is three stateless functions on `conn`, returning a Promise of a vector** (a bounded `$SRV.*` fan-out gathered until `:max-results`/`:timeout-ms`, narrowed by `:name`/`:id`), not a handle. There is no separate local introspection of a Service you host: asking your own Service is the same wire request, narrowed by its own `:name`/`:id`. One mental model — every info/stats/ping is a request.
- **Endpoints are declared as data in the create config** (`:endpoints [{:name :subject :handler …}]`, `:subject` defaulting to `:name`), the config-as-data idiom of `create-stream`/`create-consumer`. Declare-then-create is the intersection both natives support (jnats builds endpoints at build time; nats.js adds them post-`add`), so the portable surface exposes only it. The native `Group` subject-prefix namespace is **not** surfaced — it puts nothing distinct on the wire (INFO/STATS report fully-resolved subjects regardless), so a consumer composes a grouped subject directly.
- **The handler is an ADR-0007 push Handler** (serial per endpoint, may return a promise for backpressure, must not block), and **replies go through explicit verbs that thread `conn`** — `(service/respond conn msg data)` / `(service/respond-error conn msg code description data?)` — consistent with `core/reply` and the JetStream acks, and routing through the native message so per-endpoint stats stay correct. A thrown or rejected handler auto-replies a service error (code 500) and is counted natively: a service's "sink" is the error reply, not `:on-error`.
- **`(service/stop svc)` drains** (ADR 0002 single async stop); the handle carries a `:stopped` promise resolving to nil, paralleling the Watch handle's `:initialized`.

## Considered options

- **A `(service conn)` context for symmetry with KV/JetStream.** Rejected: it would group nothing the natives keep together and would imply an entry verification that has nothing to verify. Symmetry for its own sake is the wrong master; the context exists in KV/JetStream because 0017's rationale held there, and it does not hold here.
- **A Discovery handle** (mirroring nats.js' `ServiceClient` / jnats' `Discovery`, caching prefix and fan-out limits). Rejected: discovery is stateless — just `conn` plus per-query options — so a cached handle buys nothing over re-passing `opts`, and it would reintroduce a noun for a query.
- **Local introspection on the Service handle** (`getStatsResponse()`-style accessors). Rejected as redundant: a Service answers `$SRV.*` for itself, so self-inspection is already expressible through Discovery narrowed by `:name`/`:id`. One way to read info/stats, not two.

## Consequences

- The portable surface gains the nouns **Service**, **Endpoint**, and **Discovery** (see CONTEXT.md) but **not** a context — the first family feature without one, precisely because it is the first without a server feature to verify.
- `nats-cljc.service` spans both roles: `create`/`stop` (hosting) and `ping`/`info`/`stats` (querying) both take a `Connection`. The role of a given call is read from the verb, not from a distinct handle type; the Discovery glossary entry carries that disambiguation.
- Whether a service handler's returned promise actually serializes the next request on the JS leg (nats.js callback `await` behavior) is a verification obligation, not an assumption; if it does not, the realization drives the endpoint's async iterator instead, or the contract is narrowed for services and documented — the contract follows verified behavior.
