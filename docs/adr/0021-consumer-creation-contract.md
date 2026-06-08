# JetStream consumer creation: create-only, durable-by-default with an explicit ephemeral flag

`create-consumer` is **create-only** on both legs (jnats `.createConsumer`, nats.js
`consumers.add` with `action=create`), not an upsert. A config-changing re-create rejects
identically on both legs; configuration *updates* are a separate, deliberate future
`update-consumer` verb (`nts-01ktmkk6hxwc`), mirroring how Streams keep create-only
`create-stream` and defer mutation to `update-stream`.

Durability is an **explicit** `:durable?` flag (default `true`), never inferred from `:name`
presence:

- `{:name "D"}` → durable, `:name` required
- `{:durable? false}` → ephemeral, server-assigned name
- `{:name "E" :durable? false}` → named ephemeral

The discriminant is the native `durable_name` field's *absence* — verified symmetric across
clients: jnats emits no `durable_name` when only `.name` is set, and nats.js treats a
`name`-without-`durable_name` config as ephemeral. `:name` remains the single name key on both
the write and read sides; `consumer-info->map` returns a derived `:durable?` (jnats
`getDurable` non-nil / nats.js `durable_name` present) so the curated map round-trips.

## Considered options

- **Upsert (create-or-update) as the default**, which most newer clients surface as their
  convenience method (nats.js `createOrUpdateConsumer`, jnats `addOrUpdateConsumer`, Rust
  `create_consumer`, .NET `CreateOrUpdateConsumerAsync`). Rejected: silently mutating a live
  consumer's config is exactly the cross-client clobbering ADR-37 (JetStream Simplification)
  warns against, and it breaks symmetry with the create-only Stream sibling. The JVM leg
  currently using `.addOrUpdateConsumer` is the divergence this decision fixes.
- **Infer durability from `:name` presence** (present ⇒ durable, absent ⇒ ephemeral). Rejected:
  a forgotten durable name would silently create an auto-expiring ephemeral — a nasty, silent
  failure — and it forecloses named ephemerals. The explicit flag also lets a missing durable
  `:name` fail as a clear "missing required key" rather than `:invalid-name {:name nil}`.
- **Separate `:durable-name` and `:name` keys** mirroring the two native fields. Rejected: the
  read path reads both kinds back as `:name` (via `getName`), so split write keys would create a
  write/read vocabulary mismatch.
