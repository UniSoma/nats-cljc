# Internal structure and namespace layout

To let one `.cljc` surface delegate to two very different native clients, the library is layered: the **public API is a thin portable facade** in `nats-cljc.core` (`.cljc`) that owns codec encode/decode and ergonomics, sitting on top of an **internal protocol** of primitive operations (publish-bytes, subscribe, request, unsubscribe, flush, drain, close, status). A **`Connection` is a platform record** that implements that protocol by wrapping the native client. All platform and native-client code is quarantined in `nats-cljc.impl.jvm` (`.clj`, wraps `jnats`) and `nats-cljc.impl.js` (`.cljs`, wraps `@nats-io/nats-core`); the codec lives in `nats-cljc.codec`. Phase-2 surfaces (JetStream, KV, Object) hang off the same `Connection`.

## Namespace root

The root is **`nats-cljc.*`** (always aliased `nats`), **not** `nats.*`. The entire `nats.*` root — `nats.core`, `nats.stream`, `nats.consumer`, `nats.kv` — is already taken by [cjohansen/clj-nats](https://github.com/cjohansen/clj-nats), the JVM-only sibling that also wraps `jnats`. Matching our artifact name avoids the collision and the `-cljc` suffix signals the cross-platform differentiation.

## Public vs. internal namespaces (amended)

The public surface is exactly **`nats-cljc.core`**, **`nats-cljc.codec`**, **`nats-cljc.jetstream`**, **`nats-cljc.kv`**, **`nats-cljc.service`**, and **`nats-cljc.blocking.core`**, plus the opt-in codec namespaces **`nats-cljc.codec.<name>`** — those are public-by-require (ADR 0011 pins both the convention and the registry-miss hint that derives it), even though their contents are `^:no-doc`. **Every other namespace lives under an `impl` segment, nested per area**: root internals in `nats-cljc.impl.*` (`auth`, `error`, `protocol`, alongside the `jvm`/`js` legs this ADR already quarantined there), JetStream internals in `nats-cljc.jetstream.impl.*` (`acks`, `consumer`, `error`, `pub`, `pull`, `refill`, `stream`, alongside its `jvm`/`js` legs), KV internals in `nats-cljc.kv.impl.*` (`bucket`, `error`, alongside its `jvm`/`js` legs), Service internals in `nats-cljc.service.impl.*` (`config`, alongside its `jvm`/`js` legs). Internal namespaces carry both signals: the `impl` path segment for editor autocomplete, `^:no-doc` for doc generators.

A flat `nats-cljc.impl.*` bucket was considered and rejected: it would churn the already-correct `nats-cljc.jetstream.impl.{jvm,js}` quarantine and re-introduce a name collision between the root and JetStream `error` namespaces. The rename from the original first-level internals was a straight cut with no deprecation shims — every moved namespace was `^:no-doc` and never documented, and shims would defeat the autocomplete goal by keeping the old names completable.

A new internal namespace goes under its area's `impl.*`; a new first-level namespace is a deliberate act of publishing API.

## Considered options

- **Reader-conditional bodies in the public functions** (`#?(:clj … :cljs …)`) instead of a protocol — rejected: the native client leaks into the public namespace, every function carries a platform fork, and it rots once JetStream multiplies the surface.

## Consequences

- The public API can stay stable even if a native client is swapped (reinforcing ADR 0003's "client choice is localized").
- The protocol leaves a clean seam for an in-memory **test connection** later. This is a possibility the design preserves, not a committed goal.
