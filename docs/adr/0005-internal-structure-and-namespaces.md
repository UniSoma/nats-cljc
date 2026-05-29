# Internal structure and namespace layout

To let one `.cljc` surface delegate to two very different native clients, the library is layered: the **public API is a thin portable facade** in `nats-cljc.core` (`.cljc`) that owns codec encode/decode and ergonomics, sitting on top of an **internal protocol** of primitive operations (publish-bytes, subscribe, request, unsubscribe, flush, drain, close, status). A **`Connection` is a platform record** that implements that protocol by wrapping the native client. All platform and native-client code is quarantined in `nats-cljc.impl.jvm` (`.clj`, wraps `jnats`) and `nats-cljc.impl.js` (`.cljs`, wraps `@nats-io/nats-core`); the codec lives in `nats-cljc.codec`. Phase-2 surfaces (JetStream, KV, Object) hang off the same `Connection`.

## Namespace root

The root is **`nats-cljc.*`** (always aliased `nats`), **not** `nats.*`. The entire `nats.*` root — `nats.core`, `nats.stream`, `nats.consumer`, `nats.kv` — is already taken by [cjohansen/clj-nats](https://github.com/cjohansen/clj-nats), the JVM-only sibling that also wraps `jnats`. Matching our artifact name avoids the collision and the `-cljc` suffix signals the cross-platform differentiation.

## Considered options

- **Reader-conditional bodies in the public functions** (`#?(:clj … :cljs …)`) instead of a protocol — rejected: the native client leaks into the public namespace, every function carries a platform fork, and it rots once JetStream multiplies the surface.

## Consequences

- The public API can stay stable even if a native client is swapped (reinforcing ADR 0003's "client choice is localized").
- The protocol leaves a clean seam for an in-memory **test connection** later. This is a possibility the design preserves, not a committed goal.
