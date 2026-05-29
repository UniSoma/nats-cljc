# Wrapped clients and supported ClojureScript toolchain

nats-cljc wraps the official platform clients: **`io.nats:jnats`** (2.x) on the JVM and **`@nats-io/nats-core`** on ClojureScript, using its `wsconnect` WebSocket transport, which serves browser and Node from one package. The supported ClojureScript toolchain is **shadow-cljs**, which has first-class npm interop; the npm dependency is declared in the library's `deps.cljs`.

## Scope boundary (the explicit no's)

- **Self-hosted / bootstrapped ClojureScript (e.g. scittle) is out of scope.** Our JS interop assumes a real compiler + bundler.
- **nbb is best-effort only, not a goal.** If it happens to work, fine; we won't constrain the design to keep it working.
- **Node raw-TCP is out of scope.** Node-CLJS uses the same WebSocket path as the browser (see ADR 0001).

## Consequences

- CLJS consumers need a bundler-based build and `@nats-io/nats-core` on `node_modules`.
- The wrapped clients sit behind our facade, so swapping the underlying client later is localized work — this is why the client choice itself is recorded here as a boundary note rather than as a deep architectural commitment.
