# Native promises for one-shots, callback handlers for subscriptions

The portable non-blocking core (ADR 0001) needs a cross-platform async primitive, and NATS operations come in two distinct shapes. We split them:

- **One-shot operations** (`connect`, `request`, `publish`-with-ack, `flush`, `close`, …) return a **promise**. It is the platform-native promise type — `js/Promise` on ClojureScript, `CompletableFuture` on the JVM — natively `await`-able on ClojureScript (see below), `deref`-able on the JVM, and composable across both platforms with promesa (which operates *on* these native types).
- **Subscriptions** deliver each message to a **handler** callback, `(fn [msg] …)`, for as long as the subscription is active.

**The library forces no async dependency on consumers.** Because the return type is the *native* promise — not a promesa type — the library never needs promesa to build or compose it: every internal transform (decode a `request` reply, normalize an error) is native interop quarantined in the per-platform `impl.*` namespaces (ADR 0005). promesa is therefore a **recommended, not required**, way to consume the API portably, declared only in the library's test/dev aliases (it drives the portable `.cljc` round-trip suite). A consumer who wants one-source-for-both-platforms composition adds `funcool/promesa` themselves; a CLJS-only consumer can use native `await`, a JVM-only consumer `deref`, and core.async/missionary remain **opt-in adapters** layered on top later. None of these is imposed by depending on nats-cljc.

## Considered options

- **Uniform core.async channels for both** — rejected as the *core* primitive: `<!!` is JVM-only and `<!` only works inside go-blocks, so a portable consumer pays go-block ceremony everywhere; a channel-of-one for one-shot ops is awkward.
- **Manifold** — rejected: JVM-only, so it cannot serve ClojureScript.
- **A callback for one-shots too** — rejected: loses error propagation and composition that promises give for free.

## Consequences

- The consumer dependency floor is **just the native NATS client** (`jnats` on the JVM, `@nats-io/nats-core` on CLJS). No async library — not promesa, not core.async — is forced on anyone; promesa is the documented *recommendation* for portable composition, brought in by the consumer when wanted.
- Owning a recommendation we don't ship means it must stay documented and version-tested: the README shows the promesa path alongside the native `await`/`deref` fallbacks, and the test aliases pin the promesa version the suite runs against.
- The returned promise composes with ClojureScript 1.12.145's native `^:async`/`await`, so CLJS-only code gets a zero-ceremony native consumption path on the very same object that portable code awaits via promesa macros.
- A bare *fire-and-forget* callback cannot carry backpressure, but a **promise-returning** handler can (ADR 0007). JetStream **pull** consumers (Phase 2) are therefore delivered through that same handler contract — not a channel/missionary adapter — with core.async/missionary remaining opt-in adapters layered on top (Phase 5). *(Amended: this consequence originally specified a channel/missionary adapter for pull, before ADR 0007's promise-return backpressure made a plain handler sufficient — see ADR 0018.)*
