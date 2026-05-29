# promesa promises for one-shots, callback handlers for subscriptions

The portable non-blocking core (ADR 0001) needs a cross-platform async primitive, and NATS operations come in two distinct shapes. We split them:

- **One-shot operations** (`connect`, `request`, `publish`-with-ack, `flush`, `close`, …) return a **promise**. It is the platform-native promise type — `js/Promise` on ClojureScript, `CompletableFuture` on the JVM — and therefore promesa-compatible on both, and natively `await`-able on ClojureScript (see below).
- **Subscriptions** deliver each message to a **handler** callback, `(fn [msg] …)`, for as long as the subscription is active.

core.async and missionary are **not** the core primitive; they are planned as **opt-in adapters** layered on top later.

## Considered options

- **Uniform core.async channels for both** — rejected as the *core* primitive: `<!!` is JVM-only and `<!` only works inside go-blocks, so a portable consumer pays go-block ceremony everywhere; a channel-of-one for one-shot ops is awkward.
- **Manifold** — rejected: JVM-only, so it cannot serve ClojureScript.
- **A callback for one-shots too** — rejected: loses error propagation and composition that promises give for free.

## Consequences

- The consumer dependency floor is **promesa** (one cross-platform dependency); core.async is never forced on anyone.
- The returned promise composes with ClojureScript 1.12.145's native `^:async`/`await`, so CLJS-only code gets a zero-ceremony native consumption path on the very same object that portable code awaits via promesa macros.
- A bare callback cannot carry backpressure. JetStream **pull** consumers (Phase 2) will therefore be delivered through a channel/missionary adapter rather than a plain handler, because flow control matters there.
