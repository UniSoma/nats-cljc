# Portable non-blocking core with per-platform transports

The goal of nats-cljc is write-once-run-both: the same `.cljc` consumer code compiles and runs unchanged on the JVM, the browser, and Node. A browser can reach NATS only over WebSocket, and ClojureScript cannot block a thread. We therefore make the **core API non-blocking on every platform** — no blocking call leaks into the portable surface — and fix one transport per platform: **JVM ⇒ TCP** (via `jnats`), **all ClojureScript ⇒ WebSocket**. Any NATS server reachable from a ClojureScript platform must have its websocket listener enabled.

## Considered options

- **Expose the JVM's native blocking API and a separate async CLJS API** — rejected: defeats write-once-run-both; the two surfaces would diverge.
- **Block on the JVM, async on CLJS, behind one name** — rejected: not portable, since a `.cljc` consumer can't block on CLJS.

## Consequences

- JVM users do not get blocking-by-default. A thin **blocking convenience layer** may be offered in `.clj`-only namespaces, built on top of the portable core, for callers who want it.
- Every NATS deployment that serves ClojureScript clients must enable WebSocket. This is an operational requirement we document, not something the library can paper over.
- Node-CLJS rides the same WebSocket path as the browser (raw TCP on Node is out of scope for now), keeping the ClojureScript side to a single transport code path.
