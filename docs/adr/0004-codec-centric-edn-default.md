# Codec-centric payload with an EDN default; Transit/JSON opt-in

NATS carries bytes, and raw bytes are not a uniform type across platforms (`byte[]` on the JVM, `Uint8Array` on ClojureScript). So the API is **codec-centric**: a pluggable **codec** converts between Clojure values and wire bytes. A **default codec lives on the connection and is overridable per `publish` / `subscribe` / `request`**.

Codecs split into two tiers by what they cost the consumer:

- **Built-in, dependency-free** — `:bytes` (passthrough), `:string` (UTF-8), `:edn`. Always available, implemented with only what ships in Clojure/ClojureScript core (`pr-str` + `clojure.edn` / `cljs.reader`). The **default is `:edn`**.
- **Opt-in, third-party** — `:transit` (needs `com.cognitect/transit-clj` on the JVM, `transit-cljs` on ClojureScript) and `:json` (the JVM has no built-in JSON, so it needs a JSON library; ClojureScript uses ambient `js/JSON`). Their impls live in their own namespaces (`nats-cljc.codec.transit`, `nats-cljc.codec.json`) that a consumer `require`s after adding the dependency. The keyword resolves only once that namespace is loaded; using it unloaded yields an actionable error.

This keeps the library's **forced dependency footprint to just the native NATS clients** (ADR 0002 does the same on the async side). A consumer who wants Transit's compactness or a polyglot JSON wire opts in and pays for it; everyone else carries nothing.

## Why EDN as the default

The common case — `(publish conn "subj" {:any :clojure-data})` round-tripping end to end — should need no ceremony *and* no dependency. EDN is the only structured option that is both: it round-trips ordinary Clojure data and keeps the delivered `:data` a **portable Clojure value** on all three platforms, using nothing outside core. Decode with `clojure.edn/read-string` (not `core/read-string`) so it never evaluates.

## Considered options

- **Transit default** (the original choice) — rejected as the *default*: Transit is the best wire format on technical merit (compact, fast, rich types, JSON-shaped for polyglot), but on the JVM it drags a heavy transitive tree (Jackson, msgpack, javassist, jaxb) onto every consumer, including polyglot ones who never use it. A heavy *forced* default contradicts "bring only NATS deps." It stays available as the recommended opt-in.
- **`:bytes` default** — the purest "honest transport" stance, but `:data` would then be the platform-native byte type (`byte[]` vs `Uint8Array`), so the no-override experience stops being write-once — the opposite of the library's reason for being. `:bytes` is the right *explicit* codec, not the default.
- **`:string` default** — portable and dependency-free, but discards structure; rejected in favour of `:edn`, which keeps the data-in/data-out feel at the same dependency cost (none).

## Consequences

- The default forces no third-party dependency; only `:transit` / `:json` users add one.
- EDN is more verbose and slower on the wire than Transit, and is Clojure-only. A subject shared with a non-Clojure service should override to opt-in `:json`, or to `:string` / `:bytes`. The EDN envelope on a shared subject is the same foot-gun the Transit default had, and is the documented reason overrides exist.
- Opt-in codecs need a **codec registry** (already required for custom codecs) plus a clear "unknown / unloaded codec" error.
- Portable consumers still never touch platform byte types unless they explicitly select `:bytes`.
