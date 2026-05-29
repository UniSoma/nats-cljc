# Codec-centric payload with a Transit default

NATS carries bytes, and raw bytes are not a uniform type across platforms (`byte[]` on the JVM, `Uint8Array` on ClojureScript). So the API is **codec-centric**: a pluggable **codec** converts between Clojure values and wire bytes. A **default codec lives on the connection and is overridable per `publish` / `subscribe` / `request`**. Built-in codecs: `:bytes` (passthrough), `:string` (UTF-8), `:json`, `:edn`, `:transit`. The **default is `:transit`** (JSON flavor).

## Why Transit by default

The library's reason for being is unified Clojure across JVM and ClojureScript. Transit is the canonical portable Clojure serialization, fast, handles rich types, and is JSON-shaped so polyglot consumers can still parse it. The common case — `(publish conn "subj" {:any :clojure-data})` round-tripping end to end — should need no ceremony.

## Considered options

- **No default / `:string` / `:json` default** — the "honest transport" stance, safest for subjects shared with non-Clojure services. Rejected as the default because it makes the primary CLJ↔CLJS case verbose, but it is exactly the override we recommend for polyglot subjects.

## Consequences

- Portable consumers never touch platform byte types unless they explicitly select `:bytes`.
- A subject shared with non-Clojure services must override to `:string`/`:json`; otherwise the Transit envelope will surprise them. This is the one foot-gun of the chosen default, and is the documented reason overrides exist.
