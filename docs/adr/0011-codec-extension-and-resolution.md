# Codec extension & resolution contract

ADR 0004 fixed *that* codecs are pluggable, EDN is the default, and Transit/JSON are opt-in behind a registry. This ADR pins the *shape* of that extension point and how a codec reference resolves.

A codec is a **`defprotocol ICodec`** with `-encode`/`-decode`, living in `nats-cljc.codec`. The three built-ins (`:edn`/`:string`/`:bytes`) are records implementing it, held in a **`defonce` registry** atom keyed by keyword and seeded at load. Opt-in codecs self-register from their own namespace via the public **`register!`** — `(require '[nats-cljc.codec.transit])` is what makes `:transit` resolvable. A **custom codec** is any `ICodec` instance, accepted **wherever a keyword is** (connection `:codec`, or a per-call `:codec` on publish/subscribe/request/reply).

`encode`/`decode` stay the public seam. They `resolve-codec` the reference — an `ICodec` instance passes through; a keyword is looked up in the registry — then call the protocol method inside a try/catch that normalizes **any** failure to `ex-info` `:type :codec-error` (3-arg, original as cause; an already-`:codec-error` ex-info is rethrown unwrapped, not nested).

A keyword that misses the registry also throws `:codec-error`, distinguished only by `ex-data`, not by a new canonical type: a **known opt-in** keyword carries `:require '<ns>` and an actionable "require this namespace" message; a **genuinely unknown** keyword carries just `:codec`.

## Considered options

- **`{:encode fn :decode fn}` map instead of a protocol** — lighter for a custom-codec author (a literal map, no `reify`), but "implement the encode/decode protocol" (ADR 0004's own words) reads as a Clojure protocol, and a protocol gives the polymorphic dispatch the code-quality follow-up (nts-01ksxx84gzkf, Finding 2) wanted in place of the original `case`. The two-function-wrapper convenience was deliberately *not* added on top — `reify ICodec` is ≤5 lines and a second construction path is speculative until proven common.
- **A multimethod on the codec keyword** — natural for keyword-dispatched built-ins, but a custom codec is an *instance*, not a keyword, so "accepted wherever a keyword is" would need type-based dispatch or a wrapper. The protocol covers both keyword-resolved and instance codecs uniformly.
- **A new canonical `:type :codec-not-loaded`** for the unloaded-opt-in case — rejected: the canonical `:type` set (CONTEXT.md, ADR 0006) is public contract, and "the codec couldn't be resolved" is still a codec error. The unloaded/unknown nuance rides in `ex-data` (`:require`) instead of expanding the vocabulary.

## Consequences

- `ICodec` and its method names are **public API** the moment a consumer ships a custom codec — changing them is a breaking change.
- `defonce` means re-evaluating `nats-cljc.codec` in a REPL preserves prior registrations (stable for opt-in loading); a redefined built-in needs an explicit registry reset to take effect.
- Registration is global process state, so the "unloaded keyword → actionable error" path is only observable before any namespace requires the opt-in codec. Its test is JVM-only (runtime `require` lets one deftest walk unloaded → require → round-trip in guaranteed order); the cljs legs load the opt-in namespace at compile time and test round-trip only.
- Where a *decode* failure travels after it is raised (to `:on-error` / the `:on-status` `:error` sink) is the normalized-error model's concern (ADR 0006), not this ADR's — here the contract is only that the `ex-info` is shaped `:codec-error`.
