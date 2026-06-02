---
id: nts-01kstx9pbqe5
title: 'Codecs: built-ins, per-call override, custom protocol'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:22:24.119127883Z'
updated: '2026-06-02T03:11:07.669990407Z'
closed: '2026-06-02T03:11:07.669990407Z'
acceptance:
- title: A per-call `:codec` option overrides the connection default on publish, subscribe, and request
  done: true
- title: A custom codec implementing the encode/decode protocol works wherever a keyword codec is accepted
  done: true
- title: An encode or decode failure surfaces `ex-info` `:type :codec-error`
  done: true
- title: Each built-in codec (`:edn` `:string` `:bytes`) round-trips a representative value on all three platforms; `:edn` is the connection default
  done: true
- title: Opt-in codecs `:transit` and `:json` round-trip once their namespace is required (with the third-party dep present); referencing an unloaded codec keyword surfaces an actionable error
  done: true
deps:
- nts-01kstx8ysgv5
links:
- nts-01ksxx84gzkf
---

## Description

Pluggable payload encoding. Built-in dependency-free codecs `:edn` (default), `:string`, `:bytes`, selectable as the connection default (`:codec`) or overridden per call on publish/subscribe/request. `:transit` and `:json` are opt-in codecs in their own namespaces (`nats-cljc.codec.transit`, `nats-cljc.codec.json`) that a consumer requires after adding the third-party dependency — never forced on consumers (ADR 0004). Custom codecs implement the `encode`/`decode` protocol and are accepted anywhere a codec keyword is, via the same registry the opt-in codecs use. An encode or decode failure surfaces as `ex-info` `:type :codec-error`.

ADRs: 0004 (codec-centric, EDN default, Transit/JSON opt-in), 0006 (normalized errors).

## Design

Resolved via a `grill-with-docs` session. This section is the implementation handoff — an agent should be able to build the slice from it without re-deriving the decisions. Authority docs: **ADR 0011** (codec extension & resolution contract, written for this slice), **ADR 0004** (codec-centric, EDN default, opt-in tiers), **ADR 0006** (normalized errors). CONTEXT *Codec* was updated to list `reply` among override sites.

### Codec abstraction (AC2)

- `defprotocol ICodec` in `nats-cljc.codec` with `-encode`/`-decode` — **public** (it's the custom-codec extension point; dash-prefix matches house style in `nats-cljc.protocol`).
- Built-ins are `defrecord`s (`EdnCodec`/`StringCodec`/`BytesCodec`) implementing `ICodec`.
- A custom codec is any `ICodec` instance: `(reify nats-cljc.codec/ICodec (-encode [_ v] …) (-decode [_ b] …))`. **No** two-fn wrapper helper — reify directly.

### Registry + resolution

- `defonce registry` atom in `nats-cljc.codec`, seeded with the three built-ins.
- Public `register!` — `(codec/register! :transit (->TransitCodec))`. Opt-in namespaces call it at top level, so `(require …)` is what makes the keyword resolvable.
- Private `resolve-codec`: `ICodec` instance → as-is; keyword → registry lookup.
- Registry miss throws `ex-info :type :codec-error` (AC5), distinguished only by `ex-data`, **no new canonical type** (keep ADR 0006's set stable):
  - known opt-in (`:transit`/`:json`) → `{:type :codec-error :codec :transit :require 'nats-cljc.codec.transit}` + actionable "require this ns" message. Use a static hint map `{:transit 'nats-cljc.codec.transit, :json 'nats-cljc.codec.json}`.
  - genuinely unknown → `{:type :codec-error :codec :foo}`.

### Public `encode`/`decode` (AC3)

- Keep today's signatures `[codec value]` / `[codec bytes]` — they stay the seam `core.cljc` calls.
- Body: `resolve-codec` then call the protocol method inside try/catch (JVM `Throwable`, cljs `:default`). Normalize **any** failure to `ex-info :type :codec-error`, 3-arg with original as cause, `ex-data` = `{:type :codec-error :codec … :op :encode|:decode}`.
- **No double-wrap**: an already-`:codec-error` ex-info (the `:bytes` guard, registry miss) is rethrown as-is.
- **Minimal ex-data** — no raw value/bytes (can be large/sensitive).
- **Scope boundary**: this slice only produces the `:codec-error` *shape*. Where a decode failure *travels* (to `:on-error` / `:on-status :error`) is the error-model ticket **nts-01kstxatbw6k**, not here. In `subscribe`'s handler-wrapper a decode failure simply throws the normalized error.

### Built-in codec semantics (AC4) — `:edn` default

- `:edn` — unchanged from today (`pr-str` + `clojure.edn`/`cljs.reader`, never `eval`). Connection default.
- `:string` — **lenient**: encode `(str->bytes (str value))`; decode → UTF-8 string. No type guard.
- `:bytes` — **strict passthrough**: encode returns the value iff it's platform bytes (`bytes?` on JVM, `(instance? js/Uint8Array x)` on cljs), else `:codec-error`; decode returns bytes as-is. Consequence: delivered `:data` is platform-native bytes — the ADR-0004-sanctioned exception to *Data* being a portable Clojure value.

### Per-call override (AC1)

- Precedence everywhere: `(or (:codec opts) (:codec conn))`. The per-call value may be a keyword **or** an `ICodec` instance.
- Wire `:codec` into the opts map of `publish`, `subscribe`, `request`, **and `reply`** — `reply` gains an optional opts map: `([conn msg data] [conn msg data opts])`. (AC1 names only the first three; `reply` is added so the polyglot response leg can match the request's codec.)
- `request` encode-failures **throw synchronously** (uniform with publish/reply, which have no promise to reject); the reject-the-promise refinement is deferred to nts-01kstxatbw6k.

### Opt-in codecs (AC5) — own `.cljc` namespaces, self-register at load

- `nats-cljc.codec.transit` — **transit-json**, default read/write handlers (msgpack ruled out: transit-cljs has none, and transit-json is the cross-platform interop format). transit-clj on JVM, transit-cljs on cljs (both expose `cognitect.transit`; reader-conditional bodies for the differing writer/reader construction). Clojure-faithful (keywords/sets/symbols survive).
- `nats-cljc.codec.json` — `org.clojure/data.json` on JVM, ambient `js/JSON` + `clj->js`/`js->clj` on cljs. **Keywordize keys on decode** (`:key-fn keyword` / `:keywordize-keys true`). Documented **lossy** (keyword *values* → strings, no rich types) — it's a polyglot wire, not a Clojure round-trip format.
- Each namespace `register!`s itself at top level.

### Dependency placement (keep the forced footprint clean)

- **Unchanged**: `deps.edn :deps` (clojure + jnats), `src/deps.cljs :npm-deps` (nats-core), `package.json dependencies`.
- `deps.edn :test` += `org.clojure/data.json`, `com.cognitect/transit-clj`.
- `deps.edn :cljs` += `com.cognitect/transit-cljs` (no data.json on cljs — ambient JSON).
- `package.json devDependencies` += transit-cljs's npm peer `transit-js` — **read the exact package/version off transit-cljs's own `deps.cljs`, don't guess**.
- Opt-in `.cljc` namespaces are compiled only when required, so a consumer who never requires them needs none of these deps even though the source ships.

### File layout

- Rework `src/nats_cljc/codec.cljc` (protocol + registry + `register!` + `resolve-codec` + built-in records + wrapped `encode`/`decode`; keep `str->bytes`/`bytes->str`).
- New `src/nats_cljc/codec/transit.cljc`, `src/nats_cljc/codec/json.cljc`.
- `src/nats_cljc/core.cljc` — add `:codec` to the opts destructuring at `publish`/`subscribe`/`request`, add the opts arity + `:codec` to `reply`, and pass `(or (:codec opts) (:codec conn))` through. No impl/protocol changes (Connection already carries `codec`).

### Test matrix

- Built-in round-trips (`:edn`/`:string`/`:bytes`, representative value each) — JVM + Node local, browser CI (ADR 0010).
- Per-call override: publish with `:codec` ≠ conn default, assert decode.
- Custom codec: a `reify ICodec` round-trips inline.
- `:codec-error` shape: decode garbage with `:edn`; `:bytes` given a non-byte.
- Opt-in transit/json round-trips with the dep present (browser gets transit-cljs/transit-js via `:cljs` + devDependencies).
- **Unloaded-codec error — JVM-only**, one self-contained `deftest` per opt-in codec walking unloaded → runtime `require` → round-trip (guaranteed order; relies on no other test ns top-level-requiring the opt-in codec on the JVM). cljs legs top-level-require and test round-trip only — the unloaded path is platform-agnostic logic, proven on the JVM leg.

### Before commit (AGENTS hard rules)

- `clj-kondo --lint src test`.
- Run the suite on JVM + Node against a ws-enabled `nats-server`.

### Landing-time follow-ups (do at ship, not before)

- `knot add-note nts-01ksxx84gzkf` recording that Finding 2's **codec-dispatch half** is satisfied here (registry+protocol superseded the `case`); leaves auth-variant reshape + Finding 1 test-consolidation open. **Do not close it, do not edit its ACs.**

## Notes

**2026-06-02T03:11:07.669990407Z**

Pluggable codecs shipped, green on JVM + Node (browser CI-only, ADR 0010). nats-cljc.codec reworked from a single-:edn `case` into an ICodec protocol + defonce registry: built-ins :edn (default), :string (lenient), :bytes (strict platform-byte passthrough) are records seeded in the registry; resolve-codec passes ICodec instances through and looks keywords up, so a custom (reify ICodec) works wherever a keyword does (AC2). encode/decode wrap the protocol call, normalizing any failure to ex-info :type :codec-error (:op :encode|:decode), rethrowing an existing :codec-error as-is — the :bytes guard, garbage-EDN, and registry-miss all surface it (AC3). Per-call :codec wired into publish/subscribe/request with precedence (or (:codec opts) (:codec conn)); reply gained an opts arity for the response leg (AC1). Opt-in :transit (transit-json; transit-clj JVM / transit-cljs cljs) and :json (data.json JVM / ambient js/JSON cljs, keywordized + lossy) live in their own nats-cljc.codec.{transit,json} namespaces that self-register at load — requiring the ns is what makes the keyword resolvable; an unloaded opt-in keyword surfaces :codec-error with an actionable :require '<ns> hint (AC5). transit-clj/data.json are :test-scoped and transit-cljs is :cljs-scoped (it pulls com.cognitect/transit-js classpath JS transitively — no package.json change), keeping the forced footprint clean (ADR 0004). 8 new codec_test deftests + 2 core_test integration tests; JVM unloaded->require->round-trip walked in-process per opt-in codec, cljs requires them at compile time. Note left on nts-01ksxx84gzkf: Finding 2's codec-dispatch half is satisfied here.
