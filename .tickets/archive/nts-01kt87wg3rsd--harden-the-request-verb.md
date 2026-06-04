---
id: nts-01kt87wg3rsd
title: Harden the `request` verb
status: closed
type: bug
priority: 1
mode: afk
created: '2026-06-04T02:37:33.688031805Z'
updated: '2026-06-04T04:03:20.054876927Z'
closed: '2026-06-04T04:03:20.054876927Z'
tags:
- review
- request
acceptance:
- title: '`(request conn subject data)` issues a request with the 5000 ms default and resolves on both legs'
  done: true
- title: An encode failure (`:bytes` codec on non-bytes) rejects the returned promise with `:codec-error` on both legs, never a synchronous throw
  done: true
- title: An over-max payload request rejects with `{:type :max-payload-exceeded}` on both legs, no raw native throw
  done: true
- title: clj-kondo clean; suite green on JVM and Node
  done: true
- title: 'JVM `-publish` rewired onto the shared `max-payload-error` helper without regression: the existing 1.1 MB publish over-max test stays green (in-scope side-change tightening publish''s unconditional IAE->`:max-payload-exceeded` mapping to a size check)'
  done: true
links:
- nts-01kt87wgex5a
---

## Description

The portable `request` verb must work end-to-end as documented on both legs (JVM + Node). Three gaps:

(a) The documented 3-arity `(request conn subject data)` does not exist — only the 4-arity does — so the documented call throws `ArityException`. Add it, delegating to the 4-arity with the 5000 ms default.

(b) `request` encodes its payload with `codec/encode` outside the promise chain, so an encode failure escapes as a synchronous `:codec-error` throw instead of a rejected promise. Move the encode inside the promise so failure rejects, matching the reply-decode side.

(c) JVM `-request`'s synchronous-throw guard catches only `IllegalStateException`, so other native sync throws (e.g. an over-max payload's `IllegalArgumentException`) leak raw. Broaden it and normalize an over-max request to `:max-payload-exceeded` on both legs (ADR 0006 — request must reject with a typed ex-info, never throw raw).

Coordinates with the JVM bare-ex-info ticket (linked) — both touch JVM `-request`.

## Notes

**2026-06-04T03:38:48.876441558Z**

**2026-06-04T03:50:08.424715173Z**

**2026-06-04T04:03:20.054876927Z**

Hardened the request verb across all three gaps, test-first. (a) Added the documented 3-arity (request conn subject data) delegating to the 4-arity with the 5000 ms default. (b) Rewrote the 4-arity as a resolved->then(encode)->bind(-request)->then(decode) chain plus new per-leg resolved/bind primitives, so an encode failure rejects rather than throwing synchronously at the call site. (c) Added a shared max-payload-error helper per leg (JVM by size, JS by type), wired into -request (new IAE catch -> rejected future) and rewired -publish onto it. clj-kondo clean; JVM 85/187 + Node 64/121 green.

## Plan amendment — scope clarifications (folded from review)

Two clarifications to the agreed approach above; the plan's mechanism is unchanged.

**1. `-publish` rewiring is in scope, not just `-request`.** Sharing the `max-payload-error` helper moves JVM `-publish`'s catch (jvm.clj:118-122) from an *unconditional* `IllegalArgumentException -> :max-payload-exceeded` onto the helper's size check. This is a deliberate behavior tightening — a non-over-max IAE now rethrows raw instead of being mislabeled — kept so publish and request discriminate over-max identically rather than diverging. Regression-guarded by the new AC below (the existing 1.1 MB publish over-max test still trips the size check). The alternative — apply the helper to `-request` only and leave publish's inline mapping — was rejected to avoid two verbs discriminating over-max differently.

**2. Bare ex-info at the portable seam on JVM is NOT this ticket's job — it is nts-01kt87wgex5a's.** AC "An over-max payload request rejects with `{:type :max-payload-exceeded}` on both legs" is satisfied via the test idiom `deref` + catch `ExecutionException` + `.getCause`. Until ex5a lands, a portable consumer reading `(:type (ex-data e))` directly on a JVM rejection still gets nil (the ex-info rides under `CompletionException.getCause`). The `bind`/`.thenCompose` added here adds no extra wrapping layer (CompletableFuture keeps the cause flat), so the `.getCause` idiom stays valid. No land-order dependency either way.

## Agreed implementation approach (grilled; ready for handover)

Self-contained plan for all three gaps. File:line anchors are from the current tree.

### (a) 3-arity — core.cljc `request` (126-138)
Add `([conn subject data] (request conn subject data {}))`, mirroring `publish` (70). The 4-arity already reads `(:timeout-ms opts 5000)`, so `{}` gives the 5000ms default.

### (b) encode-in-chain — bind-chain (NOT a try/catch wrapper)
The encode produces the bytes that feed `-request`, so it can't ride the post-`-request` `then` the way decode does. Rewrite the 4-arity as a 3-stage promise chain where encode and decode are BOTH `then` stages and request is the lone flattening `bind`:

```clojure
([conn subject data opts]
 (let [codec (effective-codec conn opts)]
   (-> (impl/resolved nil)
       (impl/then (fn [_]     (codec/encode codec data)))
       (impl/bind (fn [bytes] (proto/-request conn subject bytes (:timeout-ms opts 5000))))
       (impl/then (fn [raw]   (decode-msg codec raw))))))
```

A throw inside `then`/`bind` completes the promise exceptionally on both legs (does not propagate to the caller), so the encode's `:codec-error` rejects exactly as decode already does. `effective-codec` (60) is a pure `or` and can't throw, so encode is the only sync-throw site in the body.

Two NEW per-leg primitives (the existing `then` is value->value and does NOT flatten on the JVM, which is why `bind` exists):
- jvm.clj (beside `then`, 372):
  - `(defn resolved [x] (CompletableFuture/completedFuture x))`
  - `(defn bind [^CompletableFuture p f] (.thenCompose p (reify Function (apply [_ x] (f x)))))`  ; `Function` already imported
- js.cljs (beside `then`, 335):
  - `(defn resolved [x] (js/Promise.resolve x))`
  - `(defn bind [p f] (.then ^js p f))`  ; `.then` already flattens a returned thenable, so bind ≡ then here

### (c) over-max normalization — shared `max-payload-error` helper, mirroring `op-state-error`
Neither `IllegalArgumentException` (JVM) nor `InvalidArgumentError` (JS) is exclusively "over-max", so do NOT blanket-map by type.

- JS is already precise by type at our call site: a bad subject throws `InvalidSubjectError` (nats.js:69), and we always pass timeout>=1, never reply/noMux, never headers. nats.js catches the internal over-max and REJECTS (mux path, nats.js:359-369 -> request.js:131 `cancel(err)`) with the BARE `InvalidArgumentError` — not a sync throw, not a RequestError wrap.
- JVM is ambiguous: jnats throws `IllegalArgumentException` for over-max AND bad subject, so discriminate by SIZE.

Add a `max-payload-error` helper per leg that returns the typed ex-info OR the original exception (exactly like `op-state-error`):

jvm.clj (beside `op-state-error`, 65):
```clojure
(defn- max-payload-error
  [^Connection client subject ^bytes bytes ^Throwable e]
  (let [max (.getMaxPayload client)]
    (if (and (pos? max) (> (alength bytes) max))
      (ex-info "Message payload exceeds the server's max payload"
               {:type :max-payload-exceeded :subject subject :size (alength bytes) :max max} e)
      e)))
```
- `-request` (186-216): add a catch that is the structural twin of the existing IllegalStateException catch (212-216):
  ```clojure
  (catch IllegalArgumentException e
    (let [x (max-payload-error client subject bytes e)]
      (if (identical? x e) (throw e) (CompletableFuture/failedFuture x))))
  ```
- `-publish` (108-127): rewire its existing IAE catch to `(throw (max-payload-error client subject bytes e))`. The size-check tightens publish's current unconditional IAE->max-payload mapping with no test regression (the 1.1MB publish test still trips the size check).

js.cljs:
- `max-payload-error` helper = the ex-info builder (discrimination is the type-match): `{:type :max-payload-exceeded :subject subject :size (.-length bytes) :max (some-> (.-info ^js client) .-max_payload)}`.
- `-request` (180-200): add a branch to the existing `.catch` cond: `(instance? nats-core/InvalidArgumentError e) (max-payload-error client subject bytes)`.
- `-publish` (123-142): rewire its `InvalidArgumentError` branch onto the helper.

(JVM-by-size vs JS-by-type is the same kind of per-leg divergence `op-state-error` already has: JVM by message, JS by drain-state.)

### Tests — test/nats_cljc/core_test.cljc (idioms already in this file)
1. NEW `request-3-arity-uses-default-timeout`: round-trip via `(request conn subject data)` (no opts), mirroring `request-reply-round-trip` (962). Kept separate so the 4-arity+explicit-timeout path keeps its coverage.
2. NEW `request-encode-failure-rejects`: `{:codec :bytes}` on a string (the `:bytes`-on-non-bytes that codec_test `bytes-codec-rejects-non-bytes` (57) proves throws `:codec-error` on both legs). MUST prove rejection, not sync throw — split the call from the deref on the JVM so a regression throws at the call site:
   ```clojure
   (let [p (nats/request conn codec-error-subject "not bytes" {:codec :bytes})]
     (is (= :codec-error
            (try (deref p 2000 ::timeout) nil
                 (catch java.util.concurrent.ExecutionException e (:type (ex-data (.getCause e))))))
         "an encode failure rejects with :codec-error rather than throwing synchronously"))
   ```
   JS is the symmetric `p/then`(is false)/`p/catch` form. No responder needed (fails before the wire); reuse `codec-error-subject` ("err.codec", 122).
3. EXTEND + RENAME `max-payload-exceeded-throws` (1293) -> `max-payload-exceeded-normalized`: keep the publish-throws leg, add a request-rejects leg reusing `big`/`payload-subject`. Mirrors `connection-closed-normalized` (1317), which already covers a publish-sync-throw + request-reject in one deftest. JVM request-reject idiom: deref + catch ExecutionException + `.getCause` (see 1327-1331).

### Pre-implementation checks / coordination
- REPL-verify (clj-nrepl-eval) that jnats `requestWithTimeout` throws the over-max IAE SYNCHRONOUSLY (the whole sync-catch hinges on it; `-publish` already relies on it). If it instead completes the future exceptionally, the over-max branch must move into the `.handle` BiFunction's `:else` instead of the sync catch.
- Linked nts-01kt87wgex5a also edits JVM `-request` (unwraps CompletionException at the async-reject seam). No land-order dependency for THIS ticket's ACs: they verify via deref+`.getCause`, which already unwraps. Merge coordination only.
- No CONTEXT.md change (no new/sharpened glossary term) and no ADR (reversible internal helpers implementing the existing ADR 0006 reject contract).

### Done = AGENTS.md hard rules
`clj-kondo --lint src test` clean; suite green on JVM + Node against a ws-enabled nats-server.
