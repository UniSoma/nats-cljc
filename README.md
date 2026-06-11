# nats-cljc - [NATS](https://nats.io) for Clojure and ClojureScript

[![License](https://img.shields.io/badge/Licence-Apache%202.0-blue.svg)](./LICENSE)
[![cljdoc](https://cljdoc.org/badge/io.github.unisoma/nats-cljc)](https://cljdoc.org/d/io.github.unisoma/nats-cljc/CURRENT)
[![Clojars](https://img.shields.io/clojars/v/io.github.unisoma/nats-cljc.svg)](https://clojars.org/io.github.unisoma/nats-cljc)
[![CI](https://github.com/unisoma/nats-cljc/actions/workflows/ci.yml/badge.svg)](https://github.com/unisoma/nats-cljc/actions/workflows/ci.yml)

> **Status: `0.5.0`.** The Phase 1 core, the Phase 1.5 blocking layer, Phase 2 JetStream (`nats-cljc.jetstream`), Phase 3 KV (`nats-cljc.kv`), and Phase 4 services (`nats-cljc.service`) are implemented, tested on the JVM, Node, and the browser, and published to Clojars. Being pre-1.0, the API may still evolve as Object Store and the async adapters (Phases 5–6) land — but within [ADR 0009](./docs/adr/0009-project-foundations-and-versioning.md)'s stability discipline: adding a normalized vocabulary member is a minor bump, renaming or removing one is a major bump. The decisions behind every choice live in [`CONTEXT.md`](./CONTEXT.md) (glossary) and [`docs/adr/`](./docs/adr/) (architecture decision records).

---

## Why

There are good NATS wrappers for the JVM (e.g. [clj-nats](https://github.com/cjohansen/clj-nats)). There isn't one where the *same* code runs on a JVM service and in a browser. nats-cljc is that: one non-blocking surface, two native clients underneath (`jnats` on the JVM, `@nats-io/nats-core` on ClojureScript), one transport rule per platform.

| Platform | Transport | Underlying client |
|---|---|---|
| JVM | TCP (`nats://…:4222`) | `io.nats:jnats` |
| Browser | WebSocket (`wss://…`) | `@nats-io/nats-core` (`wsconnect`) |
| Node | WebSocket (`wss://…`) | `@nats-io/nats-core` (`wsconnect`) |

> ClojureScript can only reach NATS over WebSocket, so **your server must enable its `websocket` listener**, and the WS port is **not** 4222.

## Install

```clojure
;; deps.edn
io.github.unisoma/nats-cljc {:mvn/version "0.5.0"}
```

That coordinate pulls in only the JVM client **`io.nats:jnats`** transitively. It deliberately forces **no other runtime dependency** — no async library (one-shot operations return the platform-native promise; see [Composing results](#composing-results)) and no serialization library (the default `:edn` codec uses only Clojure core; see [Codecs](#codecs)). On ClojureScript you additionally install the JS client yourself (shadow-cljs reads it from our `deps.cljs`):

```
npm install @nats-io/nats-core
```

## The model in one breath

- **One-shot operations return a promise** (`connect`, `request`, `flush`, `drain`, `close`). It's the platform-native promise — a `js/Promise` on CLJS, a `CompletableFuture` on the JVM — so portable code awaits it with [promesa](https://github.com/funcool/promesa), and CLJS-only code can `await` the *same* value natively.
- **Subscriptions deliver to a handler** — `(fn [message] …)` — called once per message.
- **Everything is data.** Payloads are encoded through a **codec** (`:edn` by default); messages are plain maps; errors are `ex-info` with a canonical `:type`.

---

## Composing results

One-shot operations return the **platform-native promise** — a `js/Promise` on ClojureScript, a `CompletableFuture` on the JVM. nats-cljc bundles **no** async library to compose them, so you pick the style you want; because the return type is native (not a promesa type), these interoperate freely and the choice is never imposed by the library:

- **Portable (recommended) — [promesa](https://github.com/funcool/promesa).** One source that awaits on every platform. Add it to your own deps:
  ```clojure
  funcool/promesa {:mvn/version "11.0.678"}
  ```
  then use `p/let` / `p/catch` as shown throughout this README.
- **ClojureScript only — native `await`** (1.12.145+), on the very same value — see [below](#clojurescript-only-native-await).
- **JVM only — `deref`.** `@(nats/connect …)` blocks for the result; or use the [blocking convenience layer](#jvm-only-blocking-convenience-layer) for synchronous ergonomics throughout.

---

## Quick start (portable — identical on all three platforms)

```clojure
(require '[nats-cljc.core :as nats]
         '[promesa.core :as p])

(p/let [conn (nats/connect {:servers "wss://demo.nats.io:8443"})] ; nats://…:4222 on the JVM
  ;; publish is fire-and-forget — returns nil
  (nats/publish conn "orders.created" {:id 123 :total 49.90})

  ;; subscribe returns a Subscription synchronously; the handler runs per message
  (nats/subscribe conn "orders.>"
    (fn [{:keys [subject data]}]
      (println "event on" subject "→" data)))

  ;; ensure the publish reached the server
  (nats/flush conn))
```

## Core API (`nats-cljc.core`, aliased `nats`)

| Verb | Signature | Returns |
|---|---|---|
| `connect` | `(connect opts)` | `Promise<Connection>` |
| `publish` | `(publish conn subject data)` · `(… opts)` | `nil` (fire-and-forget) |
| `subscribe` | `(subscribe conn subject handler)` · `(… opts)` | `Subscription` (sync) |
| `request` | `(request conn subject data)` · `(… opts)` | `Promise<Message>` |
| `reply` | `(reply conn msg data)` · `(… opts)` | `nil` |
| `unsubscribe` | `(unsubscribe sub)` · `(unsubscribe sub max)` | `nil` (sync) |
| `flush` | `(flush conn)` | `Promise` |
| `drain` | `(drain conn)` · `(drain sub)` | `Promise` |
| `close` | `(close conn)` | `Promise` |
| `subject` | `(subject & parts)` | `String` (e.g. `(nats/subject "orders" id "created")`) |

**Connection options:**

```clojure
(nats/connect
  {:servers   ["wss://a:8443" "wss://b:8443"]  ; string or vector (a cluster)
   :name      "orders-service"
   :codec     :edn                             ; default codec for this connection
   :auth      {:token "…"}                      ; or {:user … :pass …} / {:nkey … :seed …}
                                                ; / {:jwt … :seed …} / {:creds "<string content>"}
   :reconnect {:max 10 :wait-ms 2000 :jitter-ms 100}
   :on-status (fn [{:keys [type data]}] …)})    ; lifecycle + async errors land here
```

> `:creds` takes **string content**, not a file path — the browser has no filesystem. (A path is JVM-only sugar.)

> **`:servers` is the one non-portable value.** The browser needs `wss://host:8443`, the JVM `nats://host:4222` — different scheme *and* port. Treat the endpoint as per-deployment configuration (env / EDN), injected like a database URL: your `.cljc` code stays identical; only the supplied string differs per platform. Everything else in this API is write-once.

## Messages

A delivered or published message is a plain map. `:data` is the **decoded** value; `:reply` is always present (nil when the sender expects no reply); only `:headers` appears when set.

```clojure
{:subject "orders.123.created"
 :data    {:id 123 :total 49.90}        ; decoded via the codec
 :headers {"Nats-Msg-Id" ["abc-123"]}   ; string keys, vector-of-string values
 :reply   "_INBOX.x9f…"}                ; always present; nil when the sender expects no reply
```

## Request / reply

```clojure
;; responder (portable)
(nats/subscribe conn "echo"
  (fn [msg] (nats/reply conn msg {:echo (:data msg)})))

;; requester (portable)
(p/let [reply (nats/request conn "echo" {:hello "world"} {:timeout-ms 1000})]
  (println (:data reply)))            ;=> {:echo {:hello "world"}}
```

## Queue groups (competing consumers)

```clojure
;; every worker shares the queue group "workers"; the server delivers each
;; message to exactly one of them
(nats/subscribe conn "jobs.incoming" process-job {:queue "workers"})
```

## Codecs

The default is **`:edn`** — structured Clojure data round-trips with zero added dependencies. Override per connection or per call.

**Built-in (dependency-free):** `:edn` (default) · `:string` (UTF-8) · `:bytes` (passthrough — `:data` is the platform-native byte type).

**Opt-in (add the dependency, then require the codec namespace):** `:transit` and `:json` are not forced on consumers (see ADR 0004); you bring the dependency only if you use them.

```clojure
;; deps.edn — for :transit
com.cognitect/transit-clj {:mvn/version "1.0.333"}   ; CLJS: com.cognitect/transit-cljs

;; require the codec ns once at startup so the keyword resolves
(require 'nats-cljc.codec.transit)
(nats/publish conn "metrics.report" {:cpu 0.7} {:codec :transit})
```

```clojure
;; a subject shared with a non-Clojure service: speak raw bytes / UTF-8
(nats/subscribe conn "sensor.raw" handle-bytes {:codec :bytes})
```

Custom codecs implement a small protocol (`encode`/`decode`) and can be passed wherever a codec keyword is accepted — the same registry the opt-in codecs use.

## Connection status & errors

Lifecycle events and async errors arrive at `:on-status`. One-shot failures **reject the promise** with an `ex-info` whose `:type` is canonical on every platform.

```clojure
(nats/connect
  {:servers "wss://…"
   :on-status (fn [{:keys [type data]}]
                (case type
                  :reconnecting (println "reconnecting…")
                  :reconnected  (println "back online")
                  :error        (println "async error:" data)
                  nil))})

(-> (nats/request conn "maybe.nobody" {:q 1} {:timeout-ms 500})
    (p/catch (fn [e]
               (case (:type (ex-data e))
                 :no-responders (println "no one subscribes to that subject")
                 :timeout       (println "responders exist, none answered in time")
                 (throw e)))))
```

Canonical error `:type`s: `:timeout` · `:no-responders` · `:connect-failed` · `:connection-closed` · `:permissions-violation` · `:codec-error` · `:max-payload-exceeded` · `:protocol-error` · `:drained` · `:slow-consumer` · `:auth-invalid`.

**Caller-misuse validation errors** are a separate category ([ADR 0015](docs/adr/0015-validation-errors-are-a-separate-category.md)). A malformed argument — a non-token header name, an out-of-range `max`, a `reply` to a message with no reply subject — is caught *before* any native call and surfaced on the operation's own channel: synchronous operations **throw** it (`publish`, `subscribe`, `reply`, `unsubscribe`), while `connect` **rejects its promise**. They never reach an `:on-status`/`:on-error` sink. Their `:type`s — `:invalid-header` · `:invalid-max` · `:invalid-max-pending` · `:no-reply-subject` · `:invalid-capacity` — are diagnostic (fix the call, don't branch on them in production), and the set is open: new guards may add more.

## Backpressure without core.async

Within a subscription, messages are delivered **in order, one at a time**. Handlers must **never block**. To do async work in order without overrunning, **return a promise** — delivery of the next message waits for it to settle:

```clojure
(nats/subscribe conn "uploads.*"
  (fn [{:keys [data]}]
    (store-async data)))   ; returns a promise → the next message waits for it
```

## ClojureScript-only: native `await`

The returned promise is a native `js/Promise`, so on CLJS you can use the language's [native async/await](https://clojurescript.org/news/2026-05-07-release) (1.12.145+) instead of promesa — on the very same values:

```clojure
(defn ^:async show-echo []
  (let [conn  (await (nats/connect {:servers "wss://demo.nats.io:8443"}))
        reply (await (nats/request conn "echo" {:hi 1}))]
    (js/console.log (clj->js (:data reply)))))
```

## KV (`nats-cljc.kv`)

A portable facade over NATS Key/Value — the last-value registry built on JetStream. It speaks KV vocabulary throughout, never the stream substrate ([ADR 0023](./docs/adr/0023-kv-speaks-kv-vocabulary-not-its-stream-substrate.md)): Buckets and Revisions, `:bucket-not-found` rather than `:stream-not-found`, `:wrong-revision` rather than `:wrong-last-sequence`.

```clojure
(require '[nats-cljc.kv :as kv])

(p/let [ctx    (kv/kv conn)                                   ; verified at entry
        bucket (kv/create-bucket ctx {:bucket "config" :history 5})
        rev    (kv/put bucket "service.timeout-ms" 5000)      ; → new Revision
        entry  (kv/get bucket "service.timeout-ms")]
  (println "current:" (:value entry))                         ; decoded via the Bucket's codec
  (kv/update bucket "service.timeout-ms" 7500 rev))           ; compare-and-set
```

- **Entries are plain maps** — `{:bucket :key :value :revision :created :operation}`. `get` on an absent key resolves to `nil` (a normal outcome to branch on, not an error); a stored nil stays distinguishable as `{:value nil …}`. A `get` may pin `{:revision n}` to read an exact past Revision.
- **Compare-and-set** — `create` writes only when the key is absent; `update` only when the expected Revision is still latest. A lost race rejects with `:wrong-revision`.
- **Tombstones and history** — `delete` keeps the key's history readable; `purge` erases it down to a marker; `purge-deletes` reclaims every Tombstoned key Bucket-wide; `history` returns the retained Entries oldest-to-newest, markers visible; `keys` enumerates the live keys (optionally filtered, e.g. `"user.>"`).
- **Watch** — each matching Entry is pushed to a handler under the same promise-return backpressure contract as core subscriptions. `:deliver` chooses the replay (`:latest` default / `:history` / `:updates`), `:keys` filters by subject-style patterns, `:ignore-deletes?` suppresses markers, `:on-error` is the per-watch failure sink. The handle's `:initialized` promise resolves when the replay completes — the "cache is warm" signal — and `stop` ends it, idempotently.
- **One codec per Bucket** — bound at `create-bucket`/`open-bucket` (the connection default unless overridden there); a Bucket's values are homogeneous, never per-operation choices.
- **Operators** — `bucket-names`, `list-buckets`, `bucket-status`, `delete-bucket`.

On ClojureScript, `@nats-io/kv` is version-pinned and installed automatically alongside the core client; a bundle that never requires `nats-cljc.kv` ships zero KV bytes.

## Services (`nats-cljc.service`)

A portable facade for hosting discoverable, instrumented request-reply **Services**. Services is pure convenience over core request-reply — a queue-subscribed handler per endpoint plus the framework's auto-responders on `$SRV.PING|INFO|STATS.*` — so unlike KV/JetStream there is **no service context** and nothing is verified at entry ([ADR 0024](./docs/adr/0024-service-has-no-context-and-verifies-nothing-at-entry.md)). A client just calls an endpoint with plain `core/request`; there is no new caller verb.

```clojure
(require '[nats-cljc.service :as svc]
         '[nats-cljc.core :as nats])

;; host: endpoints are declared as data
(p/let [service (svc/create conn
                  {:name      "calc"
                   :version   "1.0.0"
                   :endpoints [{:name    "add"
                                :handler (fn [{:keys [data] :as msg}]
                                           (if (every? number? data)
                                             (svc/respond conn msg (apply + data))
                                             (svc/respond-error conn msg 400 "numbers only")))}]})]
  ;; a caller invokes the endpoint with an ordinary request — no service verb
  (p/let [reply (nats/request conn "add" [1 2 3])]
    (if-let [err (svc/error reply)]
      (println "service error" (:code err) (:description err))
      (println "sum:" (:data reply)))))                         ;=> sum: 6
```

- **Create as data** — `:name` and `:version` (semver) are required; `:description`, `:metadata`, and `:endpoints` are optional. Each endpoint is `{:name :subject :handler :queue-group :metadata}`: `:subject` defaults to `:name`, `:queue-group` load-balances across instances, and `:handler` is an ordinary [backpressure](#backpressure-without-coreasync) push handler (a returned promise applies per-endpoint backpressure). There is no Group noun — compose a grouped subject directly with `nats/subject`.
- **Reply** — `respond` answers a request through its native service message so the endpoint's stats stay correct (the service analog of `core/reply`). `respond-error` replies with a first-class service error — an integer `code` and string `description`, optionally a `data` body — which is a *successful* reply carrying an error, not a transport failure ([ADR 0025](./docs/adr/0025-service-application-errors-are-reply-payloads-not-normalized-errors.md)); like `respond` it is not terminal and sends exactly one reply. A handler that throws or returns a rejected promise auto-replies the same shape with code 500.
- **Read errors** — `(svc/error reply)` returns `nil` on a normal success or `{:code :description}` when the Service answered with an error. A service error is data the caller branches on, so `core/request` resolves normally and never throws on it.
- **Lifecycle** — `(svc/stop service)` resolves once stopped, **draining** in-flight requests (each runs to completion and still replies); afterwards a fresh request rejects with `:no-responders`. Idempotent. The Service handle carries a `:stopped` promise that resolves to nil once it stops for any reason — the react-to-shutdown signal.
- **Discovery** — `ping`, `info`, and `stats` query running Services over the control subjects, each a bounded fan-out resolving a **vector** of normalized maps: `ping` the identity `{:name :id :version}`, `info` adds `:description`/`:endpoints`, `stats` adds `:started` and per-endpoint counters (`:num-requests`, `:num-errors`, processing-time nanos). `opts` narrows by `:name`/`:id` and bounds the gather with `:max-results`/`:timeout-ms`. There is no Discovery handle and no local introspection — a Service inspects itself with the same wire request.
  ```clojure
  (p/let [services (svc/info conn {:name "calc" :timeout-ms 500})]
    (doseq [s services] (println (:name s) (:version s) (map :subject (:endpoints s)))))
  ```
- **One codec per Service** — bound at `create` (the connection default unless a `:codec` override there); a single `respond`/`respond-error` may override it per call.

On ClojureScript, `@nats-io/services` is version-pinned and installed automatically alongside the core client; a bundle that never requires `nats-cljc.service` ships zero services bytes. The dependency is unconditional and floors the nats-io trio at `3.4.0` ([ADR 0026](./docs/adr/0026-services-joins-the-unconditional-nats-family.md)).

## JVM-only: blocking convenience layer

When you want synchronous ergonomics on the JVM, require the parallel blocking tree instead. Same verb names; one-shots block, and subscriptions become a **pull loop** the async core can't offer:

```clojure
(require '[nats-cljc.blocking.core :as nats])   ; JVM only

(let [conn (nats/connect {:servers "nats://localhost:4222"})  ; blocks → Connection
      sub  (nats/subscribe conn "orders.>")]                  ; pull handle (no callback)
  (loop []
    (when-let [{:keys [subject data]} (nats/take-message sub 5000)]  ; blocks ≤ 5 s
      (println subject data)
      (recur)))
  (nats/close conn))                                          ; blocks until closed
```

## Roadmap

- **Phase 1** ✅ *(0.1.0)* — Core NATS: pub/sub, queue groups, request/reply, headers, codecs, lifecycle/status, errors.
- **Phase 1.5** ✅ *(0.1.0)* — `nats-cljc.blocking.core`.
- **Phase 2** ✅ *(0.2.0)* — JetStream (`nats-cljc.jetstream`): streams, consumers, acked publish, ack/nak/term; pull consumers delivered through the same promise-return handler as core subscriptions for backpressure (core.async/missionary adapters land in Phase 6).
- **Phase 3** ✅ *(0.4.0)* — KV (`nats-cljc.kv`): Bucket lifecycle and operator surface, compare-and-set writes, Tombstones/history/archaeology, and watches — speaking KV vocabulary, never its stream substrate (ADR 0023). Also shipped: JetStream direct get (`jetstream/get-message`, `:no-message-found`).
- **Phase 4** ✅ *(0.5.0)* — services (`nats-cljc.service`): host discoverable, instrumented request-reply Services and discover them with `ping`/`info`/`stats` — pure core request-reply, with no context to verify at entry (ADR 0024).
- **Phase 5** — Object Store (`nats-cljc.object`).
- **Phase 6** — core.async + missionary subscription adapters; `request-many` scatter-gather.

## Design docs

- [`CONTEXT.md`](./CONTEXT.md) — the project glossary (canonical terms).
- [`docs/adr/`](./docs/adr/) — every architecture decision and the trade-off behind it.

## License

Apache-2.0.
