# nats-cljc

**[NATS](https://nats.io) for Clojure and ClojureScript under one portable `.cljc` API.** Write your messaging code once; run it unchanged on the **JVM**, the **browser**, and **Node**.

> **Status: design sketch.** This README describes the intended public API, not a released library. The decisions behind every choice live in [`CONTEXT.md`](./CONTEXT.md) (glossary) and [`docs/adr/`](./docs/adr/) (architecture decision records). Signatures may shift during implementation.

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
;; deps.edn  (once published)
io.github.UniSoma/nats-cljc {:mvn/version "0.1.0"}
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

Canonical error `:type`s: `:timeout` · `:no-responders` · `:connect-failed` · `:connection-closed` · `:permissions-violation` · `:codec-error` · `:max-payload-exceeded` · `:protocol-error` · `:drained`.

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

- **Phase 1** — Core NATS (this sketch): pub/sub, queue groups, request/reply, headers, codecs, lifecycle/status, errors.
- **Phase 1.5** — `nats-cljc.blocking.core`.
- **Phase 2** — JetStream (`nats-cljc.jetstream`): streams, consumers, acked publish, ack/nak/term; pull consumers delivered through a channel/missionary adapter for backpressure.
- **Phase 3** — KV (`nats-cljc.kv`) and Object Store (`nats-cljc.object`); core.async + missionary subscription adapters; `request-many` scatter-gather.

## Design docs

- [`CONTEXT.md`](./CONTEXT.md) — the project glossary (canonical terms).
- [`docs/adr/`](./docs/adr/) — every architecture decision and the trade-off behind it.

## License

Apache-2.0.
