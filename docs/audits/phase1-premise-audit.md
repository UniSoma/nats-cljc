# Phase 1 premise audit — nats-cljc

**Date:** 2026-05-31
**Scope audited:** the *shipped* Phase 1 core surface in `src/` at `HEAD` (6cb828f) — pub/sub,
subscription dispatch / backpressure, codecs, lifecycle (flush/drain/close), status events,
auth, reconnect, and connect-error normalization.
**Goal:** inventory every behavioral premise the shipped code bakes in about the underlying
clients, and ground each against the *pinned* versions of those clients — or mark it ASSUMED.

## Pinned versions (grounded against THESE)

| Leg | Wrapped client | Version | Source of truth used |
|---|---|---|---|
| JVM | `io.nats:jnats` | **2.25.3** | `deps.edn:9`. Grounded against `nats-io/nats.java` tag `2.25.3` source (`src/main/java/io/nats/client/…`) **and** live reflection on the loaded classpath. |
| Node + browser (CLJS) | `@nats-io/nats-core` | **3.3.1** | `package.json:7`, `src/deps.cljs:2`. Grounded against the installed `node_modules/@nats-io/nats-core/lib/*.{js,d.ts}` (the published 3.3.1 artifact; the package ships compiled `lib/`, no `src/`). |

> The CLJS leg wraps the **modular v3** package `@nats-io/nats-core`, not the legacy
> `nats`/`nats.ws` monorepo. The WebSocket transport (`wsconnect`) is bundled *inside* this package.

## Grounding method

Three independent groundings fed this ledger, all citing the pinned artifacts:
- **jnats** — `nats-io/nats.java` source at tag `2.25.3` (file:line below), cross-checked with live
  reflection on the loaded jar (return types, enum members, method existence).
- **nats-core** — the installed `@nats-io/nats-core@3.3.1` `lib/*.{js,d.ts}` (the authoritative
  shipped artifact; `.d.ts` preserve interfaces/JSDoc, `.js` preserve logic).
- **protocol** — `docs.nats.io` (FAQ, core-NATS, slow-consumers, request-reply, queue) and the
  `nats-architecture-and-design` ADRs.

Two early "findings" were **false leads** from a transiently stale shell and are recorded in the
Appendix so they are not re-derived: (a) "`wsconnect` is missing from nats-core" — **false**, it is
exported (`ws_transport.js:301`); (b) "`subscribe` is `@`-deref'd on a `Dispatcher`" — **false**,
`Dispatcher.subscribe(String, MessageHandler)` returns a `Subscription` and the tests don't deref it.

---

## 0. Scope reality check (the first premise to correct)

**Premise (from the audit brief / ready tickets): "Phase 1 core surfaces = pub/sub, queue groups,
request/reply (+ reply sugar), headers, codecs, lifecycle/status, errors, dispatch/backpressure."**

**CONTRADICTED by the shipped code.** `nats-cljc.protocol/Conn` (`src/nats_cljc/protocol.cljc:13-30`)
defines only `-publish`, `-subscribe`, `-flush`, `-drain`, `-close`. The facade
(`src/nats_cljc/core.cljc`) exposes only `connect`, `publish`, `subscribe`, `flush`, `drain`,
`close`. **Not yet shipped:** request/reply, headers, queue groups, per-call codec override, the
`:transit`/`:json` codecs and the codec *registry*, `:max-pending`, and per-subscription
`:on-error`. These are open "Ready" tickets (`nts-01kstx9hs32y` request/reply, `nts-01kstx9xghet`
headers, `nts-01kstx9snk41` queue groups, `nts-01kstx9pbqe5` codecs). The ledger covers what is
**actually in `src/`**; premises about unshipped surfaces are under §I as *intended-scope* notes.

---

## A. Subscription dispatch & backpressure  *(Exhibit A — required deep dive)*

| # | Premise (falsifiable) | Our code | Library citation @ pinned version | Class | Cross-leg note |
|---|---|---|---|---|---|
| A1 | jnats delivers to a `MessageHandler` via a `Dispatcher` (one per `-subscribe`); `Dispatcher.subscribe(subject, handler)` returns a `Subscription` synchronously. | `impl/jvm.clj:35,37` | jnats 2.25.3 `Dispatcher.java` `Subscription subscribe(String, MessageHandler)`; `impl/NatsDispatcher.java` (`createDispatcher` on `Connection.java`) | **GROUNDED** | JVM-only mechanism; JS has no dispatcher (A2). |
| A2 | nats-core delivers per-message via a `callback` option (not async iteration); `subscribe(subject,{callback})` returns the `Subscription` synchronously. | `impl/js.cljs:29-30` | nats-core 3.3.1 `lib/core.d.ts:48,72,88` (`SubOpts.callback?`), `:313` (`subscribe(...): Subscription`); `lib/protocol.js:111` `noIterator = typeof opts.callback === "function"` | **GROUNDED** | Both legs deliver via a callback handed to the native client; the native models differ but the call shape matches. A callback sub explicitly **cannot** be iterated (`queued_iterator.js:70-71` throws "iterator cannot be used when a callback is registered"). |
| A3 | **The handler's return value is IGNORED by the native client** — returning a promise does NOT make the native client wait. | `impl/jvm.clj:48-51`; `impl/js.cljs:36` | JVM: `MessageHandler.java` `void onMessage(Message) throws InterruptedException` + the inline call in `NatsDispatcher.run()`. JS: `lib/protocol.js:601-614` `processMsg` calls `sub.callback(null, new MsgImpl(...))` **un-awaited**, return discarded; `lib/core.d.ts:44` `MsgCallback = (err, msg) => void \| Promise<never>`. | **GROUNDED** | **Load-bearing fact for Exhibit A.** On *both* legs the native client discards the handler return, so the suspend-next behavior is entirely the wrapper's `tail`-atom construct. |
| A4 | The wrapper synthesizes per-subscription backpressure with a `tail` atom: each delivery composes onto the previous one's settle (`CompletableFuture.thenCompose` / `Promise.then`); a returned promise suspends the next, a non-promise delivers at once. | `impl/jvm.clj:36,42-54`; `impl/js.cljs:28,33-37`; `docs/adr/0007` | n/a (wrapper construct atop A3) | **GROUNDED (our construct)** | Structurally identical on both legs — faithful to ADR 0007 on JVM **and** CLJS. |
| A5 | jnats dispatches `onMessage` **serially on one dispatcher thread**, so the tail mutates without contention and the thread is never blocked (compose returns at once). | `impl/jvm.clj:24-26` (comment) | jnats 2.25.3 `impl/NatsDispatcher.java`: `class NatsDispatcher … implements Dispatcher, Runnable`; `run()` loops `NatsMessage msg = incoming.pop(waitForMessage); … handler.onMessage(msg);`, started via `connection.getExecutor().submit(this, …)` (one thread, one `ConsumerMessageQueue`, one msg at a time). | **GROUNDED** | CLJS analogue is the single event loop (A6). |
| A6 | The JS event loop is single-threaded, so the tail mutates without contention and is never blocked. | `impl/js.cljs:18-19` (comment) | ECMAScript single event loop (language invariant). | **GROUNDED** | CLJS-only; JVM analogue is A5. |
| A7 | **Under a sustained-slow handler the undelivered backlog grows UNBOUNDED in the wrapper chain, and the native slow-consumer never trips** — because the handler returns immediately, the native client's own queue/buffer stays empty. | `impl/jvm.clj:30-34` (comment); `impl/js.cljs:23-27` (comment) | JVM: jnats bounds *its own* `ConsumerMessageQueue` — `impl/NatsConsumer.java` `hasReachedPendingLimits()` with `Consumer.DEFAULT_MAX_MESSAGES = 512*1024`, `DEFAULT_MAX_BYTES = 64MB`; checked at enqueue in `impl/NatsConnection.deliverMessage()` → `processSlowConsumer` → `ErrorListener.slowConsumerDetected`. JS: slow detection runs off the **iterator buffer** (`protocol.js:147-152` via `getPending()`) and is **gated off for callback subs** — `nats.js:108-116` wires `setSlowNotificationFn` only when `opts.callback` is *not* a function, and `protocol.js:138-143` throws "callbacks don't support slow notifications"; it is also opt-in (`slow?: number`, default off). | **GROUNDED (both legs)** | The unbounded-backlog concern the project flags for rework is **real and confirmed on both legs**: returning immediately keeps the native buffer empty (JVM: pending-limits never reached; JS: callback subs never enqueue into the iterator buffer that feeds slow detection). The backlog instead lives in the wrapper's promise chain — unbounded and unmonitored. No `:max-pending` / `:slow-consumer` emission yet (unshipped). |
| A8 | In-order, serial delivery within a single subscription (each handler call completes before the next begins). | `docs/adr/0007`; test `single-subscription-delivers-in-order` (`core_test.cljc:555-581`, 50 msgs, one publisher) | Handler serialization: A3/A5 (JVM), A4/A6 (JS). Wire ordering: docs.nats.io/reference/faq — "messages from a given single publisher will be delivered to all eligible subscribers in the order in which they were originally published"; "not across different publishers". | **GROUNDED (with framing caveat)** | The documented guarantee is **per-publisher source ordering**, not "per-subscription" as a primitive — per-subscription order is a *consequence* for a single publisher. ADR 0007's "within one subscription, in order" holds for a single publisher (as the test exercises); **multiple concurrent publishers to one subject have no cross-publisher order guarantee** — worth stating in the contract. |
| A9 | Cross-subscription independence — a slow handler on one sub never stalls another (no cross-sub ordering/concurrency guarantee). | `docs/adr/0007`; test `subscriptions-are-independent` (`core_test.cljc:642-689`) | JVM: each `-subscribe` creates its own `Dispatcher` (`impl/jvm.clj:35`) → own thread (A5). JS: own `tail` atom per sub (`impl/js.cljs:28`) on the shared loop. | **GROUNDED** | Mechanism differs (thread-per-dispatcher vs cooperative) exactly as ADR 0007 says; the unified contract only promises independence, not parallelism. |
| A10 | `.exceptionally` / `.catch` keeps a throwing/rejecting handler from stalling the chain (error swallowed; routing deferred). | `impl/jvm.clj:52-54`; `impl/js.cljs:37` | Wrapper construct over `CompletableFuture.exceptionally` / `Promise.catch`. | **GROUNDED (our construct)** | Identical both legs. Errors are currently **dropped**, not routed to `:on-status`/`:on-error` (that sink is unshipped — §H3). |

### Exhibit A — verdict prose

**(a) Per-subscription dispatch.** Confirmed against both pinned artifacts: **jnats** runs exactly
one thread per `Dispatcher` (`NatsDispatcher.run()`, submitted to the connection executor) that
pops one message at a time from a single `ConsumerMessageQueue` and calls `handler.onMessage(msg)`
inline — strictly serial. **nats-core** invokes a per-message `callback` synchronously inside
`ProtocolHandler.processMsg` (`protocol.js:601-614`). The wrapper deliberately uses the *callback*
form on JS so the subscription stays synchronous/per-message rather than becoming an async iterable
— the ADR-0007 model.

**(b) Backpressure / slow-consumer.** The shipped contract — *"a handler may return a promise; the
next message suspends until it settles"* — is **faithful on both legs, but only because the wrapper
fakes it.** The decisive, grounded fact (A3): **both native clients ignore the handler's return.**
jnats' `onMessage` is `void`; nats-core's `processMsg` calls the callback un-awaited and discards
its result. So neither client suspends anything — the suspend is produced entirely by the wrapper's
`tail` atom (`thenCompose` on JVM, `then` on JS). The two implementations are structurally
identical, so the contract is honored equally on JVM and CLJS; the unified API papers over nothing
here.

**The "unbounded dispatch backlog" concern is real on both legs (A7), confirmed to source, for the
same root cause.** Because the handler returns immediately, the native client's own buffer never
fills:

- **JVM:** jnats' slow-consumer is enforced in `NatsConnection.deliverMessage()` against the size
  of the dispatcher's `ConsumerMessageQueue` (`hasReachedPendingLimits()`; defaults **512K msgs /
  64 MB**, not 65536). That queue only backs up if `onMessage` is slow to return — which it never
  is here. So `slowConsumerDetected` never fires.
- **CLJS:** nats-core's slow detection is computed off the *async-iterator* buffer and is wired only
  for non-callback subs (`nats.js:108-116`); `setSlowNotificationFn` actively throws for a callback
  sub. So a callback subscription gets no buffering, no bound, and no slow signal.

In both cases the undelivered work accumulates in the wrapper's promise chain — an **unbounded,
unmonitored** structure invisible to the native client — and there is no `:max-pending` /
`:slow-consumer` yet. **The protocol layer offers no rescue either** (P-C): the NATS *server's*
slow-consumer cutoff (`write_deadline`, default 10s) only fires when the server can't flush bytes
into the socket; a client that reads the socket promptly but queues into an unbounded in-process
backlog keeps a healthy connection and is never cut off — "the server protects itself, not your
heap." This is precisely the gap the in-code comments attribute to `nts-01kstxatbw6k` AC#4. The
promise-return *contract* shipped in 4d5fc30 is correct and CI-green (`nts-01kstxa6v2mm`); its
manual `tail`-chaining is **to be superseded** — not reverted — by the native-consumption rework
in `nts-01kstxatbw6k` AC#4, which deletes the tail-chaining as a side effect of moving to a model
the native client can bound. **The embedded-knowledge error was not that
backpressure "works" — it does — but the assumption that the native (or server) slow-consumer
would still provide a floor underneath it. On both legs, it does not.**

---

## B. Pub/sub & message shape

| # | Premise | Our code | Library citation @ version | Class | Cross-leg note |
|---|---|---|---|---|---|
| B1 | `publish` is fire-and-forget; facade returns **nil**. | `core.cljc:29-34` | JVM: `Connection.java:132` `void publish(String, byte[])`. JS: `core.d.ts:292` `publish(subject, payload?, options?): void`; `:267` `Payload = Uint8Array \| string`. | **GROUNDED** | Both `void`/nil. Agree. |
| B2 | Low-level handler gets `{:subject <string> :bytes <platform-bytes>}`; subject is a string, bytes the raw payload. | `protocol.cljc:17-20`; `impl/jvm.clj:40-41`; `impl/js.cljs:31-32` | JVM: `Message.getSubject → String`, `Message.getData → byte[]`. JS: `core.d.ts:407-424` `Msg { subject: string; data: Uint8Array }`; `msg.js:32-38` subject getter, `:56-63` data getter (slices header bytes). | **GROUNDED** | Raw byte type differs (`byte[]` vs `Uint8Array`) — correctly quarantined; only the codec touches it. |
| B3 | jnats/nats-core both support headers + reply-to on publish, but the wrapper wires **only** `(subject, bytes)`. | `impl/jvm.clj:18-19`; `impl/js.cljs:10-11` | jnats: `Connection.java` `publish(String,Headers,byte[])`, `(String,String,byte[])`, `(String,String,Headers,byte[])` (unused). nats-core: `core.d.ts:292` `PublishOptions` (carries headers/reply, unused). | **GROUNDED** | Confirms headers/reply are genuinely unshipped (matches §0); the capability exists when those tickets land. |
| B4 | `subscribe` returns a native `Subscription` **synchronously** (used directly, never `@`-deref'd). | `core.cljc:36-45`; asserts `core_test.cljc:269,283` | JVM: `Dispatcher.java` `subscribe(String,MessageHandler) → Subscription`. JS: `core.d.ts:313` `subscribe(...): Subscription`. | **GROUNDED** | Agree both legs. (Corrects an early false lead — Appendix.) |

---

## C. Codecs

| # | Premise | Our code | Citation | Class | Cross-leg note |
|---|---|---|---|---|---|
| C1 | EDN round-trips Clojure data via `pr-str` + a non-`eval` reader (`clojure.edn` / `cljs.reader`). | `codec.cljc:8-9,22-33` | Clojure/CLJS core; `docs/adr/0004`. | **GROUNDED (ADR/core)** | Reader differs by platform; both avoid `eval`. |
| C2 | UTF-8 via `String.getBytes(UTF_8)` / `new String(…,UTF_8)` (JVM) and `TextEncoder/TextDecoder` (CLJS). | `codec.cljc:11-17` | JDK `StandardCharsets`; WHATWG `TextEncoder/TextDecoder`. | **GROUNDED** | `TextDecoder().decode` relies on `Msg.data` being a `Uint8Array` (B2 ✓). |
| C3 | Unknown codec → `ex-info {:type :codec-error}`. | `codec.cljc:24-25,32-33` | Wrapper construct; ADR 0006 canonical `:type`. | **GROUNDED (our construct)** | Only `:edn` implemented; `:bytes`/`:string`/`:transit`/`:json` + per-call override + registry unshipped despite ADR 0004. |

---

## D. Lifecycle — flush / drain / close

| # | Premise | Our code | Library citation @ version | Class | Cross-leg note |
|---|---|---|---|---|---|
| D1 | jnats `flush(Duration)` **blocks** and is `void` → wrapper runs it off-thread to return a settling promise. | `impl/jvm.clj:55-61` | `Connection.java:454` `void flush(Duration) throws TimeoutException, InterruptedException`. | **GROUNDED** | JS `flush()` returns a Promise (`impl/js.cljs:38-41`) → no wrap needed. Divergence handled. |
| D2 | jnats `drain(Duration)` already returns `CompletableFuture<Boolean>` → used directly. | `impl/jvm.clj:62-65` | `Connection.java:483` `CompletableFuture<Boolean> drain(Duration) throws TimeoutException, InterruptedException`. | **GROUNDED** | JS `drain()` returns a Promise (`impl/js.cljs:42-45`). Both native-async. (Minor: jnats `drain` also declares `throws TimeoutException` for its initial flush.) |
| D3 | jnats `close()` is **blocking and `void`** → wrapper runs it off-thread. | `impl/jvm.clj:66-72` | `Connection.java:493` `void close() throws InterruptedException`. | **GROUNDED** | JS `close()` returns a Promise (`impl/js.cljs:46-49`). Off-thread wrap on JVM only. |
| D4 | `Subscription.drain(Duration)` returns `CompletableFuture<Boolean>` (facade routes a sub here). | `impl/jvm.clj:175-181`; `core.cljc:53-62` | `Subscription.java:39 extends Consumer`; `Consumer.java:124` `CompletableFuture<Boolean> drain(Duration) throws InterruptedException`. | **GROUNDED** | JS `sub.drain()` returns a Promise (`impl/js.cljs:156-161`). |
| D5 | `drain` distinguishes connection vs subscription via `(satisfies? proto/Conn x)`. | `core.cljc:59-62` | Wrapper construct; a native `Subscription`/`Dispatcher` does not implement `proto/Conn`. | **GROUNDED** | Only the platform `*Connection` record implements `Conn`; a native sub falls through to `drain-subscription`. Works both legs. |

---

## E. Status / lifecycle events

| # | Premise | Our code | Library citation @ version | Class | Cross-leg note |
|---|---|---|---|---|---|
| E1 | jnats `ConnectionListener.Events` = CONNECTED, DISCONNECTED, RECONNECTED, CLOSED, DISCOVERED_SERVERS, LAME_DUCK (+ RESUBSCRIBED, dropped); **no RECONNECTING**. | `impl/jvm.clj:77-83` (maps 6, drops RESUBSCRIBED) | `ConnectionListener.java:24` `enum Events { CONNECTED, CLOSED, DISCONNECTED, RECONNECTED, RESUBSCRIBED, DISCOVERED_SERVERS, LAME_DUCK }` (7 values). | **GROUNDED** | ⚠ A *different* enum, `Connection.Status` (`Connection.java`), DOES contain `RECONNECTING`/`CONNECTING` — connection status, not a listener event. Don't confuse them. |
| E2 | jnats has no reconnecting event → JVM **synthesizes** `:reconnecting` after DISCONNECTED (gated on reconnect enabled). | `impl/jvm.clj:94-106` | E1 (no RECONNECTING member); ADR 0006. | **GROUNDED** | nats-core *does* emit reconnecting natively (E4), so no synthesis there → the accepted count divergence (1/loss JVM vs 1/dial JS). |
| E3 | nats-core `status()` is an async-iterable of objects with a `.type` string; iterate via `Symbol.asyncIterator`. | `impl/js.cljs:75-89` | nats-core 3.3.1 `core.d.ts:1-43` (`Status` discriminated union); dispatched onto the iterable at the `dispatchStatus` sites in `protocol.js`. | **GROUNDED** | JVM uses a push `ConnectionListener` instead — opposite delivery model, unified by `deliver-status!`. |
| E4 | The native status **strings** map: `"disconnect"→:disconnected`, `"reconnecting"→:reconnecting`, `"reconnect"→:reconnected`, `"ldm"→:lame-duck`, `"update"→:servers-changed`, `"close"→:closed`. | `impl/js.cljs:58-64` | nats-core `core.d.ts:1-43` literal set: `"disconnect","reconnect","reconnecting","update","ldm","error","ping","staleConnection","forceReconnect","slowConsumer","close"`. Dispatch sites: `protocol.js:424/425` disconnect, `:431/432` reconnect, `:502` reconnecting, `:703` update, `:707` ldm, `:900` `push({type:"close"})`. | **GROUNDED** | All six mapped strings are valid 3.3.1 literals — including `"close"` (emitted on the iterable, `protocol.js:900`) and `"reconnecting"`. nats-core 3.3.1 uses a `type`-literal union (the old `Events`/`DebugEvents` enums are gone). Note the wrapper ignores `"error"`/`"slowConsumer"`/`"ping"`/`"staleConnection"`/`"forceReconnect"` (intended — error/slow belong to later slices). |
| E5 | nats-core emits **no** event for the initial successful connection → `:connected` is synthesized at connect. | `impl/js.cljs:142-147,55-57` | nats-core `protocol.js`/`nats.js:38-49`: the static `connect` path emits no status; `"reconnect"` is emitted only from `disconnected()` (`protocol.js:423-432`), i.e. after a drop. | **GROUNDED** | Symmetric-but-reverse of E2; jnats *does* fire CONNECTED (E1). |
| E6 | "Shape, not cadence": counts/ordering/triggers follow each native client (e.g. `DISCOVERED_SERVERS` only on genuinely-new gossip vs nats-core `update` on ~every server INFO). | `impl/jvm.clj:77-83`; `impl/js.cljs:58-64`; `docs/adr/0006`, `CONTEXT.md` | Event *mapping* grounded (E1/E4). The exact *trigger frequencies* are ADR 0006 assertions. | **GROUNDED (mapping) / ASSUMED (cadence)** | The shape normalization is grounded; the per-client trigger-frequency claims (e.g. `update` on every INFO) remain ADR assertions, not re-confirmed to source line. Low priority — the contract deliberately does not normalize cadence. |
| E7 | `deliver-status!` delivers a bare `{:type ...}` and drops unmapped events. | `impl/jvm.clj:85-92`; `impl/js.cljs:66-73` | Wrapper construct (ADR 0006). | **GROUNDED (our construct)** | Tested at the seam for `:lame-duck`/`:servers-changed` (`core_test.cljc:455-469`), bypassing a live cluster. |

---

## F. Auth

| # | Premise | Our code | Library citation @ version | Class | Cross-leg note |
|---|---|---|---|---|---|
| F1 | jnats `Options.Builder`: `.token(char[])`, `.userInfo(char[],char[])`, `.authHandler(AuthHandler)`, `.connectionListener(...)`, `.servers(String[])`. | `impl/jvm.clj:133-143,162-167` | `Options.java:1830` `token(char[])`, `:1803` `userInfo(char[],char[])`, `:1854` `authHandler(AuthHandler)`, `:1899` `connectionListener(...)`, `:1181` `servers(String[])`. | **GROUNDED** | — |
| F2 | `Nats.staticCredentials(char[] jwt, char[] seed)` & `(byte[] creds)` → `AuthHandler`; `NKey.fromSeed(char[])` → `getPublicKey()`→`char[]`, `sign(byte[])`; `AuthHandler` = sign/getID(char[])/getJWT. | `impl/jvm.clj:108-121,140-143` | `Nats.java:327` `staticCredentials(byte[])`, `:339` `staticCredentials(char[] jwt, char[] nkey)`; `NKey.java:459` `fromSeed(char[])`, `:579` `getPublicKey()→char[]`, `:644` `sign(byte[])`; `AuthHandler.java:62/70/79`. | **GROUNDED** | `getPublicKey()` returns `char[]`; our `(String. (.getPublicKey nk))` matches. (Minor: `NKey.getPublicKey/sign` declare checked exceptions.) |
| F3 | nats-core exports `nkeyAuthenticator(seed)`, `jwtAuthenticator(jwt,seed)`, `credsAuthenticator(creds)`, `nkeys.fromSeed`. | `impl/js.cljs:91-126` | `authenticator.js:71/91/106` (exported `:5-9`); `nkeys.js:37` re-exports `@nats-io/nkeys@2.0.3`; `nkeys.d.ts:56` `fromSeed(src: Uint8Array)`. | **GROUNDED** | Both legs offer token/userpass/nkey/jwt/creds. |
| F4 | nats-core options take `:token`, `:user`/`:pass`, `:authenticator`. | `impl/js.cljs:116-126` | `core.d.ts:628` `ConnectionOptions` (auth fields); `mod.d.ts` `Authenticator`. `:authenticator` grounded; the exact `:token`/`:user`/`:pass` keys not individually cited. | **GROUNDED (`:authenticator`) / ASSUMED (token/userpass keys)** | Low risk; the authenticator path (nkey/jwt/creds) is the grounded one. |
| F5 | A mismatched nkey/seed fails fast client-side as `{:type :auth-invalid}`. | `impl/jvm.clj:108-121`; `impl/js.cljs:91-102`; test `auth-with-mismatched-nkey-rejects` | Wrapper construct. `:auth-invalid` is **not** in ADR 0006's canonical `:type` set. | **GROUNDED (construct); internal drift** | Same behavior both legs; `:auth-invalid` is uncanonical (→ §H4). |

---

## G. Reconnect

| # | Premise | Our code | Library citation @ version | Class | Cross-leg note |
|---|---|---|---|---|---|
| G1 | jnats: `maxReconnects(int)` `0`=off, `-1`=unlimited, default **60**; `reconnectWait(Duration)`; `reconnectJitter(Duration)`. | `impl/jvm.clj:123-131` | `Options.java:91` `DEFAULT_MAX_RECONNECT = 60`; `:1546` `maxReconnects(int)` javadoc "0 to turn off … -1 … infinite"; `:1561` `reconnectWait(Duration)`; `:1573` `reconnectJitter(Duration)`. | **GROUNDED** | — |
| G2 | nats-core: disable with `reconnect:false` (NOT `maxReconnectAttempts:0`, which keeps reconnecting); `maxReconnectAttempts` default **10**, `-1`=unlimited; `reconnectTimeWait`; `reconnectJitter`. | `impl/js.cljs:104-114` | `options.js:29` `DEFAULT_MAX_RECONNECT_ATTEMPTS = 10`, `:44` `reconnect: true`; `protocol.js:428` `if (this.options.reconnect) { … }` (the disable switch); `:568-569` `const mra = maxReconnectAttempts \|\| 0; if (mra !== -1 && srv.reconnects >= mra) removeCurrentServer()`. | **GROUNDED** | Confirmed: `maxReconnectAttempts:0` does **not** cleanly disable (`0\|\|0→0`, and `reconnects≥0` removes the server); only `reconnect:false` disables. The wrapper gets this right — a genuine cross-leg foot-gun correctly handled. |
| G3 | Defaults differ (JVM 60, JS 10), so omitting `:max` is not portable. | `docs/adr/0006`, `CONTEXT.md:39` | G1 (`60`) + G2 (`10`). | **GROUNDED** | Accepted, documented divergence; not normalized by design. |

---

## H. Errors & connect

| # | Premise | Our code | Library citation @ version | Class | Cross-leg note |
|---|---|---|---|---|---|
| H1 | `connect` failure rejects with `{:type :connect-failed :servers …}`; only the server-side connect is wrapped (client-side auth errors keep their own `ex-info`). | `impl/jvm.clj:145-173`; `impl/js.cljs:128-154` | JVM: `Nats.connect(opts)` throws on failure (caught `:170-172`). JS: `wsconnect(...).catch` (`:149-152`). | **GROUNDED** | Both reject; both build options *inside* the async body so a client-side auth throw rejects rather than throwing synchronously. Symmetric. |
| H2 | `connect` returns the platform-native promise: `CompletableFuture/supplyAsync` (JVM) / `js/Promise` from `wsconnect` (JS). | `impl/jvm.clj:150`; `impl/js.cljs:138` | JVM: `CompletableFuture` (JDK). JS: nats-core 3.3.1 `ws_transport.js:301` `function wsconnect(opts) { … return NatsConnectionImpl.connect(opts) }` (→ `Promise<NatsConnection>`), exported via `internal_mod.js:131` / `mod.js:58` / `mod.d.ts:1`. | **GROUNDED** | `wsconnect` is a first-class nats-core export; the ws transport ships *in-package* (only `@nats-io/nkeys`+`@nats-io/nuid` are deps; Node-TCP `connect` lives in a separate `@nats-io/transport-node`). (Corrects an early false lead — Appendix.) |
| H3 | The canonical error `:type` set is the public contract. | `docs/adr/0006`; only `:connect-failed` & `:codec-error` are actually produced | ADR 0006. Most `:type`s have no producing code yet (no request → no `:timeout`/`:no-responders`; no async sink → no `:protocol-error`). | **GROUNDED (documented); mostly unshipped** | The error model is largely aspirational this phase: the async-failure sink (`:on-status :error` / per-sub `:on-error`) is **not wired** — throwing handlers are *swallowed* (A10), not routed. |
| H4 | `:auth-invalid` is the nkey/seed-mismatch type. | `impl/jvm.clj:116`; `impl/js.cljs:100` | ADR 0006 canonical set — `:auth-invalid` is **absent**. | **CONTRADICTED (internal drift)** | Not a library miss; the code emits a `:type` the error-model ADR doesn't list. Reconcile. |

---

## I. Protocol-level & intended-scope premises

| # | Premise | Where it lives | Library citation | Class |
|---|---|---|---|---|
| I1 | Core NATS delivers a single publisher's messages to a subscription in publish order. | A8 / ordering test | docs.nats.io/reference/faq: "messages from a given single publisher will be delivered … in the order in which they were originally published"; "not across different publishers." | **GROUNDED** (per-publisher; see A8 caveat). |
| I2 | Core NATS is **at-most-once**, best-effort (slow/dead subscriber drops; no persistence). | backdrop to A7 | docs.nats.io/nats-concepts/core-nats: "Core NATS provides best-effort, at-most-once message delivery." | **GROUNDED.** |
| I3 | The **server** has its own slow-consumer cutoff (`write_deadline`) that drops the *whole connection* — not per-subscription backpressure, and no help against an unbounded *client-side* handler backlog. | A7 (is there any floor?) | docs.nats.io/.../slow_consumers: "the server will disconnect the connection with the slow consumer"; `write_deadline` (default 10s). | **GROUNDED.** Decisive for A7: server protects itself, not the client heap; a client reading the socket promptly but queueing into an unbounded in-process backlog is never cut off. |
| I4 | `request` to a subject with no subscribers returns 503 → `:no-responders` (≠ `:timeout`), requiring server+client header support. | ADR 0006; **request unshipped** | docs.nats.io/.../reqreply: "opt-into no_responder … requires a server and client that support headers … `503` status"; ADR-40. nats-core surfaces `NoRespondersError` (`mod.d.ts`); `request(...): Promise<Msg>` exists (`core.d.ts:325`). | **GROUNDED — intended scope.** Cross-leg hazard for the request ticket: nats-core surfaces `NoRespondersError`; jnats reports it differently — normalization needed. |
| I5 | Queue groups load-balance so exactly one (randomly chosen) member gets each message. | ADR/CONTEXT; **queue groups unshipped** | docs.nats.io/.../queue: "only one randomly chosen subscriber of the queue group will consume a message each time." | **GROUNDED — intended scope.** Docs neither affirm nor deny cross-member ordering. Not in the protocol/facade yet (`-subscribe` takes no `:queue`). |

---

## Prioritized follow-ups

**P0 — drives the planned native-consumption rework (Exhibit A), now fully grounded:**
1. **A7 — unbounded, unmonitored dispatch backlog on BOTH legs.** GROUNDED to source that the
   native slow-consumer cannot fire under the current design (handler returns immediately, native
   buffer stays empty), that callback subs opt out of nats-core's iterator-based slow detection,
   and that the *server* cutoff (I3) is no floor for a client-side backlog. The wrapper's `tail`
   chain grows without bound or signal. Any rework must bound where the backlog actually is (the
   wrapper chain) or move to a model the native client can bound: **JVM** — block the dispatcher
   thread inside `onMessage` until the wrapper future settles, so jnats' pending-limits (512K/64MB)
   and `slowConsumerDetected` engage; **CLJS** — use the async-iterator + awaited handler, where
   nats-core buffers and signals (`slow?`/`slowConsumer`). *Correction for whoever reworks this:*
   jnats' default pending limit is **512K msgs / 64 MB** (`Consumer.DEFAULT_MAX_MESSAGES/_BYTES`),
   not 65536.

**P1 — internal-contract drift (cheap, no library dependency):**
2. **H4 — `:auth-invalid` is not in ADR 0006's canonical `:type` set.** Add it or rename.
3. **H3 — the error model is mostly unshipped:** throwing/rejecting handlers are *swallowed*
   (A10), not routed to `:on-status :error` / per-sub `:on-error` as ADR 0006 promises. Ship the
   sink or soften the ADR until the error-model slice lands.
4. **§0 — scope drift:** headers, request/reply, queue groups, per-call codec override, and the
   codec registry are described as Phase-1 surfaces but are not in `src/`. Keep the scope statement
   honest in docs/tickets.

**P2 — small clarifications:**
5. **A8 — ordering contract framing.** ADR 0007 says "within one subscription, in order"; the
   documented guarantee is per-*publisher*. State that multiple concurrent publishers to one subject
   have no cross-publisher order.
6. **E6 — status cadence claims** (`DISCOVERED_SERVERS` only on new gossip vs `update` on every
   INFO) are ADR assertions, not re-grounded to source; fine to leave as documented divergence.
7. **F4 — nats-core `:token`/`:user`/`:pass` option keys** not individually grounded (authenticator
   path is). Confirm opportunistically.

---

## What embedded knowledge got wrong

1. **The biggest one — "the native client's slow-consumer will catch a runaway handler."** The
   design returns from the handler immediately (composing a future) on the assumption that the
   native pending-limits / slow-consumer machinery still sits underneath as a floor. Grounding to
   both clients' source shows it does **not**: the handler return is `void`/ignored on both legs, so
   it can't backpressure them, and returning instantly keeps the native buffer empty so the native
   limit never trips (JVM) / the callback sub opts out of slow detection entirely (CLJS). The
   *server's* cutoff is no floor either (it drops the whole connection only when the socket backs
   up). The pieces of knowledge — *jnats has pending limits*, *nats.js has a slow consumer*, *the
   server cuts off slow consumers* — were each individually correct; the inference that any of them
   would protect *this* dispatch shape was wrong.

2. **A wrong constant rode along with the right intuition.** Where a default pending limit is
   discussed, the real jnats 2.25.3 value is **512K msgs / 64 MB**, not the oft-cited 65536. Small,
   but it's the kind of remembered number that silently mis-sizes a rework.

3. **Conflating "the contract works" with "the platform enforces it."** Promise-return backpressure
   is faithful on both legs — but entirely hand-rolled; neither client contributes. Fine, *as long
   as* the team knows there is no floor under it (point 1). The code comments actually say this; the
   embedded gap was building as if the floor existed anyway.

4. **Per-client status/reconnect details were carried as cross-platform facts.** Most resolved in
   the wrapper's favor once grounded — the six status strings are all valid 3.3.1 literals (E4), no
   initial connect event exists (E5), and `maxReconnectAttempts:0` really doesn't disable nats-core
   reconnect (G2, a real foot-gun the wrapper handles). The lesson is that these were *assumed*
   correct rather than read from the 3.3.1 contract; they happened to hold.

5. **Two confident structural "facts" were false on inspection** (Appendix: `wsconnect` "missing";
   `subscribe` "deref'd") — even structural claims need grounding, not recall. Both came from a
   stale-shell artifact, which is itself the point: verify against the artifact.

6. **Process note:** the firmest grounding came from the artifacts themselves — jnats source at tag
   2.25.3 + live reflection, nats-core's shipped `lib/`, and docs.nats.io — not from memory.

---

## Appendix — corrected false leads & method notes

**Corrected false leads (recorded so they are not re-derived):**
- ❌ *"`wsconnect` is missing from `@nats-io/nats-core@3.3.1`."* **False.** `ws_transport.js:301`
  defines and exports `wsconnect`; re-exported via `internal_mod`/`mod` (`mod.d.ts:1`). The ws
  transport ships in-package. The false negative came from a frozen shell echoing a stale
  `cd …/src` failure (the package ships `lib/`, not `src/`). **H2 GROUNDED.**
- ❌ *"`subscribe`'s result is `@`-deref'd on a `Dispatcher` (not `IDeref`), so JVM tests can't
  pass."* **False.** `Dispatcher.subscribe(String, MessageHandler)` returns a `Subscription`
  (reflection + `Dispatcher.java`), and the tests call `(nats/subscribe …)` **without** `@` — only
  the one-shots `connect`/`flush`/`drain`/`close` are deref'd. **B4 GROUNDED.**

**Method notes:**
- jnats ships as a jar (no local source); grounded against `nats-io/nats.java` tag `2.25.3` source
  (file:line above) cross-checked with live reflection on the loaded artifact.
- nats-core 3.3.1 ships compiled `lib/*.{js,d.ts}` (no `src/`); these are the authoritative shipped
  artifacts and all nats-core citations point at `node_modules/@nats-io/nats-core/lib/`.
- nats-core 3.3.1's status model is a discriminated union of `type` string literals (`core.d.ts:1-43`);
  the older `Events`/`DebugEvents` enums are gone — keep that in mind when reading other versions.
