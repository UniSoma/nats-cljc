(ns nats-cljc.core
  "Portable public facade for nats-cljc (always aliased `nats`).

   A thin `.cljc` surface over the internal protocol (ADR 0005): it owns codec
   encode/decode and ergonomics, delegating primitive operations to the platform
   Connection record. The same consumer code compiles and runs on the JVM, the
   browser, and Node."
  ;; `flush` is part of the public verb surface, shadowing clojure.core/flush
  ;; (and cljs.core/flush) here on purpose.
  (:refer-clojure :exclude [flush])
  (:require [clojure.string :as str]
            [nats-cljc.codec :as codec]
            [nats-cljc.impl.msg :as msg]
            [nats-cljc.impl.protocol :as proto]
            #?(:clj  [nats-cljc.impl.jvm :as impl]
               :cljs [nats-cljc.impl.js :as impl])))

(def version
  "The library's version string, e.g. `\"0.5.0\"`.

   A plain `def` holding a string — `nats-cljc.core/version` evaluates to the
   value directly (it is not a function; do not call it)."
  "0.5.0")

(defn connect
  "Open a connection to a NATS server and return a platform-native promise
   (CompletableFuture on the JVM, js/Promise on ClojureScript) that resolves to a
   Connection — the value every [[publish]]/[[subscribe]]/[[request]] flows through. Transport
   is fixed per platform: TCP on the JVM, WebSocket on ClojureScript, so `:servers`
   URLs use the matching scheme (ADR 0001).

   `opts` is a map; every key is optional except `:servers`:

   - `:servers` — vector of server URL strings: `[\"nats://127.0.0.1:4222\"]` on the
     JVM, `[\"ws://127.0.0.1:8080\"]` on ClojureScript. A bare string is accepted and
     normalized to a one-element vector.
   - `:codec` — the connection's default codec keyword: encodes published data and
     decodes deliveries. Built-ins `:edn` (default), `:json`, `:transit`; any op
     takes a per-call `:codec` override (ADR 0011). An unresolvable codec rejects
     this promise.
   - `:name` — string label for the connection, shown in server monitoring.
   - `:auth` — credentials map selecting exactly one auth method by its
     discriminating key (omit for an anonymous connection); see the table below.
   - `:reconnect` — map `{:max <int> :wait-ms <ms> :jitter-ms <ms>}` tuning
     automatic reconnection. `:max` is the attempt count with two sentinels — `0`
     disables reconnection, `-1` is unlimited; `:wait-ms` is the per-attempt delay
     and `:jitter-ms` its random spread. Any absent key keeps the underlying
     client's own default, and those differ (JVM 60 attempts, Node/browser 10), so
     omitting `:max` is NOT identical across platforms. A `:max` outside
     `[-1, 2147483647]` throws `:type :invalid-max`.
   - `:on-status` — 1-arg fn receiving connection-lifecycle Status events as bare
     `{:type ...}` maps. Canonical `:type`s: `:connected`, `:disconnected`,
     `:reconnecting`, `:reconnected`, `:closed`, `:error`, `:lame-duck`,
     `:servers-changed`; the `:error` event carries the offending Error under
     `:error`. React to each `:type` as an edge, not a counter — cadence differs
     per platform.

   The `:auth` map selects one method by its discriminating key:

   | `:auth` map                     | Method                                                                                                          |
   |---------------------------------|-----------------------------------------------------------------------------------------------------------------|
   | `{:token \"...\"}`              | token auth                                                                                                      |
   | `{:user \"...\" :pass \"...\"}` | user/password auth                                                                                              |
   | `{:nkey \"...\" :seed \"...\"}` | nkey auth — `:seed` signs server nonces; a `:nkey` that does not match its `:seed` rejects with `:auth-invalid` |
   | `{:jwt \"...\" :seed \"...\"}`  | JWT auth — user JWT plus the nkey `:seed` that signs                                                            |
   | `{:creds \"...\"}`              | the CONTENTS of a NATS credentials (.creds) file as a string (not a path)                                       |

   Rejects with `:type :connect-failed` when the server-side connect fails,
   `:type :auth-invalid` when client-side credential validation fails before any
   dial, or `:type :codec-error` when `:codec` cannot be resolved. CONTEXT.md
   (Connection, Reconnect, Status event) and ADR 0001/0002 are supplemental
   rationale, not required to call this.

   Example (JVM; on ClojureScript await the promise instead of deref):

   ```clojure
   @(connect {:servers   [\"nats://127.0.0.1:4222\"]
              :codec     :json
              :name      \"orders-service\"
              :auth      {:creds (slurp \"app.creds\")}
              :reconnect {:max -1 :wait-ms 250}
              :on-status #(println :status (:type %))})
   ```"
  [opts]
  ;; Resolve the default codec once here (ADR 0011) and store the resolved
  ;; `Prepared` on the connection, so steady-state encode/decode never deref the
  ;; codec registry. Done in an `impl/then` stage so an unresolvable default codec
  ;; rejects the connect promise — matching connect's contract (ADR 0002/0006) —
  ;; rather than throwing synchronously or surfacing lazily on first publish.
  (impl/then (impl/connect opts)
    (fn [conn] (assoc conn :codec (codec/prepare (:codec conn))))))

(defn publish
  "Publish `data` to `subject` on `conn`, encoding `data` with the connection's
   codec. Fire-and-forget.

   - `conn` — a Connection (from [[connect]]).
   - `subject` — the destination subject string.
   - `data` — any value the active codec can encode.
   - `opts` (optional map):

   | key        | type                 | default        | effect                                                                                                                                                              |
   |------------|----------------------|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
   | `:codec`   | keyword or `ICodec`  | conn's `:codec`| Codec for this call only, overriding the connection default (ADR 0011). Built-ins `:edn`, `:json`, `:transit`.                                                      |
   | `:headers` | map                  | none           | Message headers `{name -> value}`, names case-sensitive strings, each value a string or a vector of strings (a scalar is wrapped to a one-element vector).          |

   Returns nil (ADR 0002).

   Throws synchronously (publish is fire-and-forget — there is no promise to
   reject):
   - `:type :invalid-header` if a header name or value is not a string, a name is
     not a printable-ASCII token without a colon, or a value contains CR/LF or a
     non-ASCII char.
   - `:type :codec-error` if encoding `data` fails (ADR 0006/0011).
   - `:type :max-payload-exceeded` if the encoded payload exceeds the server's max.
   - `:type :connection-closed` if `conn` is closed.

   Best-effort during drain: a publish issued while the connection is draining is
   silently dropped, returning nil rather than throwing `:drained`, on both
   platforms (ADR 0014). Each `:type` above is the `:type` key of the thrown
   `ex-info`'s `ex-data`.

   ```clojure
   (publish conn \"orders.created\" {:id 7} {:headers {\"X-Trace\" \"abc\"}})
   ```"
  ([conn subject data] (publish conn subject data {}))
  ([conn subject data {:keys [headers] :as opts}]
   (proto/-publish conn subject (msg/normalize-headers headers)
                   (codec/encode (msg/effective-codec conn opts) data))
   nil))

(defn- decode-msg
  "Decode a raw delivery/reply map `{:subject :bytes :reply :headers}` into the
   public message shape `{:subject :data :reply}`, decoding `:bytes` with
   `codec`. `:headers` (canonical `{name -> vector-of-strings}`) is added only
   when the message carried some, so it is absent otherwise.

   This is where the portable header-value contract is enforced: values are
   trimmed by `trim-headers` (surrounding whitespace is insignificant), and an
   empty map is dropped so `:headers` stays absent regardless of any platform
   quirk in producing it (CONTEXT: Headers)."
  [codec {:keys [subject bytes reply headers]}]
  (cond-> {:subject subject
           :reply   reply
           :data    (codec/decode codec bytes)}
    (seq headers) (assoc :headers (msg/trim-headers headers))))

(defn subscribe
  "Subscribe to `subject` on `conn`, returning a Subscription synchronously.

   - `conn` — a Connection (from [[connect]]).
   - `subject` — the subject string to subscribe to (may contain NATS wildcards).
   - `handler` — a 1-arg fn invoked once per message with the decoded message map
     `{:subject <string> :data <decoded> :reply <string-or-nil>}`, where `:data`
     is decoded with the effective codec and `:reply` is the message's reply-to
     subject (nil when absent), which [[reply]] answers. The handler runs
     serially, one message at a time; returning a promise applies backpressure
     (ADR 0007).
   - `opts` (optional map):

   | key            | type                | default         | effect                                                                                                                                                     |
   |----------------|---------------------|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
   | `:codec`       | keyword or `ICodec` | conn's `:codec` | Codec for decoding deliveries on this subscription only (ADR 0011).                                                                                        |
   | `:queue`       | string              | none            | Queue-group name: subscriptions sharing it compete, so the server delivers each matching message to exactly one member. nil/blank is a plain subscription. |
   | `:on-error`    | 1-arg fn            | none            | Async-failure sink for this subscription (see below).                                                                                                      |
   | `:max-pending` | positive int        | native default  | Pending-message threshold above which `:slow-consumer` fires; only armed when `:on-error` is also set.                                                     |

   Returns a Subscription; pass it to [[unsubscribe]], [[drain]], or test it as
   the `conn-or-sub` arg of [[drain]].

   Async failures are never thrown from this call; they route to exactly one sink
   (strict — never both). The failures: a thrown handler value (passed through
   unchanged, no canonical `:type`), a decode failure (`:type :codec-error` —
   caught here so the handler never sees garbage), or `:type :slow-consumer`.

   - With `:on-error` set, it receives the **bare `ex-info`** for thrown-handler
     and decode failures — read the classification with `(:type (ex-data e))` —
     and is the only sink that ever sees `:slow-consumer`.
   - With no `:on-error`, a thrown-handler / decode failure instead reaches the
     connection's `:on-status` as an `:error` event, which **wraps** the same
     `ex-info` as `{:type :error :error <ex-info>}` (so `:on-status` dispatch on
     `(:type ev)` stays uniform); `:slow-consumer` is dropped in this case.

   The native slow-consumer detector engages only when both `:on-error` and
   `:max-pending` are set. See (ADR 0006) for the full routing rationale.

   Throws synchronously `:type :invalid-max-pending` if `:max-pending` is present
   and not a positive integer (the `:type` key of the thrown `ex-info`'s
   `ex-data`).

   ```clojure
   (subscribe conn \"greet.*\" prn {:queue \"workers\" :max-pending 1000
                                   :on-error #(println :sub-error (ex-message %))})
   ```"
  ([conn subject handler] (subscribe conn subject handler {}))
  ([conn subject handler {:keys [queue on-error max-pending] :as opts}]
   ;; Fail fast on caller misuse: a 0/negative/non-int threshold would otherwise
   ;; arm a zero (or sentinel-unbounded) native cap and silently deafen the sub.
   (when (and (some? max-pending) (not (pos-int? max-pending)))
     (throw (ex-info "subscribe :max-pending must be a positive integer"
                     {:type :invalid-max-pending :max-pending max-pending})))
   (let [codec (msg/effective-codec conn opts)]
     (proto/-subscribe conn subject (when-not (str/blank? queue) queue)
                       {:on-error on-error :max-pending max-pending}
                       (fn [raw] (handler (decode-msg codec raw)))))))

(defn request
  "Send a request to `subject` on `conn`, encoding `data` with the effective
   codec, and return a platform-native promise (`CompletableFuture` on the JVM,
   `js/Promise` on ClojureScript) that resolves to the decoded reply message
   `{:subject <string> :data <decoded> :reply <string-or-nil>}` (ADR 0002).

   - `conn` — a Connection (from [[connect]]).
   - `subject` — the subject to send the request to.
   - `data` — any value the effective codec can encode.
   - `opts` (optional map):

   | key           | type                | default         | effect                                                                                          |
   |---------------|---------------------|-----------------|-------------------------------------------------------------------------------------------------|
   | `:codec`      | keyword or `ICodec` | conn's `:codec` | Codec for both the request encode and the reply decode on this call only (ADR 0011).            |
   | `:timeout-ms` | int (milliseconds)  | `5000`          | How long to wait for a reply before rejecting with `:timeout`.                                  |

   The promise rejects with an `ex-info` whose `ex-data` `:type` is one of (ADR 0006):
   - `:no-responders` — nobody is subscribed to `subject`.
   - `:timeout` — responders exist but none answered within `:timeout-ms`.
   - `:codec-error` — encoding `data` or decoding the reply failed (ADR 0011).
   - `:max-payload-exceeded` — the encoded request exceeds the server's max payload.
   - `:drained` — the connection is draining.
   - `:connection-closed` — the connection is closed.

   Encode and reply-decode failures reject the returned promise rather than
   throwing at the call site.

   ```clojure
   @(request conn \"time.now\" nil {:timeout-ms 1000})
   ```"
  ([conn subject data] (request conn subject data {}))
  ([conn subject data opts]
   ;; Encode and decode are BOTH `then` stages and `-request` is the lone
   ;; flattening `bind`, so an encode failure rejects the returned promise exactly
   ;; as a reply-decode failure already does — never a synchronous throw at the
   ;; call site (ADR 0006). `effective-codec` is a pure `or` (can't throw), so the
   ;; encode is the only sync-throw site, and seeding from a resolved promise turns
   ;; its throw into a rejection.
   (let [codec (msg/effective-codec conn opts)]
     (-> (impl/resolved nil)
         (impl/then (fn [_]     (codec/encode codec data)))
         (impl/bind (fn [bytes] (proto/-request conn subject bytes (:timeout-ms opts 5000))))
         (impl/then (fn [raw]   (decode-msg codec raw)))))))

(defn reply
  "Reply to a request message `msg` with `data`, encoding `data` with the
   effective codec and publishing it to the request's `:reply` subject. Sugar over
   [[publish]].

   - `conn` — a Connection (from [[connect]]).
   - `msg` — the message map a [[subscribe]] `handler` received; its `:reply` key
     names the subject to answer on (nil for a plain pub/sub message).
   - `data` — any value the effective codec can encode; the reply payload.
   - `opts` (optional map):

   | key      | type                | default         | effect                                                                                                           |
   |----------|---------------------|-----------------|------------------------------------------------------------------------------------------------------------------|
   | `:codec` | keyword or `ICodec` | conn's `:codec` | Codec for encoding the reply on this call only, so a polyglot response can match the request's codec (ADR 0011). |

   Returns nil (ADR 0002).

   Throws synchronously:
   - `:type :no-reply-subject` when `msg` has no `:reply` (e.g. a plain pub/sub
     message), rather than publishing to a nil subject.
   - `:type :codec-error` if encoding `data` fails (ADR 0006/0011).
   - `:type :max-payload-exceeded` if the encoded reply exceeds the server's max.
   - `:type :connection-closed` if `conn` is closed.

   Each `:type` is the `:type` key of the thrown `ex-info`'s `ex-data`.

   ```clojure
   (subscribe conn \"time.now\" (fn [msg] (reply conn msg (System/currentTimeMillis))))
   ```"
  ([conn msg data] (reply conn msg data {}))
  ([conn msg data opts]
   (if-let [reply-subject (:reply msg)]
     (do (proto/-publish conn reply-subject nil (codec/encode (msg/effective-codec conn opts) data))
         nil)
     (throw (ex-info "Message has no reply subject"
                     {:type :no-reply-subject :subject (:subject msg)})))))

(defn unsubscribe
  "End a single subscription `sub` abruptly, returning nil synchronously: the
   server is told to stop and any not-yet-delivered messages are dropped — the
   lower-level sibling of [[drain]], which flushes the backlog first and is
   awaitable (ADR 0002/0012).

   - `sub` — a Subscription (from [[subscribe]]).
   - `max` (optional) — a positive integer no greater than `2147483647`. With it,
     the subscription auto-unsubscribes once it has received that many messages
     over its lifetime (counted from subscription start, so messages already
     delivered past the limit are never recalled, and if it has already received
     `max` it stops now). The `2147483647` cap is the JVM `Dispatcher.unsubscribe`
     `int` limit, enforced on both platforms so the contract is portable.

   Returns nil.

   Idempotent: unsubscribing an already-ended subscription (a prior
   [[unsubscribe]], a [[drain]], or the connection closing) is a silent no-op, not
   an error (ADR 0012).

   Throws synchronously `:type :invalid-max` (the `:type` key of the thrown
   `ex-info`'s `ex-data`) when `max` is given and is not a positive integer
   `<= 2147483647`.

   ```clojure
   (unsubscribe sub 10)   ;; stop after 10 lifetime messages
   ```"
  ([sub] (proto/-unsubscribe sub nil))
  ([sub max]
   ;; Bound by Integer/MAX_VALUE: the JVM native overload takes an `int`, and a
   ;; larger value would pass `pos-int?` only to throw an uncaught ArithmeticException
   ;; at `(int max)` there while succeeding on JS. Reject portably, before native.
   (when-not (and (pos-int? max) (<= max 2147483647))
     (throw (ex-info "unsubscribe max must be a positive integer no greater than 2147483647"
                     {:type :invalid-max :max max})))
   (proto/-unsubscribe sub max)))

(defn flush
  "Flush `conn`, returning a platform-native promise (`CompletableFuture` on the
   JVM, `js/Promise` on ClojureScript) that settles once the server has processed
   everything buffered on the connection (ADR 0002).

   - `conn` — a Connection (from [[connect]]).

   The promise resolves with no meaningful value once the buffer is acknowledged.

   Failure behavior: unlike [[request]], `flush` does NOT normalize connection
   op-state — it runs the native client's flush directly on both legs (JVM jnats
   `flush`, ClojureScript nats.js `flush`), so a flush on a draining/closed
   connection rejects with the **native client's own error passed through
   unchanged**, carrying NO canonical `:type` (do not match on `:type :drained` or
   `:type :connection-closed` here). The JVM leg additionally rejects if the native
   flush does not complete within the connection's op-timeout. (ADR 0006 lists
   `:drained` among one-shot ops, but this facade does not surface it for `flush`.)"
  [conn]
  (proto/-flush conn))

(defn drain
  "Drain a connection or a single subscription, returning a platform-native
   promise (`CompletableFuture` on the JVM, `js/Promise` on ClojureScript) that
   settles once draining completes (ADR 0002).

   - `conn-or-sub` — either a Connection (from [[connect]]) or a Subscription
     (from [[subscribe]]); the same fn dispatches over either.

   For a Connection: stops all its subscriptions after their pending messages are
   delivered, flushes, then closes the connection (a final `:closed` status
   reaches `:on-status`). For a Subscription: ends just that one after its pending
   messages drain, leaving the connection open. The graceful, awaitable
   counterpart to [[unsubscribe]] (subscription) and [[close]] (connection).

   Failure behavior: unlike [[request]], `drain` does NOT normalize connection
   op-state on either dispatch arm — both the Connection arm (JVM jnats
   `Connection.drain`, ClojureScript nats.js `drain`) and the Subscription arm
   (JVM `Subscription.drain`, ClojureScript `sub.drain`) run the native client's
   drain directly. So a `drain` on an already-draining/closed connection (or sub)
   rejects with the **native client's own error passed through unchanged**,
   carrying NO canonical `:type` (do not match on `:type :drained` /
   `:type :connection-closed` here). The JVM legs additionally reject if the native
   drain does not complete within the connection's op-timeout. (ADR 0006 lists
   `:drained` among one-shot ops, but this facade does not surface it for `drain`.)"
  [conn-or-sub]
  (proto/-drain conn-or-sub))

(defn close
  "Close `conn`, returning a platform-native promise (`CompletableFuture` on the
   JVM, `js/Promise` on ClojureScript) that settles once the connection is fully
   closed (ADR 0002/0006).

   - `conn` — a Connection (from [[connect]]).

   Closing ends all of the connection's subscriptions abruptly (unlike [[drain]],
   it does not wait for pending messages to be delivered first); a final `:closed`
   status reaches the connection's `:on-status`.

   Failure behavior: unlike [[request]], `close` does NOT normalize connection
   op-state — it runs the native client's close directly on both legs (JVM jnats
   `close`, ClojureScript nats.js `close`). The settle path carries NO canonical
   `:type`; do not match on `:type :drained` or `:type :connection-closed` here.
   Closing an already-closed connection is benign on both legs. (ADR 0006 lists
   `:drained` among one-shot ops, but this facade does not surface it for `close`.)"
  [conn]
  (proto/-close conn))

(defn subject
  "Compose the canonical dot-delimited subject string from `parts`.

   - `parts` — zero or more values (varargs); each is stringified (via `str`) and
     the results are joined with `.`.

   Returns the joined subject string (the empty string when called with no parts).
   Pure string sugar — it does not validate against NATS subject rules, and never
   throws.

   ```clojure
   (subject \"orders\" 7 \"created\")   ;; => \"orders.7.created\"
   ```"
  [& parts]
  (str/join "." parts))
