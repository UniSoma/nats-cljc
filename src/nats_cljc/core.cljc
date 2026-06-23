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
  "Current library version."
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

   | `:auth` map | Method |
   | --- | --- |
   | `{:token \"...\"}` | token auth |
   | `{:user \"...\" :pass \"...\"}` | user/password auth |
   | `{:nkey \"...\" :seed \"...\"}` | nkey auth — `:seed` signs server nonces; a `:nkey` that does not match its `:seed` rejects with `:auth-invalid` |
   | `{:jwt \"...\" :seed \"...\"}` | JWT auth — user JWT plus the nkey `:seed` that signs |
   | `{:creds \"...\"}` | the CONTENTS of a NATS credentials (.creds) file as a string (not a path) |

   Rejects with `:type :connect-failed` when the server-side connect fails, or
   `:type :auth-invalid` when client-side credential validation fails before any
   dial. CONTEXT.md (Connection, Reconnect, Status event) and ADR 0001/0002 are
   supplemental rationale, not required to call this.

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
  "Publish `data` to `subject` on `conn`, encoding it with the connection's codec.
   Fire-and-forget: returns nil (ADR 0002). `opts` may set `:codec` to override the
   connection's default codec for this call (ADR 0011), and `:headers`, a map of
   case-sensitive string names to one or more string values; a scalar value is
   normalized to a one-element vector (CONTEXT: Headers). Header names and values
   must be strings, and names must be valid header tokens (printable ASCII, no
   colon); invalid input throws."
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
  "Subscribe to `subject`, returning a Subscription synchronously. `handler` is
   invoked once per message with `{:subject :data :reply}`, where `:data` is
   decoded with the connection's codec and `:reply` is the message's reply-to
   subject (nil when absent), which `reply` answers (ADR 0007). `opts` may set
   `:codec` to override the connection's default codec for decoding (ADR 0011),
   and `:queue`: subscriptions sharing a queue-group name compete, so the server
   load-balances each matching message to exactly one member of the group. A nil
   or blank `:queue` is a plain subscription (normalized here so both platforms
   agree, rather than relying on each native layer's truthiness).

   `opts` may also set the async-failure sink and overflow threshold (ADR 0006):
   `:on-error`, a 1-arg fn receiving this subscription's failures as a bare
   `ex-info` — a thrown handler value (unchanged, no canonical `:type`), a decode
   failure (`:codec-error`), or `:slow-consumer` — and `:max-pending`, a positive
   message-count threshold above which `:slow-consumer` fires. With no `:on-error`,
   a thrown-handler/decode failure falls back to the connection's `:on-status`
   `:error` event and `:slow-consumer` is dropped; the override is strict (never
   both). A decode failure is caught here, so the handler never sees garbage.
   A non-positive `:max-pending` throws a `:type :invalid-max-pending` ex-info."
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
  "Send a request to `subject` on `conn`, encoding `data` with the connection's
   codec, and return a platform-native promise that resolves to the decoded reply
   message `{:subject :data :reply}` (ADR 0002). `opts` may set `:codec` to
   override the connection's default codec for both the request encode and the
   reply decode (ADR 0011), and `:timeout-ms` (default 5000). The promise rejects
   with an `ex-info` whose `:type` is `:no-responders` (nobody subscribes
   `subject`) or `:timeout` (responders exist but none answer within
   `:timeout-ms`) (ADR 0006)."
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
  "Reply to a request message `msg` with `data`, encoding it with the connection's
   codec and publishing to the request's `:reply` subject. Sugar over `publish`;
   returns nil (ADR 0002). `opts` may set `:codec` to override the connection
   default, so a polyglot response can match the request's codec (ADR 0011).
   Throws an `ex-info` `:type :no-reply-subject` when `msg` has no `:reply` (e.g. a
   plain pub/sub message), rather than publishing to a nil subject."
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
   lower-level sibling of `drain`, which flushes the backlog first and is
   awaitable (ADR 0002/0012). Idempotent: unsubscribing an already-ended
   subscription (a prior `unsubscribe`, a `drain`, or the connection closing) is a
   silent no-op rather than an error (ADR 0012).

   With `max`, the subscription auto-unsubscribes once it has received that many
   messages over its lifetime — counted from subscription start, so messages
   already delivered past the limit are never recalled, and if it has already
   received `max` it stops now. `max` must be a positive integer no greater than
   2147483647 (the JVM `Dispatcher.unsubscribe(sub, int)` cap, enforced on both
   platforms so the contract is portable); anything else throws a `:type
   :invalid-max` ex-info."
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
  "Flush `conn`, returning a platform-native promise that settles once the server
   has processed everything buffered on the connection (ADR 0002)."
  [conn]
  (proto/-flush conn))

(defn drain
  "Drain a connection or a single subscription, returning a platform-native
   promise that settles once draining completes. For a connection, it stops all
   the connection's subscriptions after their pending messages are delivered,
   then closes the connection; for a subscription, it ends just that one and
   leaves the connection open (ADR 0002)."
  [conn-or-sub]
  (proto/-drain conn-or-sub))

(defn close
  "Close `conn`, returning a platform-native promise that settles once the
   connection is fully closed. Closing ends all of the connection's
   subscriptions; a final `:closed` status reaches `:on-status` (ADR 0002/0006)."
  [conn]
  (proto/-close conn))

(defn subject
  "Compose the canonical dot-delimited Subject string from `parts`, each
   stringified and joined with `.` — e.g. (subject \"orders\" id \"created\")
   => \"orders.<id>.created\". The string form is canonical; this is just sugar
   for building one from its tokens (CONTEXT: Subject)."
  [& parts]
  (str/join "." parts))
