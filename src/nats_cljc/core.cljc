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
            [nats-cljc.protocol :as proto]
            #?(:clj  [nats-cljc.impl.jvm :as impl]
               :cljs [nats-cljc.impl.js :as impl])))

(def version
  "Current library version."
  "0.1.0-SNAPSHOT")

(defn connect
  "Open a connection to the NATS server(s) in `:servers`, returning a
   platform-native promise (CompletableFuture on the JVM, js/Promise on
   ClojureScript) that resolves to a Connection. The connection's default codec
   is `:codec` (default `:edn`). Transport is fixed per platform: TCP on the JVM,
   WebSocket on ClojureScript (ADR 0001)."
  [opts]
  (impl/connect opts))

(defn- normalize-headers
  "Normalize a user `:headers` map to the canonical portable form the protocol
   carries: `{name -> vector-of-strings}` with case-sensitive string names. A
   scalar value is wrapped in a one-element vector; a vector is kept as-is
   (CONTEXT: Headers). Returns nil for nil/empty input so `:headers` stays absent
   when none were set.

   Names and values must be strings: jnats and nats.js diverge on a non-string
   value (notably nil — jnats drops it and publishes headerless, nats.js throws),
   so we reject it here with a portable `:type :invalid-header` ex-info rather
   than leak that divergence. A set is treated as a scalar (its order is
   undefined); pass a vector for multiple values."
  [headers]
  (when (seq headers)
    (reduce-kv (fn [m k v]
                 (when-not (string? k)
                   (throw (ex-info "Header names must be strings"
                                   {:type :invalid-header :name k})))
                 (let [vs (if (sequential? v) (vec v) [v])]
                   (when-not (every? string? vs)
                     (throw (ex-info "Header values must be strings"
                                     {:type :invalid-header :name k :values vs})))
                   (assoc m k vs)))
               {} headers)))

(defn- effective-codec
  "The codec for a single call: a per-call `:codec` in `opts` overrides the
   connection default, else the connection's `:codec` (ADR 0011). The one place
   the precedence rule lives, so publish/subscribe/request/reply can't drift."
  [conn opts]
  (or (:codec opts) (:codec conn)))

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
   (proto/-publish conn subject (normalize-headers headers)
                   (codec/encode (effective-codec conn opts) data))
   nil))

(defn- decode-msg
  "Decode a raw delivery/reply map `{:subject :bytes :reply :headers}` into the
   public message shape `{:subject :data :reply}`, decoding `:bytes` with
   `codec`. `:headers` (canonical `{name -> vector-of-strings}`) is added only
   when the message carried some, so it is absent otherwise.

   This is where the portable header-value contract is enforced: surrounding
   whitespace is insignificant and stripped on delivery (nats.js already trims,
   so the JS leg double-trims harmlessly; jnats does not, so this is what makes
   the JVM leg agree), and an empty map is dropped so `:headers` stays absent
   regardless of any platform quirk in producing it (CONTEXT: Headers)."
  [codec {:keys [subject bytes reply headers]}]
  (cond-> {:subject subject
           :reply   reply
           :data    (codec/decode codec bytes)}
    (seq headers) (assoc :headers (reduce-kv (fn [m k vs] (assoc m k (mapv str/trim vs)))
                                             {} headers))))

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
   (let [codec (effective-codec conn opts)]
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
  [conn subject data opts]
  (let [codec (effective-codec conn opts)]
    (impl/then (proto/-request conn subject (codec/encode codec data) (:timeout-ms opts 5000))
               (fn [raw] (decode-msg codec raw)))))

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
     (do (proto/-publish conn reply-subject nil (codec/encode (effective-codec conn opts) data))
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
