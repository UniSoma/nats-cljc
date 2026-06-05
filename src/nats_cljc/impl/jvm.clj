(ns nats-cljc.impl.jvm
  "JVM platform implementation: a Connection record wrapping io.nats:jnats over
   TCP (ADR 0001/0003). All jnats interop is quarantined here (ADR 0005)."
  (:require [clojure.string :as str]
            [nats-cljc.auth :as auth]
            [nats-cljc.error :as error]
            [nats-cljc.protocol :as proto])
  (:import [io.nats.client Nats Options Options$Builder Connection Consumer Subscription Dispatcher MessageHandler Message AuthHandler NKey ConnectionListener ConnectionListener$Events ErrorListener]
           [io.nats.client.impl Headers]
           [java.nio.charset StandardCharsets]
           [java.time Duration]
           [java.util.concurrent CompletableFuture CompletionStage CompletionException ExecutionException CancellationException TimeoutException]
           [java.util.function Supplier Function BiFunction BiConsumer]))

;; Upper bound for the blocking flush/drain one-shots; a healthy connection
;; settles in milliseconds. Finite (not Duration.ZERO = wait-forever) so a dead
;; connection rejects rather than hangs; richer timeout handling is the
;; error-model slice's job.
(def ^:private ^Duration op-timeout (Duration/ofSeconds 10))

(defn- ->headers
  "Build a jnats Headers from the canonical portable map `{name -> [str ...]}`, or
   nil for none. Names are added verbatim (the Collection overload), so the
   case-sensitive names survive to the wire."
  [headers]
  (when headers
    (reduce-kv (fn [^Headers h ^String k vs] (.add h k ^java.util.Collection vs))
               (Headers.) headers)))

(defn- raw-headers
  "Extract a jnats Message's headers into the canonical portable map
   `{name -> [str ...]}`, or nil when it carries none. keySet preserves the
   case-sensitive names (ADR 0005)."
  [^Message msg]
  (when (.hasHeaders msg)
    (let [^Headers h (.getHeaders msg)]
      (reduce (fn [m ^String k] (assoc m k (vec (.get h k))))
              {} (.keySet h)))))

(defn- msg->raw
  "Lift a jnats Message into the raw map the facade decodes (ADR 0005): subject,
   wire bytes, the reply-to subject (nil when absent), and the headers map (nil
   when absent)."
  [^Message msg]
  {:subject (.getSubject msg)
   :bytes   (.getData msg)
   :reply   (.getReplyTo msg)
   :headers (raw-headers msg)})

(defn- unwrap-completion
  "Peel the wrapper CompletableFuture puts around a value thrown inside a stage: a
   CompletionException on the async-reject / blocked-CompletionStage paths, an
   ExecutionException from a blocking `.get`. Unwrap to the value actually thrown so
   a thrown handler value / typed ex-info passes through unchanged and a reader of
   `(ex-data e)` sees its `:type` (ADR 0006)."
  [e]
  (if (or (instance? CompletionException e) (instance? ExecutionException e))
    (or (.getCause ^Throwable e) e)
    e))

(defn- deliver-bare
  "Return a native promise that re-completes `p`'s rejection with the BARE ex-info.
   CompletableFuture wraps a value thrown inside a stage in a CompletionException
   (its .handle/.exceptionally/.whenComplete all surface that wrapper, whose own
   ex-data is nil), so a consumer reading `(:type (ex-data e))` — true on JS, where
   the native promise rejects with the bare value — would read nil on the JVM.
   `completeExceptionally` stores the cause unwrapped, so the async-reject seam
   matches JS and ADR 0006. (deref/`.get` still reports an ExecutionException —
   inherent to `.get`, peeled by the blocking layer — ADR 0008.)"
  [^CompletableFuture p]
  (let [out (CompletableFuture.)]
    (.whenComplete p
                   (reify BiConsumer
                     (accept [_ v e]
                       (if e
                         (.completeExceptionally out ^Throwable (unwrap-completion e))
                         (.complete out v)))))
    out))

(defn- route-error!
  "Route a caught async dispatch failure to its sink (ADR 0006): the per-sub
   `on-error` if set (the bare value), else the connection `on-status` as a
   non-bare `{:type :error :error e}` event; both nil drops it. Strict override —
   never both."
  [on-error on-status e]
  (if on-error
    (on-error e)
    (when on-status (on-status {:type :error :error e}))))

(defn- op-state-error
  "Normalize an IllegalStateException from a publish/request on a non-open
   connection by its jnats message (ADR 0006): the drain WINDOW (`Draining`) →
   `:drained` — a don't-retry signal — and a fully closed connection (`Closed`) →
   `:connection-closed`, which is retry-able. jnats keeps `getStatus` CONNECTED
   during drain, so the message — not the status — distinguishes the two.
   Unrecognized states return the original exception for the caller to rethrow."
  [subject ^Throwable e]
  (let [msg (str (.getMessage e))]
    (cond
      (str/includes? msg "Draining")
      (ex-info "Connection is draining" {:type :drained :subject subject} e)
      (str/includes? msg "Closed")
      (ex-info "Connection is closed" {:type :connection-closed :subject subject} e)
      :else e)))

(defn- max-payload-error
  "Normalize an IllegalArgumentException from a publish/request to a typed
   `:max-payload-exceeded` ex-info (ADR 0006) when `bytes` actually exceeds the
   connection's max payload, else return the original exception to rethrow. jnats
   throws IllegalArgumentException for an over-max payload AND for a bad subject,
   so discriminate by SIZE rather than blanket-mapping the type — keeping publish
   and request from diverging on what counts as over-max."
  [^Connection client subject ^bytes bytes ^Throwable e]
  (let [max (.getMaxPayload client)]
    (if (and (pos? max) (> (alength bytes) max))
      (ex-info "Message payload exceeds the server's max payload"
               {:type :max-payload-exceeded :subject subject :size (alength bytes) :max max} e)
      e)))

(defrecord JvmSubscription [^Subscription sub registry ^Dispatcher dispatcher]
  proto/Drainable
  ;; jnats sub.drain returns a CompletableFuture<Boolean> that settles once the
  ;; subscription's pending messages are delivered and it is removed — ending just
  ;; this subscription, leaving the connection open. Dissoc this dispatcher from
  ;; the slow-consumer registry so a drained sub leaks no sink (ADR 0006).
  (-drain [_] (swap! registry dissoc dispatcher) (.drain sub op-timeout))
  proto/Sub
  (-active? [_] (.isActive sub))
  ;; jnats subs are dispatcher-owned, so route teardown through the dispatcher,
  ;; never sub.unsubscribe(); dissoc the slow-consumer registry entry too (like
  ;; -drain) so an unsubscribed sub leaks no sink (ADR 0006). `max` nil stops now;
  ;; a positive int selects the unsubscribe(sub, after) overload that auto-stops
  ;; after that many lifetime messages. A closed dispatcher or already-removed sub
  ;; makes Dispatcher.unsubscribe throw IllegalStateException; swallow it to nil
  ;; for the idempotent no-op (ADR 0012). Returns nil either way.
  (-unsubscribe [_ max]
    (swap! registry dissoc dispatcher)
    (try
      (if max
        (.unsubscribe dispatcher sub (int max))
        (.unsubscribe dispatcher sub))
      (catch IllegalStateException _ nil))
    nil))

(defrecord JvmConnection [^Connection client codec on-status registry]
  proto/Conn
  (-publish [_ subject headers bytes]
    ;; The headers map selects jnats' publish(subject, Headers, body) overload; a
    ;; nil headers map is a plain publish (no header frame on the wire). jnats
    ;; rejects an over-max payload with an IllegalArgumentException; normalize it
    ;; via the shared helper to a synchronous `:max-payload-exceeded` (fire-and-forget
    ;; has no promise to reject — ADR 0006). A non-over-max IAE rethrows raw.
    (try
      (if-let [^Headers h (->headers headers)]
        (.publish client ^String subject h ^bytes bytes)
        (.publish client ^String subject ^bytes bytes))
      (catch IllegalArgumentException e
        (throw (max-payload-error client subject bytes e)))
      ;; A closed connection refuses the publish; surface it as the retry-able
      ;; `:connection-closed` (publish is allowed during drain, so it never sees
      ;; `:drained`). `op-state-error` returns the original for unrecognized states.
      (catch IllegalStateException e
        (throw (op-state-error subject e)))))
  (-subscribe [_ subject queue {:keys [on-error max-pending]} handler]
    ;; Road 2 (ADR 0007): onMessage BLOCKS this dispatcher's thread on the
    ;; handler's CompletionStage (no timeout). Serial, one-at-a-time delivery and
    ;; promise-return backpressure fall out for free — and, crucially, the backlog
    ;; builds in jnats' OWN dispatcher queue, so its native pending-limits and
    ;; slowConsumerDetected actually engage (the superseded tail-chain kept the
    ;; queue empty). A non-CompletionStage return delivers the next message at
    ;; once. A synchronous decode/handler throw, or an async rejection (surfaced as
    ;; a CompletionException by .join), is caught and routed to the sub's
    ;; :on-error / the connection's :on-status — the subscription survives.
    (let [^Dispatcher dispatcher (.createDispatcher client)
          ^MessageHandler mh
          (reify MessageHandler
            (onMessage [_ msg]
              (try
                (let [r (handler (msg->raw msg))]
                  (when (instance? CompletionStage r)
                    (.join (.toCompletableFuture ^CompletionStage r))))
                ;; Catch Exception, not Throwable, so a JVM Error (OOM, etc.)
                ;; propagates to jnats instead of being delivered as a fake
                ;; handler failure while the dispatch loop carries on.
                (catch Exception e
                  (route-error! on-error on-status (unwrap-completion e))))))]
      ;; The native slow-consumer wiring engages only when BOTH :max-pending and
      ;; :on-error are set (ADR 0007): :max-pending caps jnats' dispatcher queue
      ;; (bytes unlimited) and the dispatcher — the native Consumer that
      ;; slowConsumerDetected fires with — is registered so the connection
      ;; ErrorListener can route the overflow back to this :on-error. With no sink
      ;; there is nothing to signal, so leave jnats' 512K-msg / 64 MB defaults
      ;; rather than silently dropping harder than default. Handler-throw / decode
      ;; failures still route via the onMessage catch above, independent of this.
      (when (and max-pending on-error)
        (.setPendingLimits dispatcher (long max-pending) -1)
        (swap! registry assoc dispatcher
               {:on-error on-error :subject subject :max-pending max-pending}))
      ;; The queue-group name selects jnats' three-arg subscribe overload
      ;; (subject, queue, handler); a nil queue is a plain subscription. Wrap the
      ;; native handle in a JvmSubscription so the facade returns a uniform
      ;; Subscription (drain/active?) instead of leaking jnats' type.
      (->JvmSubscription
       (if queue
         (.subscribe dispatcher ^String subject ^String queue mh)
         (.subscribe dispatcher ^String subject mh))
       registry dispatcher)))
  (-flush [_]
    ;; jnats flush blocks until the server has processed the buffer (or the
    ;; timeout elapses, completing the future exceptionally); run it off-thread
    ;; so the facade returns a settling promise (ADR 0002).
    (CompletableFuture/runAsync
     (reify Runnable
       (run [_] (.flush client op-timeout)))))
  (-close [_]
    ;; jnats close is blocking and void; run it off-thread so the facade returns
    ;; a settling promise (ADR 0002). The CLOSED event reaches :on-status via the
    ;; ConnectionListener as the connection tears down.
    (CompletableFuture/runAsync
     (reify Runnable
       (run [_] (.close client)))))
  (-request [_ subject bytes timeout-ms]
    ;; jnats' requestWithTimeout returns a CompletableFuture<Message> over its
    ;; muxed reply-inbox. `.handle` lifts a successful Message into the raw map the
    ;; facade decodes, and normalizes the two failure modes (ADR 0006): with the
    ;; connection's useTimeoutException set (see `connect`), a timeout completes the
    ;; future with a TimeoutException, while a no-responders 503 cancels it
    ;; (CancellationException). We re-throw each as a typed ex-info so the facade's
    ;; promise rejects with it.
    ;; A closed or draining connection makes requestWithTimeout throw
    ;; synchronously; convert that to a rejected future so request rejects (never
    ;; throws) with `:connection-closed` (retry-able) or `:drained`. An over-max
    ;; payload throws IllegalArgumentException synchronously the same way; the
    ;; shared helper normalizes it to a rejected `:max-payload-exceeded` (ADR 0006).
    (try
      (.handle ^CompletableFuture (.requestWithTimeout client ^String subject ^bytes bytes (Duration/ofMillis timeout-ms))
               (reify BiFunction
                 (apply [_ msg ex]
                   (cond
                     (nil? ex)
                     (msg->raw msg)
                     (instance? TimeoutException ex)
                     (throw (ex-info "Request timed out"
                                     {:type :timeout :subject subject :timeout-ms timeout-ms}))
                     (instance? CancellationException ex)
                     (throw (ex-info "No responders for request"
                                     {:type :no-responders :subject subject}))
                     :else
                     (throw ex)))))
      (catch IllegalArgumentException e
        (let [x (max-payload-error client subject bytes e)]
          (if (identical? x e)
            (throw e)
            (CompletableFuture/failedFuture x))))
      (catch IllegalStateException e
        (let [x (op-state-error subject e)]
          (if (identical? x e)
            (throw e)
            (CompletableFuture/failedFuture x))))))
  proto/Drainable
  (-drain [_]
    ;; jnats drain already returns a CompletableFuture<Boolean>; draining ends the
    ;; connection's subscriptions and closes it (CLOSED reaches :on-status).
    (.drain client op-timeout)))

;; Status spine (ADR 0006/0009): map jnats' native lifecycle events onto the
;; canonical status :type set. Unmapped events (e.g. RESUBSCRIBED) are dropped;
;; :slow-consumer / :error production belong to the delivery / error-model slices.
(def ^:private event->type
  {ConnectionListener$Events/CONNECTED          :connected
   ConnectionListener$Events/DISCONNECTED       :disconnected
   ConnectionListener$Events/RECONNECTED        :reconnected
   ConnectionListener$Events/LAME_DUCK          :lame-duck
   ConnectionListener$Events/DISCOVERED_SERVERS :servers-changed
   ConnectionListener$Events/CLOSED             :closed})

(defn deliver-status!
  "Normalize one native jnats lifecycle event; when mapped, deliver it to
   `on-status` as a plain `{:type ...}` map and return the canonical type, else
   nil. Unmapped events (e.g. RESUBSCRIBED) are ignored."
  [on-status ev]
  (when-let [t (event->type ev)]
    (on-status {:type t})
    t))

(defn status-listener
  "A ConnectionListener that normalizes each lifecycle event onto `on-status`
   (see `deliver-status!`). jnats has no reconnecting event, so when reconnection
   is enabled a DISCONNECTED is followed by a synthesized `:reconnecting`,
   matching nats.js' native signal. `deliver-status!` runs unconditionally — the
   `reconnect?` gate guards only the synthesized `:reconnecting`, never the
   delivery of the underlying event."
  ^ConnectionListener [on-status reconnect?]
  (reify ConnectionListener
    (connectionEvent [_ _conn ev]
      (let [t (deliver-status! on-status ev)]
        (when (and reconnect? (= :disconnected t))
          (on-status {:type :reconnecting}))))))

(defn error-listener
  "A connection-level ErrorListener (ADR 0006). `slowConsumerDetected` fires with
   only the native Consumer — the Dispatcher — so the dispatcher→sink `registry`
   is the only bridge back to the originating subscription's `:on-error`; an
   unregistered dispatcher (the sub set no `:on-error`, or no `:max-pending`) drops
   it. A registered dispatcher releases its entry on the subscription's `-drain`; a
   sub whose reference is dropped without draining retains its entry until the
   connection closes (bounded by connection GC) — the deferred `unsubscribe` op
   must dissoc here too. `errorOccurred` is connection-level with no subscription
   identity, so a permissions/protocol error reaches `:on-status` as an `:error`
   event ONLY — never a per-sub override — and is dropped when no `:on-status` is
   set. `:pending` is native-approximate."
  ^ErrorListener [on-status registry]
  (reify ErrorListener
    (slowConsumerDetected [_ _conn consumer]
      (when-let [{:keys [on-error subject max-pending]} (get @registry consumer)]
        (on-error (ex-info "Slow consumer"
                           {:type :slow-consumer :subject subject :max-pending max-pending
                            :pending (.getPendingMessageCount ^Consumer consumer)}))))
    (errorOccurred [_ _conn error]
      (when on-status
        (on-status {:type  :error
                    :error (ex-info error {:type (error/server-error-type error)})})))))

(defn- nkey-auth-handler
  "An nkey AuthHandler signing nonces with `seed`. When the public `nkey` is
   given, assert it matches the seed-derived key so a mismatched pair fails fast
   (:auth-invalid) instead of as an opaque server-side Authorization Violation."
  ^AuthHandler [nkey seed]
  (let [nk  (NKey/fromSeed (char-array seed))
        pub (String. (.getPublicKey nk))]
    (when (and nkey (not= nkey pub))
      (throw (ex-info "nkey does not match seed"
                      {:type :auth-invalid :nkey nkey :derived pub})))
    (reify AuthHandler
      (sign  [_ nonce] (.sign nk nonce))
      (getID [_] (char-array pub))
      (getJWT [_] nil))))

(defn- with-reconnect
  "Apply the `:reconnect {:max :wait-ms :jitter-ms}` connect-option to the jnats
   Options builder. `:max 0` disables reconnection, `:max -1` is unlimited (jnats'
   own sentinels); absent keys leave jnats' own defaults in place."
  ^Options$Builder [^Options$Builder builder {:keys [max wait-ms jitter-ms]}]
  ;; Bound by Integer/MAX_VALUE: jnats' .maxReconnects takes an int, so a larger
  ;; value would throw an uncaught ArithmeticException at (int max) here while JS
  ;; accepts it silently. Reject portably before native (mirroring the
  ;; core/unsubscribe max guard), letting the -1 (unlimited) and 0 (off) sentinels
  ;; through.
  (when (and max (not (and (int? max) (<= -1 max 2147483647))))
    (throw (ex-info "reconnect :max must be an integer in [-1, 2147483647]"
                    {:type :invalid-max :max max})))
  (cond-> builder
    max       (.maxReconnects (int max))
    wait-ms   (.reconnectWait (Duration/ofMillis wait-ms))
    jitter-ms (.reconnectJitter (Duration/ofMillis jitter-ms))))

(defn- with-auth
  "Apply the `:auth` connect-option to the jnats Options builder, dispatching on the
   shared `auth/auth-variant`. The auth seam the advanced-auth slices extend."
  ^Options$Builder [^Options$Builder builder {:keys [token user pass nkey seed jwt creds] :as auth}]
  (case (auth/auth-variant auth)
    :token     (.token builder (char-array token))
    :user-pass (.userInfo builder (char-array user) (char-array pass))
    :nkey      (.authHandler builder (nkey-auth-handler nkey seed))
    :jwt       (.authHandler builder (Nats/staticCredentials (char-array jwt) (char-array seed)))
    :creds     (.authHandler builder (Nats/staticCredentials (.getBytes ^String creds StandardCharsets/UTF_8)))
    nil        builder))

(defn connect
  "Open a TCP connection to the first of `:servers`, resolving a CompletableFuture
   to a JvmConnection (ADR 0002: connect returns the platform-native promise).
   `:codec` defaults to :edn. `:auth` selects an auth method (e.g. `{:token ...}`)."
  [{:keys [servers codec auth on-status reconnect name] :or {codec :edn}}]
  (deliver-bare
   (CompletableFuture/supplyAsync
    (reify Supplier
      (get [_]
        ;; Build options inside the supplier so a client-side auth error (e.g. an
        ;; :nkey/seed mismatch) completes the future exceptionally — with its own
        ;; ex-info, unwrapped — rather than throwing synchronously from connect
        ;; (ADR 0002/0006: connect rejects its promise). Only the server-side
        ;; Nats/connect is wrapped as :connect-failed.
        ;; reconnect? gates the synthesized :reconnecting: it is on unless the
        ;; caller explicitly disabled reconnection with :reconnect {:max 0} (0 is
        ;; jnats' "off" sentinel; absent :max keeps the client default, which is on).
        (let [;; A bare-string :servers is the one non-portable value the README
              ;; quick-start documents; nats.js normalizes it to a one-element list,
              ;; so the JVM does too — otherwise (into-array String ...) seqs the
              ;; string into chars and throws "array element type mismatch".
              servers       (cond-> servers (string? servers) vector)
              reconnect?    (not= 0 (:max reconnect))
              ;; dispatcher→sink registry the ErrorListener routes slowConsumerDetected
              ;; through, and -subscribe assocs into (ADR 0006/0007).
              registry      (atom {})
              ^Options opts (-> (Options/builder)
                                (.servers (into-array String servers))
                                ;; Surface a request timeout as a TimeoutException
                                ;; (vs the default CancellationException, which a
                                ;; no-responders 503 also raises) so -request can
                                ;; tell the two failure modes apart (ADR 0006).
                                (.useTimeoutException)
                                (cond-> on-status (.connectionListener (status-listener on-status reconnect?)))
                                ;; The connection name surfaces in the server's
                                ;; monitoring (/connz); absent :name leaves jnats'
                                ;; own default (no name) in place.
                                (cond-> name (.connectionName name))
                                (.errorListener (error-listener on-status registry))
                                (with-reconnect reconnect)
                                (with-auth auth)
                                (.build))]
          (try
            (->JvmConnection (Nats/connect opts) codec on-status registry)
            (catch Exception e
              (throw (ex-info "Failed to connect to NATS"
                              {:type :connect-failed :servers servers}
                              e))))))))))

(defn then
  "Map `f` over the value the native promise `p` resolves to, returning a new
   native promise. The facade uses it to decode a raw message map without touching
   jnats' Message type (ADR 0005); a rejection surfaces as the bare ex-info
   (`deliver-bare`), matching JS and ADR 0006."
  [^CompletableFuture p f]
  (deliver-bare (.thenApply p (reify Function (apply [_ x] (f x))))))

(defn resolved
  "An already-resolved native promise of `x` — the seed of a promise chain so the
   first stage's throw rejects instead of escaping synchronously (ADR 0002/0006)."
  [x]
  (CompletableFuture/completedFuture x))

(defn bind
  "Like `then`, but `f` returns a native promise that is flattened into the
   result (thenApply does NOT flatten on the JVM, hence the distinct primitive).
   The facade uses it to splice an async `-request` into an encode/decode chain
   without nesting a CompletableFuture<CompletableFuture>; a rejection surfaces as
   the bare ex-info (`deliver-bare`), matching JS and ADR 0006."
  [^CompletableFuture p f]
  (deliver-bare (.thenCompose p (reify Function (apply [_ x] (f x))))))
