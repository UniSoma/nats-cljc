(ns nats-cljc.impl.jvm
  "JVM platform implementation: a Connection record wrapping io.nats:jnats over
   TCP (ADR 0001/0003). All jnats interop is quarantined here (ADR 0005)."
  (:require [nats-cljc.protocol :as proto])
  (:import [io.nats.client Nats Options Options$Builder Connection Subscription Dispatcher MessageHandler Message AuthHandler NKey ConnectionListener ConnectionListener$Events]
           [java.time Duration]
           [java.util.concurrent CompletableFuture CompletionStage CancellationException TimeoutException]
           [java.util.function Supplier Function BiFunction]))

;; Upper bound for the blocking flush/drain one-shots; a healthy connection
;; settles in milliseconds. Finite (not Duration.ZERO = wait-forever) so a dead
;; connection rejects rather than hangs; richer timeout handling is the
;; error-model slice's job.
(def ^:private ^Duration op-timeout (Duration/ofSeconds 10))

(defn- msg->raw
  "Lift a jnats Message into the raw map the facade decodes (ADR 0005): subject,
   wire bytes, and the reply-to subject (nil when absent)."
  [^Message msg]
  {:subject (.getSubject msg)
   :bytes   (.getData msg)
   :reply   (.getReplyTo msg)})

(defrecord JvmSubscription [^Subscription sub]
  proto/Drainable
  ;; jnats sub.drain returns a CompletableFuture<Boolean> that settles once the
  ;; subscription's pending messages are delivered and it is removed — ending just
  ;; this subscription, leaving the connection open.
  (-drain [_] (.drain sub op-timeout))
  proto/Sub
  (-active? [_] (.isActive sub)))

(defrecord JvmConnection [^Connection client codec]
  proto/Conn
  (-publish [_ subject bytes]
    (.publish client ^String subject ^bytes bytes))
  (-subscribe [_ subject queue handler]
    ;; Per-subscription tail: each message's handler invocation is composed onto
    ;; the previous one's settle, so a handler that returns a CompletionStage
    ;; suspends delivery of the next message until it settles — promise-return
    ;; backpressure (ADR 0007). jnats dispatches onMessage serially on this
    ;; dispatcher's single thread, so the tail mutates without contention and the
    ;; thread is never blocked (the compose returns immediately). A
    ;; non-CompletionStage return composes a completed future, delivering the next
    ;; message at once. `.exceptionally` keeps a throwing/rejecting handler from
    ;; stalling the chain (error routing is the error-model slice's job).
    ;; NB: under a sustained-slow handler the undelivered backlog grows UNBOUNDED
    ;; in this chain — onMessage returns at once, so jnats' dispatcher queue stays
    ;; empty and its native slow-consumer (setPendingLimits/slowConsumerDetected)
    ;; never trips. Honoring :max-pending + surfacing :slow-consumer is
    ;; nts-01kstxatbw6k AC#4 (likely a rework to a blocking-dispatcher model).
    (let [^Dispatcher dispatcher (.createDispatcher client)
          tail                   (atom (CompletableFuture/completedFuture nil))
          ^MessageHandler mh     (reify MessageHandler
                                   (onMessage [_ msg]
                                     (let [m (msg->raw msg)]
                                       (swap! tail
                                              (fn [^CompletableFuture prev]
                                                (-> prev
                                                    (.thenCompose
                                                     (reify Function
                                                       (apply [_ _]
                                                         (let [r (handler m)]
                                                           (if (instance? CompletionStage r)
                                                             r
                                                             (CompletableFuture/completedFuture nil))))))
                                                    (.exceptionally
                                                     (reify Function
                                                       (apply [_ _] nil)))))))))]
      ;; The queue-group name selects jnats' three-arg subscribe overload
      ;; (subject, queue, handler); a nil queue is a plain subscription. Wrap the
      ;; native handle in a JvmSubscription so the facade returns a uniform
      ;; Subscription (drain/active?) instead of leaking jnats' type.
      (->JvmSubscription
       (if queue
         (.subscribe dispatcher ^String subject ^String queue mh)
         (.subscribe dispatcher ^String subject mh)))))
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
                   (throw ex))))))
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
  (cond-> builder
    max       (.maxReconnects (int max))
    wait-ms   (.reconnectWait (Duration/ofMillis wait-ms))
    jitter-ms (.reconnectJitter (Duration/ofMillis jitter-ms))))

(defn- with-auth
  "Apply the `:auth` connect-option to the jnats Options builder. The auth seam
   the advanced-auth slices extend."
  ^Options$Builder [^Options$Builder builder {:keys [token user pass nkey seed jwt creds]}]
  (cond-> builder
    token (.token (char-array token))
    user  (.userInfo (char-array user) (char-array pass))
    seed  (.authHandler (if jwt
                          (Nats/staticCredentials (char-array jwt) (char-array seed))
                          (nkey-auth-handler nkey seed)))
    creds (.authHandler (Nats/staticCredentials (.getBytes ^String creds)))))

(defn connect
  "Open a TCP connection to the first of `:servers`, resolving a CompletableFuture
   to a JvmConnection (ADR 0002: connect returns the platform-native promise).
   `:codec` defaults to :edn. `:auth` selects an auth method (e.g. `{:token ...}`)."
  [{:keys [servers codec auth on-status reconnect] :or {codec :edn}}]
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
       (let [reconnect?    (not= 0 (:max reconnect))
             ^Options opts (-> (Options/builder)
                               (.servers (into-array String servers))
                               ;; Surface a request timeout as a TimeoutException
                               ;; (vs the default CancellationException, which a
                               ;; no-responders 503 also raises) so -request can
                               ;; tell the two failure modes apart (ADR 0006).
                               (.useTimeoutException)
                               (cond-> on-status (.connectionListener (status-listener on-status reconnect?)))
                               (with-reconnect reconnect)
                               (with-auth auth)
                               (.build))]
         (try
           (->JvmConnection (Nats/connect opts) codec)
           (catch Exception e
             (throw (ex-info "Failed to connect to NATS"
                             {:type :connect-failed :servers servers}
                             e)))))))))

(defn then
  "Map `f` over the value the native promise `p` resolves to, returning a new
   native promise. The facade uses it to decode a raw message map without touching
   jnats' Message type (ADR 0005); a rejection propagates untouched."
  [^CompletableFuture p f]
  (.thenApply p (reify Function (apply [_ x] (f x)))))
