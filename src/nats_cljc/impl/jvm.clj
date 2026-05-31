(ns nats-cljc.impl.jvm
  "JVM platform implementation: a Connection record wrapping io.nats:jnats over
   TCP (ADR 0001/0003). All jnats interop is quarantined here (ADR 0005)."
  (:require [nats-cljc.protocol :as proto])
  (:import [io.nats.client Nats Options Options$Builder Connection Subscription Dispatcher MessageHandler Message AuthHandler NKey ConnectionListener ConnectionListener$Events]
           [java.time Duration]
           [java.util.concurrent CompletableFuture]
           [java.util.function Supplier]))

;; Upper bound for the blocking flush/drain one-shots; a healthy connection
;; settles in milliseconds. Finite (not Duration.ZERO = wait-forever) so a dead
;; connection rejects rather than hangs; richer timeout handling is the
;; error-model slice's job.
(def ^:private ^Duration op-timeout (Duration/ofSeconds 10))

(defrecord JvmConnection [^Connection client codec]
  proto/Conn
  (-publish [_ subject bytes]
    (.publish client ^String subject ^bytes bytes))
  (-subscribe [_ subject handler]
    (let [^Dispatcher dispatcher (.createDispatcher client)]
      (.subscribe dispatcher ^String subject
                  (reify MessageHandler
                    (onMessage [_ msg]
                      (handler {:subject (.getSubject ^Message msg)
                                :bytes   (.getData ^Message msg)}))))))
  (-flush [_]
    ;; jnats flush blocks until the server has processed the buffer (or the
    ;; timeout elapses, completing the future exceptionally); run it off-thread
    ;; so the facade returns a settling promise (ADR 0002).
    (CompletableFuture/runAsync
     (reify Runnable
       (run [_] (.flush client op-timeout)))))
  (-drain [_]
    ;; jnats drain already returns a CompletableFuture<Boolean>; draining ends the
    ;; connection's subscriptions and closes it (CLOSED reaches :on-status).
    (.drain client op-timeout))
  (-close [_]
    ;; jnats close is blocking and void; run it off-thread so the facade returns
    ;; a settling promise (ADR 0002). The CLOSED event reaches :on-status via the
    ;; ConnectionListener as the connection tears down.
    (CompletableFuture/runAsync
     (reify Runnable
       (run [_] (.close client))))))

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

(defn- status-listener
  "A ConnectionListener that normalizes each lifecycle event onto `on-status`
   (see `deliver-status!`). jnats has no reconnecting event, so when reconnection
   is enabled a DISCONNECTED is followed by a synthesized `:reconnecting`,
   matching nats.js' native signal."
  ^ConnectionListener [on-status reconnect?]
  (reify ConnectionListener
    (connectionEvent [_ _conn ev]
      (when (and reconnect? (= :disconnected (deliver-status! on-status ev)))
        (on-status {:type :reconnecting})))))

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
   Options builder. Absent keys leave jnats' own defaults in place."
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
       (let [^Options opts (-> (Options/builder)
                               (.servers (into-array String servers))
                               (cond-> on-status (.connectionListener (status-listener on-status (not= 0 (:max reconnect)))))
                               (with-reconnect reconnect)
                               (with-auth auth)
                               (.build))]
         (try
           (->JvmConnection (Nats/connect opts) codec)
           (catch Exception e
             (throw (ex-info "Failed to connect to NATS"
                             {:type :connect-failed :servers servers}
                             e)))))))))

(defn drain-subscription
  "Drain a single native Subscription, returning a CompletableFuture<Boolean>
   that settles once its pending messages are delivered and it is removed. The
   facade's `drain` routes here when handed a subscription rather than a
   connection."
  [^Subscription sub]
  (.drain sub op-timeout))
