(ns nats-cljc.impl.jvm
  "JVM platform implementation: a Connection record wrapping io.nats:jnats over
   TCP (ADR 0001/0003). All jnats interop is quarantined here (ADR 0005)."
  (:require [nats-cljc.protocol :as proto])
  (:import [io.nats.client Nats Options Options$Builder Connection Dispatcher MessageHandler Message AuthHandler NKey]
           [java.util.concurrent CompletableFuture]
           [java.util.function Supplier]))

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
                                :bytes   (.getData ^Message msg)})))))))

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
  [{:keys [servers codec auth] :or {codec :edn}}]
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
                               (with-auth auth)
                               (.build))]
         (try
           (->JvmConnection (Nats/connect opts) codec)
           (catch Exception e
             (throw (ex-info "Failed to connect to NATS"
                             {:type :connect-failed :servers servers}
                             e)))))))))
