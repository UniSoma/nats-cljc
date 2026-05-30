(ns nats-cljc.impl.jvm
  "JVM platform implementation: a Connection record wrapping io.nats:jnats over
   TCP (ADR 0001/0003). All jnats interop is quarantined here (ADR 0005)."
  (:require [nats-cljc.protocol :as proto])
  (:import [io.nats.client Nats Options Options$Builder Connection Dispatcher MessageHandler Message]
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

(defn- with-auth
  "Apply the `:auth` connect-option to the jnats Options builder. The auth seam
   the advanced-auth slices extend."
  ^Options$Builder [^Options$Builder builder {:keys [token user pass]}]
  (cond-> builder
    token (.token (char-array token))
    user  (.userInfo (char-array user) (char-array pass))))

(defn connect
  "Open a TCP connection to the first of `:servers`, resolving a CompletableFuture
   to a JvmConnection (ADR 0002: connect returns the platform-native promise).
   `:codec` defaults to :edn. `:auth` selects an auth method (e.g. `{:token ...}`)."
  [{:keys [servers codec auth] :or {codec :edn}}]
  (let [^Options opts (-> (Options/builder)
                          (.servers (into-array String servers))
                          (with-auth auth)
                          (.build))]
    (CompletableFuture/supplyAsync
     (reify Supplier
       (get [_]
         (try
           (->JvmConnection (Nats/connect opts) codec)
           (catch Exception e
             (throw (ex-info "Failed to connect to NATS"
                             {:type :connect-failed :servers servers}
                             e)))))))))
