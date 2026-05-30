(ns nats-cljc.impl.js
  "ClojureScript platform implementation: a Connection record wrapping
   @nats-io/nats-core over WebSocket (ADR 0001/0003), serving browser and Node
   from one package. All JS interop is quarantined here (ADR 0005)."
  (:require [nats-cljc.protocol :as proto]
            ["@nats-io/nats-core" :as nats-core]))

(defrecord JsConnection [client codec]
  proto/Conn
  (-publish [_ subject bytes]
    (.publish ^js client subject bytes))
  (-subscribe [_ subject handler]
    ;; A subscribe with a :callback delivers per-message and returns the
    ;; Subscription synchronously (instead of becoming an async iterable).
    (.subscribe ^js client subject
                #js {:callback (fn [_err ^js msg]
                                 (handler {:subject (.-subject msg)
                                           :bytes   (.-data msg)}))})))

(defn- ->bytes [s]
  (.encode (js/TextEncoder.) s))

(defn- nkey-authenticator
  "nats-core nkey authenticator over `seed`. When the public `nkey` is given,
   assert it matches the seed-derived key so a mismatched pair fails fast
   (:auth-invalid) instead of as an opaque server-side rejection."
  [nkey seed]
  (let [seed-bytes (->bytes seed)]
    (when nkey
      (let [pub (.getPublicKey (.fromSeed nats-core/nkeys seed-bytes))]
        (when (not= nkey pub)
          (throw (ex-info "nkey does not match seed"
                          {:type :auth-invalid :nkey nkey :derived pub})))))
    (nats-core/nkeyAuthenticator seed-bytes)))

(defn- with-auth
  "Merge the `:auth` connect-option into the nats-core options map. The auth seam
   the advanced-auth slices extend."
  [opts {:keys [token user pass nkey seed jwt creds]}]
  (cond-> opts
    token (assoc :token token)
    user  (assoc :user user :pass pass)
    seed  (assoc :authenticator (if jwt
                                  (nats-core/jwtAuthenticator jwt (->bytes seed))
                                  (nkey-authenticator nkey seed)))
    creds (assoc :authenticator (nats-core/credsAuthenticator (->bytes creds)))))

(defn connect
  "Open a WebSocket connection to `:servers`, returning a js/Promise that resolves
   to a JsConnection (ADR 0002). `:codec` defaults to :edn. `:auth` selects an auth
   method (e.g. `{:token ...}`)."
  [{:keys [servers codec auth] :or {codec :edn}}]
  ;; A client-side auth error (e.g. an :nkey/seed mismatch) thrown while building
  ;; the options rejects the returned promise — with its own ex-info, unwrapped —
  ;; rather than throwing synchronously from connect (ADR 0002/0006: connect
  ;; rejects its promise). Only the wsconnect failure is wrapped as :connect-failed.
  (try
    (-> (nats-core/wsconnect (clj->js (with-auth {:servers servers} auth)))
        (.then (fn [nc] (->JsConnection nc codec)))
        (.catch (fn [e]
                  (throw (ex-info "Failed to connect to NATS"
                                  {:type :connect-failed :servers servers}
                                  e)))))
    (catch :default e
      (js/Promise.reject e))))
