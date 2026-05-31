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
                                           :bytes   (.-data msg)}))}))
  (-flush [_]
    ;; nats.js flush already returns a Promise that settles once the server has
    ;; processed the buffer.
    (.flush ^js client))
  (-drain [_]
    ;; nats.js drain already returns a Promise; draining ends the connection's
    ;; subscriptions and closes it (the "close" status event still flows).
    (.drain ^js client))
  (-close [_]
    ;; nats.js close already returns a Promise; the "close" status event reaches
    ;; :on-status via the status() pump as the connection tears down.
    (.close ^js client)))

(defn- ->bytes [s]
  (.encode (js/TextEncoder.) s))

;; Status spine (ADR 0006/0009): map nats.js' native status names onto the
;; canonical status :type set. nats.js emits no event for the initial connection
;; (:connected is synthesized at connect); unmapped names are dropped —
;; :slow-consumer / :error production belong to the delivery / error-model slices.
(def ^:private status->type
  {"disconnect"   :disconnected
   "reconnecting" :reconnecting
   "reconnect"    :reconnected
   "ldm"          :lame-duck
   "update"       :servers-changed
   "close"        :closed})

(defn deliver-status!
  "Normalize one native nats.js status object; when its type is mapped, deliver
   it to `on-status` as a plain `{:type ...}` map and return the canonical type,
   else nil. Unmapped names are ignored."
  [on-status ^js native]
  (when-let [t (status->type (.-type native))]
    (on-status {:type t})
    t))

(defn- pump-status!
  "Drain nats.js' status() async-iterable, normalizing each event onto
   `on-status` (see `deliver-status!`). The iterable ends when the connection
   closes."
  [^js nc on-status]
  (let [iterable (.status nc)
        iter     (.call (unchecked-get iterable js/Symbol.asyncIterator) iterable)]
    (letfn [(step []
              (-> (.next iter)
                  (.then (fn [^js res]
                           (when-not (.-done res)
                             (deliver-status! on-status (.-value res))
                             (step))))
                  (.catch (fn [_] nil))))]
      (step))))

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

(defn with-reconnect
  "Merge the `:reconnect {:max :wait-ms :jitter-ms}` connect-option into the
   nats-core options map. `:max 0` disables reconnection — nats.js' off-switch is
   the `reconnect` boolean, not `maxReconnectAttempts 0` (which keeps reconnecting)
   — and `:max -1` is unlimited; absent keys leave nats.js' own defaults in place."
  [opts {:keys [max wait-ms jitter-ms]}]
  (cond-> opts
    (= max 0)              (assoc :reconnect false)
    (and max (not= max 0)) (assoc :maxReconnectAttempts max)
    wait-ms                (assoc :reconnectTimeWait wait-ms)
    jitter-ms              (assoc :reconnectJitter jitter-ms)))

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
  [{:keys [servers codec auth on-status reconnect] :or {codec :edn}}]
  ;; A client-side auth error (e.g. an :nkey/seed mismatch) thrown while building
  ;; the options rejects the returned promise — with its own ex-info, unwrapped —
  ;; rather than throwing synchronously from connect (ADR 0002/0006: connect
  ;; rejects its promise). Only the wsconnect failure is wrapped as :connect-failed.
  (try
    (-> (nats-core/wsconnect (clj->js (-> {:servers servers}
                                          (with-reconnect reconnect)
                                          (with-auth auth))))
        (.then (fn [nc]
                 ;; nats.js emits no event for the initial connection, so the
                 ;; baseline :connected is synthesized here — before the
                 ;; connection promise resolves — to match the jnats listener.
                 (when on-status
                   (on-status {:type :connected})
                   (pump-status! nc on-status))
                 (->JsConnection nc codec)))
        (.catch (fn [e]
                  (throw (ex-info "Failed to connect to NATS"
                                  {:type :connect-failed :servers servers}
                                  e)))))
    (catch :default e
      (js/Promise.reject e))))

(defn drain-subscription
  "Drain a single native Subscription, returning the Promise nats.js' sub.drain()
   yields. The facade's `drain` routes here when handed a subscription rather than
   a connection."
  [^js sub]
  (.drain sub))
