(ns nats-cljc.impl.js
  "ClojureScript platform implementation: a Connection record wrapping
   @nats-io/nats-core over WebSocket (ADR 0001/0003), serving browser and Node
   from one package. All JS interop is quarantined here (ADR 0005)."
  (:require [nats-cljc.error :as error]
            [nats-cljc.protocol :as proto]
            ["@nats-io/nats-core" :as nats-core]))

(defn- no-responders?
  "True when `e` is nats.js' no-responders rejection: a RequestError whose
   .isNoResponders() is true, or a bare NoRespondersError (its usual cause)."
  [e]
  (or (instance? nats-core/NoRespondersError e)
      (and (instance? nats-core/RequestError e) (.isNoResponders ^js e))))

(defn- timeout?
  "True when `e` is nats.js' request-timeout rejection: a TimeoutError, directly
   or as the cause of a wrapping RequestError."
  [e]
  (or (instance? nats-core/TimeoutError e)
      (instance? nats-core/TimeoutError (.-cause ^js e))))

(defn- ->headers
  "Build a nats.js MsgHdrs from the canonical portable map `{name -> [str ...]}`,
   or nil for none. append's default Exact match stores names verbatim, so the
   case-sensitive names survive to the wire."
  [headers]
  (when headers
    (let [h (nats-core/headers)]
      (doseq [[k vs] headers
              v       vs]
        (.append ^js h k v))
      h)))

(defn- raw-headers
  "Extract a nats.js Msg's headers into the canonical portable map
   `{name -> [str ...]}`, or nil when it carries none. toRecord preserves the
   case-sensitive names (ADR 0005)."
  [^js msg]
  (when-let [h (.-headers msg)]
    (let [r (js->clj (.toRecord ^js h))]
      (when (seq r) r))))

(defn- msg->raw
  "Lift a nats.js Msg into the raw map the facade decodes (ADR 0005): subject,
   wire bytes, the reply-to subject (nil when absent), and the headers map (nil
   when absent)."
  [^js msg]
  {:subject (.-subject msg)
   :bytes   (.-data msg)
   :reply   (.-reply msg)
   :headers (raw-headers msg)})

(defrecord JsSubscription [sub]
  proto/Drainable
  ;; nats.js sub.drain returns a Promise that settles once the subscription's
  ;; pending messages are delivered and it closes — ending just this subscription,
  ;; leaving the connection open.
  (-drain [_] (.drain ^js sub))
  proto/Sub
  (-active? [_] (not (.isClosed ^js sub)))
  ;; nats.js Subscription.unsubscribe(max?) ends the sub abruptly and is already a
  ;; no-op once closed (the idempotent contract, ADR 0012). It branches on `if (max)`
  ;; internally, so a nil `max` (passed straight through) stops now and a positive
  ;; int auto-stops after that many lifetime messages. Return nil rather than its void.
  (-unsubscribe [_ max]
    (.unsubscribe ^js sub max)
    nil))

(defn- op-state-error
  "Build the ex-info for a nats.js ClosedConnectionError from a publish/request on
   a non-open connection (ADR 0006): the drain WINDOW → `:drained` (a don't-retry
   signal), else fully closed → `:connection-closed` (retry-able). nats.js raises
   the same ClosedConnectionError for both, so the live `isDraining` — not the
   error type — distinguishes them."
  [^js client subject]
  (if (.isDraining client)
    (ex-info "Connection is draining" {:type :drained :subject subject})
    (ex-info "Connection is closed" {:type :connection-closed :subject subject})))

(defn- consume!
  "Drive a no-callback nats.js Subscription as an async-iterable (road 2, ADR
   0007): a detached `.next` loop (mirroring `pump-status!`) awaits the handler
   before pulling the next message, so a returned promise applies per-subscription
   backpressure and the backlog fills nats.js' OWN buffer — where the iterator-only
   slow-consumer threshold lives (the superseded callback-tail kept it empty). One
   funnel catches a sync decode throw, a sync handler throw, and a rejecting
   handler promise alike, routing each to the sub's `on-error` (else the
   connection `on-status` :error) and then CONTINUING — the subscription survives.
   The iterable completing (drain/unsubscribe/close) ends the loop; the `.next`
   `.catch` swallows that close-race. It also swallows the rejection a subscription
   `:permissions-violation` raises (nats-core's `stop(err)` makes `.next` throw):
   that is deliberate — nats-core ALSO dispatches the error to the status stream
   (protocol.js `processError` → `dispatchStatus {:type \"error\"}`), so it reaches
   `:on-status` as `:permissions-violation` and nothing is lost by ending the loop
   here. The sub becoming inactive on this leg (vs staying live on jnats) is an
   accepted divergence (ADR 0006)."
  [^js sub handler on-error on-status]
  (let [iter (.call (unchecked-get sub js/Symbol.asyncIterator) sub)]
    (letfn [(route [e]
              (try
                (if on-error (on-error e) (when on-status (on-status {:type :error :error e})))
                (catch :default _ nil))
              (step))
            (step []
              (-> (.next iter)
                  (.then (fn [^js res]
                           (when-not (.-done res)
                             ;; Run the handler inside the Promise executor so a sync
                             ;; decode/handler throw is captured as a rejection by the
                             ;; constructor — the same `route` funnel as a rejecting
                             ;; handler promise — without a throwaway resolved-promise
                             ;; hop. `.then` still awaits it before the next `.next`
                             ;; (per-publisher backpressure, ADR 0007).
                             (-> (js/Promise. (fn [resolve _] (resolve (handler (msg->raw (.-value res))))))
                                 (.then (fn [_] (step)))
                                 (.catch route)))))
                  (.catch (fn [_] nil))))]
      (step))))

(defrecord JsConnection [client codec on-status]
  proto/Conn
  (-publish [_ subject headers bytes]
    ;; The headers map rides in nats.js' PublishOptions; omit it for a plain
    ;; publish (no header frame on the wire). nats.js rejects an over-max payload
    ;; with an InvalidArgumentError; normalize it to a synchronous
    ;; `:max-payload-exceeded` (fire-and-forget has no promise to reject — ADR 0006).
    (try
      (if-let [h (->headers headers)]
        (.publish ^js client subject bytes #js {:headers h})
        (.publish ^js client subject bytes))
      (catch :default e
        (cond
          (instance? nats-core/InvalidArgumentError e)
          (throw (ex-info "Message payload exceeds the server's max payload"
                          {:type :max-payload-exceeded :subject subject
                           :size (.-length bytes) :max (some-> (.-info ^js client) .-max_payload)}))
          ;; A closed or draining connection refuses the publish (nats.js, unlike
          ;; jnats, rejects a draining publish too); normalize by drain state (ADR 0006).
          (instance? nats-core/ClosedConnectionError e)
          (throw (op-state-error client subject))
          :else (throw e)))))
  (-subscribe [_ subject queue {:keys [on-error max-pending]} handler]
    ;; Road 2 (ADR 0007): NO :callback — the subscription is an async-iterable that
    ;; `consume!` drives with a detached loop awaiting the handler, so a returned
    ;; promise applies backpressure and the backlog fills nats.js' own buffer
    ;; (where the iterator-only slow-consumer threshold lives; it throws under a
    ;; :callback sub). A decode/handler throw or rejection routes to :on-error /
    ;; :on-status and the loop continues — the subscription survives.
    (let [opts #js {}]
      ;; The queue-group name rides in nats.js' SubscriptionOptions; omit it for a
      ;; plain subscription. Wrap the native Sub in a JsSubscription so the facade
      ;; returns a uniform Subscription (drain/active?) without leaking nats.js' type.
      (when queue (set! (.-queue opts) queue))
      (let [sub (.subscribe ^js client subject opts)]
        ;; :max-pending arms nats.js' iterator-only slow-consumer threshold; its
        ;; notifier routes the overflow to this sub's :on-error (ADR 0006/0007).
        ;; nats.js does NOT auto-drop over-limit messages (unbounded buffer) — the
        ;; signal is portable, the drop is native (an accepted divergence). Absent
        ;; :on-error drops the signal, so only wire it when there's a sink.
        ;; NB: setSlowNotificationFn lives on nats-core's internal SubscriptionImpl,
        ;; not the public Subscription interface — re-verify on any nats-core bump
        ;; (pinned 3.3.1).
        (when (and max-pending on-error)
          (.setSlowNotificationFn ^js sub max-pending
                                  (fn [pending]
                                    (on-error (ex-info "Slow consumer"
                                                       {:type :slow-consumer :subject subject
                                                        :max-pending max-pending :pending pending})))))
        (consume! sub handler on-error on-status)
        (->JsSubscription sub))))
  (-flush [_]
    ;; nats.js flush already returns a Promise that settles once the server has
    ;; processed the buffer.
    (.flush ^js client))
  (-close [_]
    ;; nats.js close already returns a Promise; the "close" status event reaches
    ;; :on-status via the status() pump as the connection tears down.
    (.close ^js client))
  (-request [_ subject bytes timeout-ms]
    ;; nats.js request() resolves to a Msg over its muxed inbox; map it into the
    ;; raw map the facade decodes. nats.js already distinguishes the two failure
    ;; modes natively, which we normalize to typed ex-infos so the facade's promise
    ;; rejects with them (ADR 0006).
    (-> (.request ^js client subject bytes #js {:timeout timeout-ms})
        (.then (fn [^js msg] (msg->raw msg)))
        (.catch (fn [e]
                  (throw (cond
                           (no-responders? e)
                           (ex-info "No responders for request"
                                    {:type :no-responders :subject subject})
                           (timeout? e)
                           (ex-info "Request timed out"
                                    {:type :timeout :subject subject :timeout-ms timeout-ms})
                           ;; A closed or draining connection rejects the request
                           ;; with `:connection-closed` (retry-able) or `:drained`,
                           ;; by drain state (ADR 0006).
                           (instance? nats-core/ClosedConnectionError e)
                           (op-state-error client subject)
                           :else e))))))
  proto/Drainable
  (-drain [_]
    ;; nats.js drain already returns a Promise; draining ends the connection's
    ;; subscriptions and closes it (the "close" status event still flows).
    (.drain ^js client)))

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
  "Normalize one native nats.js status object onto `on-status` (ADR 0006). nats.js
   funnels server errors (permissions/protocol) through the same status stream as
   an `error` type with no subscription identity, so it surfaces as the lone
   non-bare lifecycle event `{:type :error :error <ex-info>}` — never a per-sub
   override — with the error classified by `server-error-type`. A mapped lifecycle
   name delivers as a bare `{:type ...}`; unmapped names are ignored. Returns the
   canonical type, or nil."
  [on-status ^js native]
  (if (= "error" (.-type native))
    (let [msg (some-> (.-error native) .-message)]
      (on-status {:type :error :error (ex-info (str msg) {:type (error/server-error-type msg)})})
      :error)
    (when-let [t (status->type (.-type native))]
      (on-status {:type t})
      t)))

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

(defn- auth-variant
  "Derive the tagged `:auth` variant — exactly one of :token / :user-pass / :nkey /
   :jwt / :creds, or nil when no auth is configured. The shapes are mutually
   exclusive, so each is keyed off its own discriminating field; :seed is therefore
   read by exactly one variant (:jwt when a jwt is present, else :nkey) rather than
   shared across two. A stray field beside another shape can no longer silently
   switch methods."
  [{:keys [token user nkey jwt creds]}]
  (cond
    token :token
    user  :user-pass
    jwt   :jwt
    nkey  :nkey
    creds :creds))

(defn- with-auth
  "Merge the `:auth` connect-option into the nats-core options map, dispatching on
   the explicit `auth-variant`. The auth seam the advanced-auth slices extend."
  [opts {:keys [token user pass nkey seed jwt creds] :as auth}]
  (case (auth-variant auth)
    :token     (assoc opts :token token)
    :user-pass (assoc opts :user user :pass pass)
    :nkey      (assoc opts :authenticator (nkey-authenticator nkey seed))
    :jwt       (assoc opts :authenticator (nats-core/jwtAuthenticator jwt (->bytes seed)))
    :creds     (assoc opts :authenticator (nats-core/credsAuthenticator (->bytes creds)))
    nil        opts))

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
                 (->JsConnection nc codec on-status)))
        (.catch (fn [e]
                  (throw (ex-info "Failed to connect to NATS"
                                  {:type :connect-failed :servers servers}
                                  e)))))
    (catch :default e
      (js/Promise.reject e))))

(defn then
  "Map `f` over the value the native promise `p` resolves to, returning a new
   native promise. The facade uses it to decode a raw message map without touching
   nats.js' Msg type (ADR 0005); a rejection propagates untouched."
  [p f]
  (.then ^js p f))
