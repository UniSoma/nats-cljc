(ns ^:no-doc nats-cljc.jetstream.impl.jvm
  "JVM JetStream implementation (ADR 0016/0017). jnats JetStream interop is
   quarantined here (ADR 0005), kept out of the core `nats-cljc.impl.jvm`: this ns
   `extend`s the JetStream protocol onto that ns's `JvmConnection` record, so
   `(jetstream conn)` vends a context without the core record itself depending on
   JetStream — the structural mirror of the CLJS confinement that keeps a core-only
   bundle JetStream-free (ADR 0016).

   jnats' `jetStream()`/`jetStreamManagement()` are cheap local constructions that
   never touch the server, so a JS-disabled server would not surface until the
   first real operation. Obtaining the context therefore forces a `$JS.API.INFO`
   round-trip, off-thread, so `:jetstream-not-enabled` surfaces at the handle on
   the JVM too — matching nats.js' native verify (ADR 0017)."
  ;; `nats-cljc.impl.jvm` (aliased `core`) is used for the promise helpers below;
  ;; the `:import` also reaches into it for the `JvmConnection` class to extend.
  (:require [nats-cljc.protocol :as proto]
            [nats-cljc.impl.jvm :as core]
            [nats-cljc.jetstream.error :as jet-err]
            [nats-cljc.jetstream.stream :as stream]
            [nats-cljc.jetstream.consumer :as consumer]
            [nats-cljc.jetstream.pull :as pull]
            [nats-cljc.jetstream.refill :as refill])
  (:import [nats_cljc.impl.jvm JvmConnection]
           [io.nats.client Connection Connection$Status JetStream JetStreamManagement JetStreamApiException PublishOptions ConsumerContext FetchConsumer FetchConsumeOptions ConsumeOptions ConsumeOptions$Builder MessageConsumer MessageHandler Message]
           [io.nats.client.impl NatsJetStreamMetaData]
           [io.nats.client.api ServerInfo StreamConfiguration StorageType RetentionPolicy StreamInfo StreamState PublishAck ConsumerConfiguration AckPolicy DeliverPolicy ConsumerInfo SequenceInfo]
           [java.io IOException]
           [java.time Duration ZonedDateTime]
           [java.time.format DateTimeFormatter DateTimeFormatterBuilder]
           [java.util.concurrent CompletableFuture CompletionStage ExecutorService RejectedExecutionException TimeUnit TimeoutException]
           [java.util.function BiFunction Supplier]))

;; The JetStream context (ADR 0017): one handle holding both jnats' data-plane
;; (`JetStream`) and management-plane (`JetStreamManagement`) objects. The native
;; client hands you the two separately; the portable surface collapses them into a
;; single value every JetStream operation flows through. `codec` is the connection's
;; default (the resolved `Prepared`), captured at entry so acked publish encodes
;; `:data` with it unless a per-call `:codec` overrides (ADR 0011). `io-executor` is
;; the connection's per-connection IO pool, carried so every off-thread JetStream op
;; runs there instead of the shared commonPool (the connection owns its lifecycle).
(defrecord JvmJetStreamContext [^JetStream js ^JetStreamManagement jsm codec ^ExecutorService io-executor])

(defn verify-io-type
  "Classify the `IOException` jnats raises when the forced `$JS.API.INFO` round-trip
   gets no usable response (ADR 0017). The round-trip is a request, so a *transient*
   timeout and a *true* no-responder are distinct failures — but jnats funnels both
   into one indistinguishable `IOException` (`responseRequired(null)`), so the
   round-trip-free `jetstream-available?` flag (`ServerInfo`'s connect-time
   `jetstream` bit) disambiguates: a server that *advertises* JetStream yet didn't
   answer in time is a transient `:timeout`; a server that advertises none means
   JetStream is off (`:jetstream-not-enabled`). Only the latter is the permanent
   signal, so a caller who would retry a blip is not told to give up (ADR 0017/0020).
   Account-level disablement never reaches here — it is a `JetStreamApiException`
   10039, normalized via the shared table (ADR 0020)."
  [jetstream-available?]
  (if jetstream-available? :timeout :jetstream-not-enabled))

(defn closing-type
  "Classify the `IOException` jnats' `ensureNotClosing` raises from the cheap
   `jetStream()`/`jetStreamManagement()` construction on a non-open connection, keyed
   on structured connection status rather than the message text so a jnats reword
   can't change the mapping (ADR 0006 — the `op-state-error` precedent): CLOSED is
   the retry-able `:connection-closed`; any other status reaching this site is a
   drain in progress (closing but not yet CLOSED) ⇒ `:drained`."
  [^Connection$Status status]
  (if (= status Connection$Status/CLOSED) :connection-closed :drained))

(extend-type JvmConnection
  proto/JetStream
  (-jetstream [conn]
    ;; `then` re-wraps the off-thread result so a rejection surfaces as the BARE
    ;; ex-info ADR 0006's `(:type (ex-data e))` contract needs, not a
    ;; CompletionException wrapper — the same deliver-bare guarantee core's `connect`
    ;; gives its own `supplyAsync`. `identity` because the Supplier already returns
    ;; the finished context; only the bare-delivery is wanted, there is nothing to
    ;; map. `supplyAsync` runs the blocking round-trip off the caller's thread (ADR 0002),
    ;; on the connection's IO pool — the forced getAccountStatistics below is a real
    ;; $JS.API.INFO round-trip, so it belongs off commonPool like every other JS op.
    (let [^Connection client          (:client conn)
          ^ExecutorService io-executor (:io-executor conn)]
      (core/then
       (CompletableFuture/supplyAsync
        (reify Supplier
          (get [_]
            (try
              ;; jetStream()/jetStreamManagement() are cheap LOCAL constructions —
              ;; they never touch the server — so their only failure is jnats'
              ;; ensureNotClosing on a closing/closed connection, caught by the
              ;; OUTER catch below. Keeping them inside this try is what lets a
              ;; closing connection surface a typed :connection-closed/:drained
              ;; instead of an untyped IOException leak (ADR 0006).
              (let [js  (.jetStream client)
                    jsm (.jetStreamManagement client)]
                ;; The forced verify-at-entry round-trip (ADR 0017):
                ;; getAccountStatistics issues $JS.API.INFO; its result is
                ;; discarded. Removing it would defer the JS-disabled failure to
                ;; the first operation and reintroduce the cross-leg asymmetry
                ;; ADR 0017 exists to prevent.
                (try
                  (.getAccountStatistics jsm)
                  (catch JetStreamApiException e
                    ;; A server-issued JetStream API error (e.g. an account-level
                    ;; disable → 10039): normalize the err_code via the shared
                    ;; table (ADR 0020).
                    (throw (ex-info (.getMessage e)
                                    {:type (jet-err/api-error-type (.getApiErrorCode e))} e)))
                  (catch IOException e
                    ;; The round-trip got no usable response. jnats can't tell a
                    ;; transient timeout from a true no-responder at this seam, so
                    ;; the round-trip-free server flag disambiguates (ADR 0017):
                    ;; only a server with no JetStream is :jetstream-not-enabled; a
                    ;; blip on a JS-enabled server is the core :timeout.
                    (let [available? (.isJetStreamAvailable ^ServerInfo (.getServerInfo client))]
                      (throw (ex-info (if available?
                                        "JetStream INFO request timed out"
                                        "JetStream is not enabled on the server or account")
                                      {:type (verify-io-type available?)} e)))))
                (->JvmJetStreamContext js jsm (:codec conn) io-executor))
              (catch IOException e
                ;; Reached ONLY from the construction above (ensureNotClosing on a
                ;; non-open connection): the round-trip IOException is already
                ;; converted to an ex-info inside the inner try, so it never lands
                ;; here. Key on connection status, not message text (ADR 0006).
                (throw (ex-info (.getMessage e)
                                {:type (closing-type (.getStatus client))} e))))))
        io-executor)
       identity))))

(defn- api-ex->ex-info
  "Normalize a jnats JetStreamApiException — what `addStream`/`getStreamInfo`/
   `deleteStream` raise for a server-issued rejection — to the portable operational
   ex-info (ADR 0020): the err_code routes to its `:type` and carries
   `{:code :description}`. A not-found is err_code 10059 ⇒ `:stream-not-found`; any
   other (e.g. a subject-overlap 10065) defaults to `:jetstream-api-error`."
  [^JetStreamApiException e]
  (ex-info (.getMessage e)
           (jet-err/api-error-data (.getApiErrorCode e) (.getErrorDescription e))
           e))

(defn- publish-ex->ex-info
  "Normalize the exception a failed `publishAsync` surfaces. A server rejection (e.g. a
   wrong `:expect` → JetStreamApiException 10071) arrives buried: jnats completes the
   future exceptionally with a RuntimeException whose cause is the JetStreamApiException,
   and the `.handle` stage wraps that once more in a CompletionException. The cause
   chain is walked to the JetStreamApiException and normalized via the shared table
   (ADR 0020); an exception with none in its chain passes through unwrapped. A bounded
   ack deadline firing surfaces as a `TimeoutException` (see `bound-ack`) ⇒ `:timeout`,
   built as a real ex-info because the deliver-bare seam peels exactly one wrapper, so a
   raw TimeoutException would reach the caller with nil ex-data. A failure with no
   JetStreamApiException in its chain (a 503/no-stream or connection-drop IOException,
   say) is wrapped at its deepest cause in the operational catch-all `:publish-failed`
   ex-info — for the same deliver-bare reason — so a non-API failure still carries a
   `:type`, not the raw IOException whose `(ex-data e)` is nil."
  [^Throwable e]
  (loop [t e]
    (cond
      (instance? TimeoutException t)      (ex-info (.getMessage t) {:type :timeout} t)
      (instance? JetStreamApiException t) (api-ex->ex-info t)
      (.getCause t)                       (recur (.getCause t))
      :else                               (ex-info (.getMessage t) {:type :publish-failed} t))))

(defn- off-thread
  "Run the blocking JetStream thunk `f` off the caller's thread (ADR 0002) on the
   connection's IO `executor` — never the shared commonPool, so a long-parking pull
   can't starve the management verbs (ADR 0018). Normalizes a server-issued
   JetStreamApiException to its portable operational `:type` and delivers the BARE
   ex-info on rejection (ADR 0006/0020) — the `then identity` re-wrap the entry point
   uses, shared by every verb. A submit racing the connection's close hits a
   shut-down executor (RejectedExecutionException); surface it as the same retry-able
   `:connection-closed` a closed connection's round-trip would, as a rejected future
   so the facade still settles rather than throwing (ADR 0002/0006)."
  [^ExecutorService executor f]
  (core/then
   (try
     (CompletableFuture/supplyAsync
      (reify Supplier
        (get [_]
          (try (f)
               (catch JetStreamApiException e (throw (api-ex->ex-info e))))))
      executor)
     (catch RejectedExecutionException _
       (CompletableFuture/failedFuture
        (ex-info "Connection is closed" {:type :connection-closed}))))
   identity))

(defn ->stream-config
  "Build a jnats StreamConfiguration from the portable closed kebab `config`
   (ADR 0020). Only the keys present are set, so an absent key takes the server
   default; the enum keywords route through the shared wire tables and
   `:max-age-ms` becomes a Duration (the CLJS leg uses Nanos)."
  ^StreamConfiguration [config]
  (let [b (StreamConfiguration/builder)]
    (.name b ^String (:name config))
    (when-let [subjects (:subjects config)]
      (.subjects b ^"[Ljava.lang.String;" (into-array String subjects)))
    (when-let [storage (:storage config)]
      (.storageType b (StorageType/get (stream/storage->wire storage))))
    (when-let [retention (:retention config)]
      (.retentionPolicy b (RetentionPolicy/get (stream/retention->wire retention))))
    (when-let [max-age-ms (:max-age-ms config)]
      (.maxAge b (Duration/ofMillis max-age-ms)))
    (.build b)))

(defn stream-config->map
  "Read a jnats StreamConfiguration back into the normalized portable kebab map —
   the active config the server applied, always the full curated set (defaults
   included), so a round-tripped info is predictable (ADR 0020). The wire enum
   strings route back through the shared tables; the Duration becomes integer ms."
  [^StreamConfiguration c]
  {:name (.getName c)
   :subjects (vec (.getSubjects c))
   :storage (stream/wire->storage (str (.getStorageType c)))
   :retention (stream/wire->retention (str (.getRetentionPolicy c)))
   :max-age-ms (.toMillis (.getMaxAge c))})

(defn ->consumer-config
  "Build a jnats ConsumerConfiguration from the portable closed kebab `config`
   (ADR 0020). Only the keys present are set, so an absent key takes the server
   default; the policy keywords route through the shared wire tables and `:ack-wait-ms`
   becomes a Duration (the CLJS leg uses Nanos). `:name` sets the consumer `name`; for a
   durable (`:durable?` absent or true) it ALSO sets `durable` — the field whose absence
   makes the consumer ephemeral (ADR 0021). An ephemeral (`:durable? false`) sets only
   `name` (a named ephemeral) or, with no `:name`, neither (the server assigns one)."
  ^ConsumerConfiguration [config]
  (let [b (ConsumerConfiguration/builder)]
    (when-let [name (:name config)]
      (.name b ^String name)
      (when-not (false? (:durable? config))
        (.durable b ^String name)))
    (when-let [ack-policy (:ack-policy config)]
      (.ackPolicy b (AckPolicy/get (consumer/ack-policy->wire ack-policy))))
    (when-let [deliver-policy (:deliver-policy config)]
      (.deliverPolicy b (DeliverPolicy/get (consumer/deliver-policy->wire deliver-policy))))
    (when-let [ack-wait-ms (:ack-wait-ms config)]
      (.ackWait b (Duration/ofMillis ack-wait-ms)))
    (when-let [max-deliver (:max-deliver config)]
      (.maxDeliver b (long max-deliver)))
    (when-let [filter-subjects (:filter-subjects config)]
      (.filterSubjects b (into-array String filter-subjects)))
    (.build b)))

(defn consumer-config->map
  "Read a jnats ConsumerConfiguration back into the normalized portable kebab map —
   the active config the server applied, always the full curated set (defaults
   included), so a round-tripped info is predictable (ADR 0020). The wire policy
   strings route back through the shared tables; the Duration becomes integer ms."
  [^ConsumerConfiguration c]
  {:name (.getName c)
   :durable? (some? (.getDurable c))
   :ack-policy (consumer/wire->ack-policy (str (.getAckPolicy c)))
   :deliver-policy (consumer/wire->deliver-policy (str (.getDeliverPolicy c)))
   :ack-wait-ms (.toMillis (.getAckWait c))
   :max-deliver (.getMaxDeliver c)
   :filter-subjects (vec (.getFilterSubjects c))})

(defn consumer-info->map
  "Curate a jnats ConsumerInfo into the normalized portable map (ADR 0020): the active
   `:config`, the `:created` timestamp as ISO-8601 (the same ZonedDateTime formatting
   as `stream-info->map`), and the delivery cursors — `:delivered` and `:ack-floor`
   each a `{:consumer-seq :stream-seq}` pair, plus the `:pending` count."
  [^ConsumerInfo ci]
  (let [^SequenceInfo d  (.getDelivered ci)
        ^SequenceInfo af (.getAckFloor ci)]
    {:stream    (.getStreamName ci)
     :name      (.getName ci)
     :config    (consumer-config->map (.getConsumerConfiguration ci))
     :created   (.format (.getCreationTime ci) DateTimeFormatter/ISO_OFFSET_DATE_TIME)
     :delivered {:consumer-seq (.getConsumerSequence d) :stream-seq (.getStreamSequence d)}
     :ack-floor {:consumer-seq (.getConsumerSequence af) :stream-seq (.getStreamSequence af)}
     :pending   (.getNumPending ci)}))

(defn stream-info->map
  "Curate a jnats StreamInfo into the normalized portable map (ADR 0020): the active
   `:config`, the `:created` timestamp as ISO-8601 (jnats hands back a
   ZonedDateTime, whose `toString` carries a `[GMT]` zone-region suffix that
   ISO_OFFSET_DATE_TIME drops), and a curated `:state`."
  [^StreamInfo si]
  (let [^StreamState st (.getStreamState si)]
    {:config (stream-config->map (.getConfiguration si))
     :created (.format (.getCreateTime si) DateTimeFormatter/ISO_OFFSET_DATE_TIME)
     :state {:messages (.getMsgCount st)
             :bytes (.getByteCount st)
             :first-seq (.getFirstSequence st)
             :last-seq (.getLastSequence st)
             :consumer-count (.getConsumerCount st)}}))

(defn ->publish-options
  "Build a jnats PublishOptions from the portable publish `opts` (ADR 0020). Only the
   keys present are set, so an absent key takes the server default; `:msg-id` becomes
   the dedup messageId, and each `:expect` field its expected-* optimistic-concurrency
   assertion. The reserved Nats-* headers these map to are set by the native client,
   which is why setting them directly in user `:headers` is rejected pre-flight. The
   ack deadline (`:timeout-ms`) is NOT set here — jnats ignores PublishOptions.streamTimeout
   on the publish path, so it is enforced separately in `bound-ack`. Returns nil for empty
   `opts` so the publish path passes a null PublishOptions (the native default) with no allocation."
  ^PublishOptions [{:keys [msg-id expect] :as opts}]
  (when (seq opts)
    (let [b (PublishOptions/builder)]
      (when msg-id (.messageId b ^String msg-id))
      (when-let [{:keys [last-seq last-msg-id stream last-subject-seq]} expect]
        (when last-seq (.expectedLastSequence b (long last-seq)))
        (when last-msg-id (.expectedLastMsgId b ^String last-msg-id))
        (when stream (.expectedStream b ^String stream))
        (when last-subject-seq (.expectedLastSubjectSequence b (long last-subject-seq))))
      (.build b))))

(defn- ->pub-ack
  "Normalize a jnats PublishAck into the portable PubAck map (ADR 0020):
   `{:stream :seq :duplicate :domain}`, `:domain` nil when none is configured. One
   shared shape so both legs surface an ack identically."
  [^PublishAck a]
  {:stream    (.getStream a)
   :seq       (.getSeqno a)
   :duplicate (.isDuplicate a)
   :domain    (.getDomain a)})

(extend-type JvmJetStreamContext
  proto/StreamManager
  (-create-stream [ctx config]
    (off-thread (:io-executor ctx) #(stream-info->map (.addStream ^JetStreamManagement (:jsm ctx) (->stream-config config)))))
  (-stream-info [ctx name]
    (off-thread (:io-executor ctx) #(stream-info->map (.getStreamInfo ^JetStreamManagement (:jsm ctx) ^String name))))
  (-delete-stream [ctx name]
    (off-thread (:io-executor ctx) #(do (.deleteStream ^JetStreamManagement (:jsm ctx) ^String name) nil))))

(extend-type JvmJetStreamContext
  proto/ConsumerManager
  (-create-consumer [ctx stream config]
    (off-thread (:io-executor ctx) #(consumer-info->map (.createConsumer ^JetStreamManagement (:jsm ctx) ^String stream (->consumer-config config)))))
  (-consumer-info [ctx stream name]
    (off-thread (:io-executor ctx) #(consumer-info->map (.getConsumerInfo ^JetStreamManagement (:jsm ctx) ^String stream ^String name))))
  (-delete-consumer [ctx stream name]
    (off-thread (:io-executor ctx) #(do (.deleteConsumer ^JetStreamManagement (:jsm ctx) ^String stream ^String name) nil)))
  (-list-consumers [ctx stream]
    (off-thread (:io-executor ctx) #(mapv consumer-info->map (.getConsumers ^JetStreamManagement (:jsm ctx) ^String stream)))))

(defn- bound-ack
  "Resolve the publishAsync `fut` to a portable PubAck, enforcing the `:timeout-ms`
   ack deadline ourselves. jnats 2.25.3 never reads PublishOptions.streamTimeout on the
   publish path — it pushes a null Duration into the request, which falls back to the
   connection's request-cleanup interval — so the deadline a caller asks for has to be
   imposed on the returned future. `.orTimeout` (applied only when `:timeout-ms` is
   present) completes `fut` with a TimeoutException at the deadline; placing it BEFORE
   `.handle` lets the BiFunction observe that exception and normalize it to `:timeout`.
   This deadline-ownership asymmetry is intentional: on Node the client library owns the
   deadline (CLJS sets opts.timeout), on the JVM the future-holder does.

   `.handle` runs on both outcomes so a server rejection (or the timeout) is normalized
   to its operational ex-info in the same stage that maps a success to the PubAck; the
   thrown ex-info then surfaces BARE through `core/then ... identity`'s deliver-bare
   (ADR 0006), like the management verbs' `off-thread`."
  [^CompletableFuture fut timeout-ms]
  (core/then
   (.handle ^CompletableFuture (cond-> fut
                                 timeout-ms (.orTimeout timeout-ms TimeUnit/MILLISECONDS))
            (reify BiFunction
              (apply [_ ack e]
                (if e
                  (throw (publish-ex->ex-info e))
                  (->pub-ack ack)))))
   identity))

;; The one canonical `:timestamp` format both legs emit (ADR 0019 pure-data parity):
;; ISO-8601 in UTC with exactly three fractional digits — e.g. 2026-06-09T01:08:17.279Z.
;; `appendInstant(3)` truncates sub-millisecond precision (never rounds) and forces the
;; `Z` offset, so a delivered message's `:timestamp` is byte-identical to the Node leg's
;; `Date#toISOString`. This is NOT the `ISO_OFFSET_DATE_TIME` `:created` uses — that
;; carries the source offset and variable fractional digits, which diverge across legs.
(def ^:private ^DateTimeFormatter canonical-instant
  (-> (DateTimeFormatterBuilder.) (.appendInstant 3) .toFormatter))

(defn- ->canonical-timestamp
  "Normalize a jnats metadata ZonedDateTime to the canonical `:timestamp` string."
  [^ZonedDateTime zdt]
  (.format canonical-instant (.toInstant zdt)))

(defn js-msg->raw
  "Lift a delivered JetStream Message into the pure-data pull map (ADR 0019): the core
   `msg->raw` carries subject/bytes/headers, the reply-to (the message's $JS.ACK ack
   subject) moves UNDER `:js` as `:ack-subject` and is dropped from the top level so a
   mistaken `(reply conn js-msg)` can't publish to it, and the native
   NatsJetStreamMetaData is read into the rest of `:js`. `:redelivered` is derived
   (delivered > 1) so both legs agree without a native redelivered flag; `:timestamp`
   is the ZonedDateTime normalized to the canonical UTC-millis form
   (`->canonical-timestamp`) so it is byte-identical to the Node leg; `:domain`
   coerces an absent domain to nil. The native object is then discarded — everything
   downstream is pure data, the lift's whole point."
  [^Message msg]
  (let [^NatsJetStreamMetaData md (.metaData msg)
        delivered (.deliveredCount md)
        domain    (.getDomain md)]
    (-> (core/msg->raw msg)
        (dissoc :reply)
        (assoc :js {:stream       (.getStream md)
                    :consumer     (.getConsumer md)
                    :stream-seq   (.streamSequence md)
                    :delivery-seq (.consumerSequence md)
                    :delivered    delivered
                    :pending      (.pendingCount md)
                    :redelivered  (> delivered 1)
                    :timestamp    (->canonical-timestamp (.timestamp md))
                    :domain       (when (seq domain) domain)
                    :ack-subject  (.getReplyTo msg)}))))

(defn- ->fetch-options
  "Build a jnats FetchConsumeOptions from the portable pull `opts` (ADR 0018): `:batch`
   is the max-messages ceiling — defaulting to `pull/default-batch`, the facade's chosen
   portable default, so the legs agree when a caller omits it. The explicit default is
   load-bearing: jnats' own FetchConsumeOptions default (DEFAULT_MESSAGE_COUNT) is 500, so
   dropping it would let this leg fall back to 500 and diverge from the JS leg. `:expires-ms`
   is the window after which a batch shorter than `:batch` settles with what it has."
  ^FetchConsumeOptions [{:keys [batch expires-ms]}]
  (let [b (FetchConsumeOptions/builder)]
    (.maxMessages b (int (or batch pull/default-batch)))
    (when expires-ms (.expiresIn b (long expires-ms)))
    (.build b)))

(defn- ->consume-options
  "Build a jnats ConsumeOptions from the portable refill knobs (ADR 0018): `:batch`
   is the per-pull-window message ceiling (explicitly defaulted to
   `pull/default-batch` — jnats' own consume default is 500, nats.js' 100, so the
   pin keeps the legs agreeing); `:threshold` is the portable refill COUNT,
   converted to jnats' percent unit by the refill deep module so the repull point
   lands exactly on the count; `:expires-ms` the pull window; `:max-bytes` the
   byte cap (jnats' batchBytes). `:idle-heartbeat-ms` has no jnats setter — the
   client derives its heartbeat from the expires window — so the knob is accepted
   portably but the cadence here is native (ADR 0006 shape-not-cadence).

   thresholdPercent/expiresIn are inherited from jnats' PROTECTED BaseConsumeOptions
   Builder with no public-subclass override (unlike FetchConsumeOptions' builder),
   so plain interop falls back to reflection and dies on the non-public declaring
   class; the param-tagged qualified calls resolve them through the public
   ConsumeOptions$Builder instead — re-verify on any jnats bump (pinned 2.25.3)."
  ^ConsumeOptions [{:keys [batch threshold expires-ms max-bytes]}]
  (let [bm (int (or batch pull/default-batch))
        ^ConsumeOptions$Builder b (ConsumeOptions/builder)]
    (.batchSize b bm)
    (when threshold (^[int] ConsumeOptions$Builder/.thresholdPercent b (int (refill/threshold->percent threshold bm))))
    (when expires-ms (^[long] ConsumeOptions$Builder/.expiresIn b (long expires-ms)))
    (when max-bytes (.batchBytes b (long max-bytes)))
    (.build b)))

;; The consume handle (ADR 0018): wraps jnats' MessageConsumer in the same
;; Drainable/Sub shape core's JvmSubscription gives a Subscription, so the core
;; facade's drain/unsubscribe dispatch over it unchanged. There is no slow-consumer
;; registry to clean up — pull has no :slow-consumer (ADR 0018).
(defrecord JvmConsumeHandle [^MessageConsumer mc ^ExecutorService io-executor]
  proto/Drainable
  ;; stop() ends new pulls but lets buffered messages deliver and the open pull
  ;; wind down (bounded by the consume's :expires-ms window); jnats flags
  ;; isFinished then, with no future to wait on, so the settle is an off-thread
  ;; poll on the connection's IO pool — the pool built for long parks (ADR 0018).
  (-drain [_]
    (.stop mc)
    (CompletableFuture/supplyAsync
     (reify Supplier
       (get [_]
         (loop []
           (if (.isFinished mc)
             true
             (do (Thread/sleep 50) (recur))))))
     io-executor))
  proto/Sub
  ;; Stopped covers the drain window too: a draining consume no longer delivers
  ;; new interest, matching "not yet drained, unsubscribed, or ended".
  (-active? [_] (not (or (.isStopped mc) (.isFinished mc))))
  ;; close() unsubscribes the underlying pull subscription now, dropping buffered
  ;; messages (un-acked, so the server redelivers them) — the abrupt sibling of
  ;; stop(). jnats' lenientClose already swallows its own teardown errors; the
  ;; catch covers the checked signature for the idempotent no-op (ADR 0012).
  ;; A consume has no auto-unsubscribe count, so any `max` is outside the range
  ;; this operation accepts (ADR 0015's :invalid-max).
  (-unsubscribe [_ max]
    (when max
      (throw (ex-info "consume handles do not support an auto-unsubscribe max"
                      {:type :invalid-max :max max})))
    (try (.close mc) (catch Exception _ nil))
    nil))

(extend-type JvmJetStreamContext
  proto/JetStreamData
  (-js-publish [ctx subject headers bytes opts]
    (bound-ack (.publishAsync ^JetStream (:js ctx) ^String subject
                              (core/->headers headers) ^bytes bytes
                              (->publish-options opts))
               (:timeout-ms opts)))
  (-js-next [ctx stream consumer opts]
    ;; getConsumerContext does a $JS.API round-trip (a missing consumer surfaces as a
    ;; JetStreamApiException → :consumer-not-found via off-thread), so the whole poll
    ;; runs off-thread (ADR 0002). next(long) waits :expires-ms and returns null on an
    ;; empty consumer; next() (no :expires-ms) uses jnats' default expiry. This inlines
    ;; the :expires-ms->window mapping that fetch routes through ->fetch-options; a new
    ;; pull option (e.g. :idle-heartbeat) must be wired into both to keep next and fetch
    ;; in step.
    (off-thread
     (:io-executor ctx)
     #(let [^ConsumerContext cctx (.getConsumerContext ^JetStream (:js ctx) ^String stream ^String consumer)
            msg (if-let [e (:expires-ms opts)] (.next cctx (long e)) (.next cctx))]
        (when msg (js-msg->raw msg)))))
  (-js-fetch [ctx stream consumer opts]
    (off-thread
     (:io-executor ctx)
     #(let [^ConsumerContext cctx (.getConsumerContext ^JetStream (:js ctx) ^String stream ^String consumer)
            ^FetchConsumer fc     (.fetch cctx (->fetch-options opts))]
        ;; FetchConsumer.nextMessage returns null once the bounded batch is exhausted
        ;; (maxMessages reached or expiresIn elapsed); close it either way to release
        ;; the pull subscription.
        (try
          (loop [acc (transient [])]
            (if-let [msg (.nextMessage fc)]
              (recur (conj! acc (js-msg->raw msg)))
              (persistent! acc)))
          (finally (.close fc))))))
  (-js-consume [ctx stream consumer opts handler]
    ;; Road 2 (ADR 0007/0018): onMessage BLOCKS the consume's dispatcher thread on
    ;; the handler's CompletionStage, exactly as core -subscribe does — serial
    ;; delivery and promise-return backpressure fall out, and the backlog gates
    ;; jnats' OWN refill (pendingUpdated never fires past a blocked handler), so
    ;; the read rate bounds the pull rate with nothing overflowing. The
    ;; getConsumerContext round-trip surfaces a missing consumer as
    ;; :consumer-not-found via off-thread, like next/fetch.
    (off-thread
     (:io-executor ctx)
     #(let [^ConsumerContext cctx (.getConsumerContext ^JetStream (:js ctx) ^String stream ^String consumer)
            mh (reify MessageHandler
                 (onMessage [_ msg]
                   (try
                     (let [r (handler (js-msg->raw msg))]
                       (when (instance? CompletionStage r)
                         (.join (.toCompletableFuture ^CompletionStage r))))
                     ;; Catch Exception, not Throwable (the core -subscribe
                     ;; precedent): a handler/decode failure must not re-raise
                     ;; into jnats' dispatch loop. Routing it to a per-consume
                     ;; :on-error is the consume error-model follow-up; until
                     ;; that lands the failure is contained and delivery
                     ;; continues.
                     (catch Exception _ nil))))]
        (->JvmConsumeHandle (.consume cctx (->consume-options opts) mh)
                            (:io-executor ctx))))))
