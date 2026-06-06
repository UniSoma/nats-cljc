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
            [nats-cljc.jetstream.stream :as stream])
  (:import [nats_cljc.impl.jvm JvmConnection]
           [io.nats.client Connection Connection$Status JetStream JetStreamManagement JetStreamApiException]
           [io.nats.client.api ServerInfo StreamConfiguration StorageType RetentionPolicy StreamInfo StreamState]
           [java.io IOException]
           [java.time Duration]
           [java.time.format DateTimeFormatter]
           [java.util.concurrent CompletableFuture]
           [java.util.function Supplier]))

;; The JetStream context (ADR 0017): one handle holding both jnats' data-plane
;; (`JetStream`) and management-plane (`JetStreamManagement`) objects. The native
;; client hands you the two separately; the portable surface collapses them into a
;; single value every JetStream operation flows through.
(defrecord JvmJetStreamContext [^JetStream js ^JetStreamManagement jsm])

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
    ;; map. `supplyAsync` runs the blocking round-trip off the caller's thread (ADR 0002).
    (core/then
     (CompletableFuture/supplyAsync
      (reify Supplier
        (get [_]
          (let [^Connection client (:client conn)]
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
                (->JvmJetStreamContext js jsm))
              (catch IOException e
                ;; Reached ONLY from the construction above (ensureNotClosing on a
                ;; non-open connection): the round-trip IOException is already
                ;; converted to an ex-info inside the inner try, so it never lands
                ;; here. Key on connection status, not message text (ADR 0006).
                (throw (ex-info (.getMessage e)
                                {:type (closing-type (.getStatus client))} e))))))))
     identity)))

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

(defn- off-thread
  "Run the blocking management thunk `f` off the caller's thread (ADR 0002),
   normalizing a server-issued JetStreamApiException to its portable operational
   `:type` and delivering the BARE ex-info on rejection (ADR 0006/0020) — the
   `then identity` re-wrap the entry point uses, shared by the three verbs."
  [f]
  (core/then
   (CompletableFuture/supplyAsync
    (reify Supplier
      (get [_]
        (try (f)
             (catch JetStreamApiException e (throw (api-ex->ex-info e)))))))
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

(extend-type JvmJetStreamContext
  proto/StreamManager
  (-create-stream [ctx config]
    (off-thread #(stream-info->map (.addStream ^JetStreamManagement (:jsm ctx) (->stream-config config)))))
  (-stream-info [ctx name]
    (off-thread #(stream-info->map (.getStreamInfo ^JetStreamManagement (:jsm ctx) ^String name))))
  (-delete-stream [ctx name]
    (off-thread #(do (.deleteStream ^JetStreamManagement (:jsm ctx) ^String name) nil))))
