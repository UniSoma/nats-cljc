(ns ^:no-doc nats-cljc.jetstream.impl.js
  "ClojureScript JetStream implementation (ADR 0016/0017). This is the ONE
   namespace that imports `@nats-io/jetstream`; it is required only by the
   JetStream facade, so a core-only consumer who never touches the facade keeps a
   JetStream-free browser bundle — shadow-cljs's module graph excludes the
   unreachable npm dep (ADR 0016). It `extend`s the JetStream protocol onto the
   core `JsConnection` record (defined in `nats-cljc.impl.js`), mirroring the JVM
   confinement.

   nats.js' `jetstreamManager(nc)` natively round-trips ($JS.API.INFO) and rejects
   when JetStream is disabled, so verify-at-entry (ADR 0017) is native here — the
   JVM forces the equivalent round-trip to match."
  (:require [nats-cljc.protocol :as proto]
            [nats-cljc.impl.js :as core]
            [nats-cljc.jetstream.error :as jet-err]
            [nats-cljc.jetstream.stream :as stream]
            [nats-cljc.jetstream.consumer :as consumer]
            [nats-cljc.jetstream.pull :as pull]
            ["@nats-io/jetstream" :as jetstream]))

;; The JetStream context (ADR 0017): one handle holding both nats.js' data-plane
;; (`jetstream`) and management-plane (`jetstreamManager`) objects. The native
;; client hands you the two separately; the portable surface collapses them into a
;; single value every JetStream operation flows through. `codec` is the connection's
;; default (the resolved `Prepared`), captured at entry so acked publish encodes
;; `:data` with it unless a per-call `:codec` overrides (ADR 0011).
(defrecord JsJetStreamContext [js jsm codec])

(defn verify-error
  "Normalize a verify-at-entry rejection to a portable ex-info (ADR 0017/0020). The
   INFO round-trip is a request, so nats.js rejects it with distinct *named* errors:
   `JetStreamNotEnabled` (the no-responder ⇒ the server has no JetStream), a bare
   `TimeoutError` (a transient blip on a healthy server), and
   `ClosedConnectionError`/`DrainingConnectionError` (a non-open connection). Each
   maps to its portable `:type` — only the no-responder is the permanent
   `:jetstream-not-enabled`; the rest pass through as their core `:type` (ADR 0006),
   matching the JVM leg's disambiguation. Anything else is returned unchanged — its
   normalization belongs to the slice that exercises it."
  [^js e]
  (if-let [type (case (.-name e)
                  "JetStreamNotEnabled"     :jetstream-not-enabled
                  "TimeoutError"            :timeout
                  "ClosedConnectionError"   :connection-closed
                  "DrainingConnectionError" :drained
                  nil)]
    (ex-info (.-message e) {:type type} e)
    e))

(extend-type core/JsConnection
  proto/JetStream
  (-jetstream [conn]
    ;; jetstream(nc) is a cheap sync construction; jetstreamManager(nc) returns a
    ;; Promise that does the $JS.API.INFO round-trip and rejects when JetStream is
    ;; disabled — the native verify-at-entry the JVM leg forces to match (ADR 0017).
    (let [client (:client conn)
          js (jetstream/jetstream client)]
      (-> (jetstream/jetstreamManager client)
          (.then (fn [jsm] (->JsJetStreamContext js jsm (:codec conn))))
          (.catch (fn [e] (throw (verify-error e))))))))

(defn ->stream-config
  "Build a nats.js StreamConfig object from the portable closed kebab `config`
   (ADR 0020). Only the keys present are set, so an absent key takes the server
   default; the enum keywords route through the shared wire tables, `:max-age-ms`
   becomes `max_age` Nanos (the JVM leg uses a Duration), and `clj->js` produces the
   string-keyed object nats.js reads — surviving advanced compilation."
  [config]
  (clj->js
   (cond-> {:name (:name config)}
     (:subjects config)   (assoc :subjects (:subjects config))
     (:storage config)    (assoc :storage (stream/storage->wire (:storage config)))
     (:retention config)  (assoc :retention (stream/retention->wire (:retention config)))
     (:max-age-ms config) (assoc :max_age (* (:max-age-ms config) 1000000)))))

(defn stream-config->map
  "Read a nats.js StreamConfig back into the normalized portable kebab map — the
   active config the server applied, always the full curated set (defaults
   included), so a round-tripped info is predictable (ADR 0020). The wire enum
   strings route back through the shared tables; `max_age` Nanos becomes integer ms."
  [^js c]
  {:name       (.-name c)
   :subjects   (vec (.-subjects c))
   :storage    (stream/wire->storage (.-storage c))
   :retention  (stream/wire->retention (.-retention c))
   :max-age-ms (quot (.-max_age c) 1000000)})

(defn ->consumer-config
  "Build a nats.js ConsumerConfig object from the portable closed kebab `config`
   (ADR 0020). Only the keys present are set, so an absent key takes the server
   default; the policy keywords route through the shared wire tables, `:ack-wait-ms`
   becomes `ack_wait` Nanos (the JVM leg uses a Duration), and `clj->js` produces the
   string-keyed object nats.js reads — surviving advanced compilation. `:name` sets the
   consumer `name`; for a durable (`:durable?` absent or true) it ALSO sets `durable_name`
   — the field whose absence makes the consumer ephemeral (ADR 0021), mirroring the JVM
   leg's durable/name split. An ephemeral (`:durable? false`) sets only `name`, or with no
   `:name` neither (the server assigns one)."
  [config]
  (clj->js
   (cond-> {}
     (:name config)                                         (assoc :name (:name config))
     (and (:name config) (not (false? (:durable? config)))) (assoc :durable_name (:name config))
     (:ack-policy config)      (assoc :ack_policy (consumer/ack-policy->wire (:ack-policy config)))
     (:deliver-policy config)  (assoc :deliver_policy (consumer/deliver-policy->wire (:deliver-policy config)))
     (:ack-wait-ms config)     (assoc :ack_wait (* (:ack-wait-ms config) 1000000))
     (:max-deliver config)     (assoc :max_deliver (:max-deliver config))
     (:filter-subjects config) (assoc :filter_subjects (:filter-subjects config)))))

(defn consumer-config->map
  "Read a nats.js ConsumerConfig back into the normalized portable kebab map — the
   active config the server applied, always the full curated set (defaults included),
   so a round-tripped info is predictable (ADR 0020). The wire policy strings route
   back through the shared tables; `ack_wait` Nanos becomes integer ms."
  [^js c]
  {:name            (.-name c)
   :durable?        (some? (.-durable_name c))
   :ack-policy      (consumer/wire->ack-policy (.-ack_policy c))
   :deliver-policy  (consumer/wire->deliver-policy (.-deliver_policy c))
   :ack-wait-ms     (quot (.-ack_wait c) 1000000)
   :max-deliver     (.-max_deliver c)
   :filter-subjects (vec (.-filter_subjects c))})

(defn- ->canonical-timestamp
  "Normalize a server ISO-8601 timestamp string to the canonical portable form by
   re-emitting through `Date#toISOString` — UTC, exactly three fractional digits,
   truncated to millis. This is the single date seam on this leg — every surfaced
   timestamp (`:js :timestamp`, the `:created` fields) routes through it rather than
   passing the raw server string (full nanosecond precision, source offset) through,
   so it is byte-identical to the JVM leg's `->canonical-timestamp`."
  [s]
  (.toISOString (js/Date. s)))

(defn consumer-info->map
  "Curate a nats.js ConsumerInfo into the normalized portable map (ADR 0020): the
   active `:config`, the `:created` timestamp normalized to the canonical UTC-millis
   string (`->canonical-timestamp`, matching the JVM leg), and the delivery cursors —
   `:delivered` and `:ack-floor` each a `{:consumer-seq :stream-seq}` pair, plus the
   `:pending` count."
  [^js ci]
  (let [d  ^js (.-delivered ci)
        af ^js (.-ack_floor ci)]
    {:stream    (.-stream_name ci)
     :name      (.-name ci)
     :config    (consumer-config->map (.-config ci))
     :created   (->canonical-timestamp (.-created ci))
     :delivered {:consumer-seq (.-consumer_seq d) :stream-seq (.-stream_seq d)}
     :ack-floor {:consumer-seq (.-consumer_seq af) :stream-seq (.-stream_seq af)}
     :pending   (.-num_pending ci)}))

(defn stream-info->map
  "Curate a nats.js StreamInfo into the normalized portable map (ADR 0020): the
   active `:config`, the `:created` timestamp normalized to the canonical UTC-millis
   string (`->canonical-timestamp`, matching the JVM leg), and a curated `:state`."
  [^js si]
  (let [st ^js (.-state si)]
    {:config  (stream-config->map (.-config si))
     :created (->canonical-timestamp (.-created si))
     :state   {:messages       (.-messages st)
               :bytes          (.-bytes st)
               :first-seq      (.-first_seq st)
               :last-seq       (.-last_seq st)
               :consumer-count (.-consumer_count st)}}))

(defn api-error
  "Normalize a nats.js JetStreamApiError — what `streams.add`/`info`/`delete` reject
   with for a server-issued rejection — to the portable operational ex-info (ADR
   0020): its `apiError()` carries the err_code (routed to a `:type`) and the
   `:description`. A not-found is err_code 10059 ⇒ `:stream-not-found`; any other
   (e.g. a subject-overlap 10065) defaults to `:jetstream-api-error`. Anything that
   is not a JetStreamApiError passes through unchanged for the slice that owns it."
  [^js e]
  (if (instance? jetstream/JetStreamApiError e)
    (let [api ^js (.apiError e)]
      (ex-info (.-message e)
               (jet-err/api-error-data (.-err_code api) (.-description api))
               e))
    e))

(defn publish-error
  "Normalize a nats.js publish rejection to a bare typed ex-info (ADR 0006), the
   publish leg's analog of the JVM `publish-ex->ex-info`. A server-issued
   JetStreamApiError (e.g. a wrong `:expect` → 10071) routes through `api-error` to
   its operational `:type`; anything else — a `TimeoutError`, a no-responders error,
   which nats.js rejects with as a raw object whose `(ex-data e)` is nil — is wrapped
   in the operational catch-all `:publish-failed`, the same keyword the JVM leg uses,
   so a non-API failure carries a `:type` on both legs. The management verbs keep
   `api-error` (a non-API error there passes through raw, matching the JVM
   `off-thread`), so this catch-all stays confined to the publish path."
  [^js e]
  (if (instance? jetstream/JetStreamApiError e)
    (api-error e)
    (ex-info (.-message e) {:type :publish-failed} e)))

(defn ->publish-options
  "Build a nats.js JetStreamPublishOptions object from the portable publish `opts`
   (ADR 0020). Only the keys present are set, so an absent key takes the server
   default; `:msg-id` becomes `msgID`, `:timeout-ms` a plain `timeout` ms number (the
   JVM leg uses a Duration), and each `:expect` field its camelCased optimistic-
   concurrency assertion under `expect`. `clj->js` produces the string-keyed object
   nats.js reads, surviving advanced compilation. The reserved Nats-* headers these
   map to are set by the native client, which is why setting them directly in user
   `:headers` is rejected pre-flight. Returns nil (undefined) for empty `opts` so a bare
   publish skips the `clj->js {}` allocation and passes no options object."
  [{:keys [msg-id expect timeout-ms] :as opts}]
  (when (seq opts)
    (clj->js
     (cond-> {}
       msg-id     (assoc :msgID msg-id)
       timeout-ms (assoc :timeout timeout-ms)
       expect     (assoc :expect (cond-> {}
                                   (:last-seq expect)         (assoc :lastSequence (:last-seq expect))
                                   (:last-msg-id expect)      (assoc :lastMsgID (:last-msg-id expect))
                                   (:stream expect)           (assoc :streamName (:stream expect))
                                   (:last-subject-seq expect) (assoc :lastSubjectSequence (:last-subject-seq expect))))))))

(defn- ->pub-ack
  "Normalize a nats.js PubAck into the portable PubAck map (ADR 0020):
   `{:stream :seq :duplicate :domain}`, `:domain` coerced from a missing `domain`
   property (undefined) to nil so it matches the JVM leg's shape exactly."
  [^js a]
  {:stream    (.-stream a)
   :seq       (.-seq a)
   :duplicate (.-duplicate a)
   :domain    (or (.-domain a) nil)})

(defn- drain-lister
  "Drain a nats.js Lister — the async-paged iterator the list/names endpoints
   return — into a vector, calling `.next` for successive pages, normalizing each
   element through `f`, and accumulating until a page comes back empty (the
   Lister's end-of-pages signal). The JVM leg's `getConsumers`/`getStreams` hand
   back the full list in one call; this loop is the CLJS analog."
  [^js lister f acc]
  (-> (.next lister)
      (.then (fn [page]
               (if (zero? (alength page))
                 acc
                 (drain-lister lister f (into acc (map f (array-seq page)))))))))

(defn- with-api-error
  "Attach the shared management-verb rejection tail: normalize a nats.js error through
   `api-error` (ADR 0020) before it propagates. Folds the catch-and-normalize tail
   repeated across the stream/consumer verbs into one place."
  [p]
  (.catch p (fn [e] (throw (api-error e)))))

(extend-type JsJetStreamContext
  proto/StreamManager
  (-create-stream [ctx config]
    (let [streams ^js (.-streams ^js (:jsm ctx))]
      (-> (.add streams (->stream-config config))
          (.then (fn [si] (stream-info->map si)))
          with-api-error)))
  (-update-stream [ctx config]
    ;; nats.js `update` reads the current config and merges `cfg` over it before
    ;; the STREAM.UPDATE request, so an absent portable key keeps its current
    ;; value — the merge semantics the JVM leg reproduces (ADR 0020).
    (let [streams ^js (.-streams ^js (:jsm ctx))]
      (-> (.update streams (:name config) (->stream-config config))
          (.then (fn [si] (stream-info->map si)))
          with-api-error)))
  (-stream-info [ctx name]
    (let [streams ^js (.-streams ^js (:jsm ctx))]
      (-> (.info streams name)
          (.then (fn [si] (stream-info->map si)))
          with-api-error)))
  (-purge-stream [ctx name]
    (let [streams ^js (.-streams ^js (:jsm ctx))]
      (-> (.purge streams name)
          (.then (fn [^js r] {:purged (.-purged r)}))
          with-api-error)))
  (-delete-stream [ctx name]
    (let [streams ^js (.-streams ^js (:jsm ctx))]
      (-> (.delete streams name)
          (.then (fn [_] nil))
          with-api-error)))
  (-list-streams [ctx]
    (let [streams ^js (.-streams ^js (:jsm ctx))]
      (-> (drain-lister (.list streams) stream-info->map [])
          with-api-error)))
  (-stream-names [ctx]
    (let [streams ^js (.-streams ^js (:jsm ctx))]
      (-> (drain-lister (.names streams) identity [])
          with-api-error))))

(extend-type JsJetStreamContext
  proto/ConsumerManager
  (-create-consumer [ctx stream config]
    (let [consumers ^js (.-consumers ^js (:jsm ctx))]
      (-> (.add consumers stream (->consumer-config config))
          (.then (fn [ci] (consumer-info->map ci)))
          with-api-error)))
  (-update-consumer [ctx stream config]
    ;; nats.js `consumers.update` reads the current config and merges `cfg` over it
    ;; before the CONSUMER.CREATE action=update request, so an absent portable key
    ;; keeps its current value — the merge semantics the JVM leg reproduces (ADR
    ;; 0020). The pre-update read rejects 10014 ⇒ :consumer-not-found for a missing
    ;; Consumer.
    (let [consumers ^js (.-consumers ^js (:jsm ctx))]
      (-> (.update consumers stream (:name config) (->consumer-config config))
          (.then (fn [ci] (consumer-info->map ci)))
          with-api-error)))
  (-consumer-info [ctx stream name]
    (let [consumers ^js (.-consumers ^js (:jsm ctx))]
      (-> (.info consumers stream name)
          (.then (fn [ci] (consumer-info->map ci)))
          with-api-error)))
  (-delete-consumer [ctx stream name]
    (let [consumers ^js (.-consumers ^js (:jsm ctx))]
      (-> (.delete consumers stream name)
          (.then (fn [_] nil))
          with-api-error)))
  (-list-consumers [ctx stream]
    (let [consumers ^js (.-consumers ^js (:jsm ctx))]
      (-> (drain-lister (.list consumers stream) consumer-info->map [])
          with-api-error))))

(defn js-msg->raw
  "Lift a delivered JsMsg into the pure-data pull map (ADR 0019): the core `msg->raw`
   carries subject/bytes/headers, the reply-to (the JsMsg's $JS.ACK ack subject) moves
   UNDER `:js` as `:ack-subject` and is dropped from the top level so a mistaken
   `(reply conn js-msg)` can't publish to it, and the native DeliveryInfo is read into
   the rest of `:js`. `:redelivered` is derived (delivered > 1) so both legs agree
   without leaning on nats.js' separate flag; `:timestamp` is normalized to the canonical
   UTC-millis form (`->canonical-timestamp`) so it is byte-identical to the JVM leg;
   `:domain` coerces an absent (empty-string) domain to nil. The native object is then
   discarded — everything downstream is pure data, the lift's whole point."
  [^js m]
  (let [info      ^js (.-info m)
        delivered (.-deliveryCount info)
        domain    (.-domain info)]
    (-> (core/msg->raw m)
        (dissoc :reply)
        (assoc :js {:stream       (.-stream info)
                    :consumer     (.-consumer info)
                    :stream-seq   (.-streamSequence info)
                    :delivery-seq (.-deliverySequence info)
                    :delivered    delivered
                    :pending      (.-pending info)
                    :redelivered  (> delivered 1)
                    :timestamp    (->canonical-timestamp (.-timestamp m))
                    :domain       (when (seq domain) domain)
                    :ack-subject  (.-reply m)}))))

(defn drain-fetch
  "Drain a nats.js ConsumerMessages — the async-iterable `fetch` resolves to — into a
   vector of lifted pull maps, lifting each JsMsg with `js-msg->raw` as it arrives. The
   iterable completes (a `done` result) once the bounded batch is exhausted (:max_messages
   reached or :expires elapsed), which terminates the loop — the CLJS analog of the JVM
   FetchConsumer.nextMessage null. On the throw path — a lift raising mid-batch — the
   ConsumerMessages is closed before the rejection propagates, releasing the pull
   subscription immediately rather than leaking it until the expires/heartbeat window;
   the natural-done path needs no close, nats.js tears the iterable down on completion.
   Public (within this `^:no-doc` ns) so the suite can drive its lifecycle directly."
  [^js cm]
  (let [it (.call (unchecked-get cm (.-asyncIterator js/Symbol)) cm)]
    (letfn [(step [acc]
              (.then (.next it)
                     (fn [^js r]
                       (if (.-done r)
                         acc
                         (step (conj acc (js-msg->raw (.-value r))))))))]
      (-> (step [])
          (.catch (fn [e]
                    ;; release the pull subscription before re-rejecting — the CLJS
                    ;; analog of the JVM fetch's `(finally (.close fc))`. close is
                    ;; idempotent, so a rejection after nats.js already tore the
                    ;; iterable down is harmless.
                    (.close ^js cm)
                    (throw e)))))))

(defn- ->fetch-options
  "Build a nats.js FetchOptions object from the portable pull `opts` (ADR 0018): `:batch`
   is `max_messages` (defaulting to `pull/default-batch`, which matches nats.js' own default,
   so the legs agree when a caller omits it) and `:expires-ms` the `expires` window. `clj->js`
   produces the string-keyed object nats.js reads, surviving advanced compilation."
  [{:keys [batch expires-ms]}]
  (clj->js (cond-> {:max_messages (or batch pull/default-batch)}
             expires-ms (assoc :expires expires-ms))))

(defn- ->consume-options
  "Build a nats.js ConsumeOptions object from the portable refill knobs (ADR 0018):
   `:batch` is `max_messages` (defaulted to `pull/default-batch`, matching nats.js'
   own default so the legs agree), `:threshold` is `threshold_messages` VERBATIM —
   the portable count is nats.js-native, the JVM leg owns the percent conversion —
   `:expires-ms` the `expires` window, `:idle-heartbeat-ms` the `idle_heartbeat`
   pulse, `:max-bytes` the `max_bytes` cap. nats.js forbids a user setting
   `max_messages` alongside `max_bytes`, so a byte window omits `max_messages`
   entirely (refill/validate-opts has already ruled out a `:batch`/`:threshold`
   pairing); a message window sends `max_messages` as before. The internal
   `:abort-on-missing-resource` (set by the NAMED consume path, never the ordered
   one — recreation is the ordered contract) maps to nats.js'
   `abort_on_missing_resource`, which ends the iterator when the consumer or its
   backing stream is gone — the terminal side-band conditions completing the
   handle (ADR 0020). `clj->js` produces the string-keyed object nats.js reads,
   surviving advanced compilation."
  [{:keys [batch threshold expires-ms idle-heartbeat-ms max-bytes abort-on-missing-resource]}]
  (clj->js (cond-> {}
             (not max-bytes)   (assoc :max_messages (or batch pull/default-batch))
             threshold         (assoc :threshold_messages threshold)
             expires-ms        (assoc :expires expires-ms)
             idle-heartbeat-ms (assoc :idle_heartbeat idle-heartbeat-ms)
             max-bytes         (assoc :max_bytes max-bytes)
             abort-on-missing-resource (assoc :abort_on_missing_resource true))))

(defn- sink!
  "Deliver `e` to a consume's `:on-error` sink, swallowing a throwing sink (the
   ADR 0007 route funnel precedent — this runs inside the detached drive/status
   loops). A nil sink drops the error: consume failures are per-consume only,
   never `:on-status` (ADR 0020)."
  [on-error e]
  (when on-error
    (try (on-error e) (catch :default _ nil))))

(defn- drive-consume!
  "Drive a nats.js ConsumerMessages — the async-iterable `consume` resolves to —
   with a detached `.next` loop that awaits the handler before pulling the next
   message (road 2, ADR 0007/0018): a returned promise applies per-message
   backpressure, and because nats.js refills from its OWN buffered count, the
   read rate gates the pull rate with nothing overflowing. The consume sibling of
   core's `consume!` and this ns's `drain-fetch`. The iterable completing
   (close/stop — including nats.js aborting on a terminal missing-resource
   condition, whose normalized error the status pump delivers) ends the loop and
   flips `active?` off; a handler/decode throw or rejection is contained, routed
   to the per-consume `:on-error`, and the loop CONTINUES (ADR 0020). The `.next`
   `.catch` swallows the close-race like core's loop."
  [^js cm handler active? on-error]
  (let [it (.call (unchecked-get cm (.-asyncIterator js/Symbol)) cm)]
    (letfn [(step []
              (-> (.next it)
                  (.then (fn [^js res]
                           (if (.-done res)
                             (reset! active? false)
                             (-> (js/Promise. (fn [resolve _] (resolve (handler (js-msg->raw (.-value res))))))
                                 (.then (fn [_] (step)))
                                 (.catch (fn [e] (sink! on-error e) (step)))))))
                  (.catch (fn [_] (reset! active? false)))))]
      (step))))

(defn- consume-status->error
  "Normalize one nats.js consume status event into its portable side-band ex-info,
   or nil for the event types that are not error conditions (debug/next/discard/
   reset/recreation chatter). The 409-backed events (`consumer_deleted`,
   `exceeded_limits`) carry the wire code+description, so they route through the
   SHARED classifier — the same table the JVM leg feeds jnats' raw Status — and
   the client-synthesized ones map directly: `heartbeats_missed` to the shared
   bare `:heartbeats-missed`, `stream_not_found` to `:stream-not-found` (the
   backing-stream loss reuse), `consumer_not_found` to `:consumer-not-found`
   (ADR 0020)."
  [^js ev]
  (case (.-type ev)
    "heartbeats_missed" (jet-err/heartbeats-missed-error)
    ("consumer_deleted" "exceeded_limits") (jet-err/side-band-error (.-code ev) (.-description ev))
    "stream_not_found" (ex-info "Stream not found"
                                {:type :stream-not-found :stream (.-name ev)})
    "consumer_not_found" (ex-info "Consumer not found"
                                  {:type :consumer-not-found :stream (.-stream ev) :consumer (.-name ev)})
    nil))

(defn- pump-consume-status!
  "Funnel a ConsumerMessages' side-band `status()` iterable onto the per-consume
   `:on-error` (ADR 0020): each error-grade event normalizes via
   `consume-status->error` and is delivered bare, exactly like core's
   :slow-consumer routing row — never `:on-status`, never both. The consume
   sibling of core's `pump-status!`; nats.js stops the listener iterator when the
   consume ends, which settles `.next` done and ends the pump."
  [^js cm on-error]
  (let [statuses (.status cm)
        it (.call (unchecked-get statuses (.-asyncIterator js/Symbol)) statuses)]
    (letfn [(step []
              (-> (.next it)
                  (.then (fn [^js res]
                           (when-not (.-done res)
                             (when-some [e (consume-status->error (.-value res))]
                               (sink! on-error e))
                             (step))))
                  (.catch (fn [_] nil))))]
      (step))))

;; The consume handle (ADR 0018): wraps nats.js' ConsumerMessages in the same
;; Drainable/Sub shape core's JsSubscription gives a Subscription, so the core
;; facade's drain/unsubscribe dispatch over it unchanged. `active?` is an atom the
;; drive loop and the teardown verbs flip — nats.js exposes no liveness predicate.
;; The drain path flips it only when close() RESOLVES, never at initiation: the
;; handle stays active through the drain window (ADR 0022).
(defrecord JsConsumeHandle [cm active?]
  proto/Drainable
  ;; nats.js close() stops the iterator and resolves once the client side has
  ;; cleaned up. Buffered undelivered messages are DISCARDED — un-acked, so the
  ;; server redelivers them — where the JVM leg delivers them first: the settle's
  ;; shape is portable, the wind-down native (ADR 0006).
  (-drain [_]
    (-> (.close ^js cm)
        (core/then (fn [_] (reset! active? false) true))))
  proto/Sub
  (-active? [_] @active?)
  ;; QueuedIterator stop() ends the iterator now and is a no-op once done — the
  ;; idempotent abrupt teardown (ADR 0012). A consume has no auto-unsubscribe
  ;; count, so any `max` is outside the range this operation accepts (ADR 0015's
  ;; :invalid-max).
  (-unsubscribe [_ max]
    (when max
      (throw (ex-info "consume handles do not support an auto-unsubscribe max"
                      {:type :invalid-max :max max})))
    (.stop ^js cm)
    (reset! active? false)
    nil))

(defn- next-msg
  "Poll one message from a nats.js pull Consumer — the interface the named
   (`consumers.get`) and ordered (`consumers.ordered`) consumers share, so the
   named and ordered pull paths drive one body. next({expires}) resolves a JsMsg or
   null on an empty consumer. This inlines the :expires-ms->:expires mapping that
   fetch routes through ->fetch-options; a new pull option (e.g. :idle-heartbeat)
   must be wired into both to keep next and fetch in step."
  [^js c opts]
  (-> (.next c (clj->js (cond-> {} (:expires-ms opts) (assoc :expires (:expires-ms opts)))))
      (.then (fn [m] (when m (js-msg->raw m))))))

(defn- start-consume
  "Start a continuous consume on a nats.js pull Consumer (named or ordered, as
   `next-msg`): consume() resolves the ConsumerMessages iterable the detached drive
   loop then owns, wrapped in the JsConsumeHandle. The handle resolves only after
   the loop is running, so no delivery can race it. A per-consume `:on-error` in
   `opts` starts the side-band status pump alongside the drive loop (ADR 0020);
   without one, side-band conditions drop — termination on a terminal condition is
   nats.js' own abort (see `->consume-options`), so it does not depend on a sink."
  [^js c opts handler]
  (let [on-error (:on-error opts)]
    (-> (.consume c (->consume-options opts))
        (.then (fn [cm]
                 (let [active? (atom true)]
                   (when on-error (pump-consume-status! cm on-error))
                   (drive-consume! cm handler active? on-error)
                   (->JsConsumeHandle cm active?)))))))

;; The Ordered consumer pull handle: nats.js' ordered pull Consumer (server-managed
;; ephemeral, ack policy none, recreated on a sequence gap) plus the context codec,
;; so the facade's decode precedence works on the handle exactly as on the context.
(defrecord JsOrderedConsumer [consumer codec])

(defn- ->ordered-config
  "Build a nats.js OrderedConsumerOptions object from the portable closed ordered
   opts (ADR 0020). Only the keys present are set, so an absent key takes the
   client default; the deliver-policy keyword routes through the shared wire table.
   `clj->js` produces the string-keyed object nats.js reads, surviving advanced
   compilation."
  [{:keys [filter-subjects deliver-policy]}]
  (clj->js
   (cond-> {}
     filter-subjects (assoc :filter_subjects filter-subjects)
     deliver-policy  (assoc :deliver_policy (consumer/deliver-policy->wire deliver-policy)))))

(extend-type JsOrderedConsumer
  proto/OrderedPull
  ;; The named-pull bodies verbatim, minus the consumers.get resolution — the
  ;; ordered Consumer is already in hand, and nats.js recreates the underlying
  ;; ephemeral across these calls when a sequence gap appears.
  (-oc-next [oc opts]
    (-> (next-msg (:consumer oc) opts)
        (.catch (fn [e] (throw (api-error e))))))
  (-oc-fetch [oc opts]
    (-> (.fetch ^js (:consumer oc) (->fetch-options opts))
        (.then drain-fetch)
        (.catch (fn [e] (throw (api-error e))))))
  (-oc-consume [oc opts handler]
    (-> (start-consume (:consumer oc) opts handler)
        (.catch (fn [e] (throw (api-error e)))))))

(extend-type JsJetStreamContext
  proto/JetStreamData
  (-js-publish [ctx subject headers bytes opts]
    ;; nats.js carries the publish headers INSIDE the options object (a `headers`
    ;; key holding a MsgHdrs), not as a separate argument the way jnats' publishAsync
    ;; does — so the canonical headers are built and attached to the native options.
    ;; ->publish-options is nil for empty opts; headers still need an object to ride in,
    ;; so a headers-only publish gets a bare #js {} while a bare publish passes undefined.
    (let [o ^js (or (->publish-options opts) (when headers #js {}))]
      (when headers (set! (.-headers o) (core/->headers headers)))
      (-> (.publish ^js (:js ctx) subject bytes o)
          (.then ->pub-ack)
          (.catch (fn [e] (throw (publish-error e)))))))
  (-js-next [ctx stream consumer opts]
    ;; consumers.get resolves the pull Consumer (a missing one rejects → normalized
    ;; via api-error); the poll itself is the shared `next-msg`.
    (-> (.get ^js (.-consumers ^js (:js ctx)) stream consumer)
        (.then (fn [c] (next-msg c opts)))
        (.catch (fn [e] (throw (api-error e))))))
  (-js-fetch [ctx stream consumer opts]
    (-> (.get ^js (.-consumers ^js (:js ctx)) stream consumer)
        (.then (fn [c] (.fetch c (->fetch-options opts))))
        (.then drain-fetch)
        (.catch (fn [e] (throw (api-error e))))))
  (-js-consume [ctx stream consumer opts handler]
    ;; consumers.get resolves the pull Consumer (a missing one rejects ->
    ;; :consumer-not-found via api-error, like next/fetch); the consume start is
    ;; the shared `start-consume`. Named consumes abort on a missing resource so a
    ;; terminal side-band condition completes the handle (ADR 0020); the ordered
    ;; path never sets the flag — recreation is its contract.
    (-> (.get ^js (.-consumers ^js (:js ctx)) stream consumer)
        (.then (fn [c] (start-consume c (assoc opts :abort-on-missing-resource true) handler)))
        (.catch (fn [e] (throw (api-error e))))))
  (-js-ordered-consumer [ctx stream opts]
    ;; consumers.ordered round-trips stream info before building the ephemeral's
    ;; config (a missing stream rejects → :stream-not-found via api-error, the
    ;; verify the JVM leg's getStreamContext matches).
    (-> (.ordered ^js (.-consumers ^js (:js ctx)) stream (->ordered-config opts))
        (.then (fn [c] (->JsOrderedConsumer c (:codec ctx))))
        (.catch (fn [e] (throw (api-error e)))))))
