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
            ["@nats-io/jetstream" :as jetstream]))

;; The JetStream context (ADR 0017): one handle holding both nats.js' data-plane
;; (`jetstream`) and management-plane (`jetstreamManager`) objects. The native
;; client hands you the two separately; the portable surface collapses them into a
;; single value every JetStream operation flows through.
(defrecord JsJetStreamContext [js jsm])

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
          (.then (fn [jsm] (->JsJetStreamContext js jsm)))
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

(defn stream-info->map
  "Curate a nats.js StreamInfo into the normalized portable map (ADR 0020): the
   active `:config`, the `:created` timestamp (nats.js already hands back an
   ISO-8601 string, so no formatting — the JVM leg formats its ZonedDateTime to
   match), and a curated `:state`."
  [^js si]
  (let [st ^js (.-state si)]
    {:config  (stream-config->map (.-config si))
     :created (.-created si)
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

(extend-type JsJetStreamContext
  proto/StreamManager
  (-create-stream [ctx config]
    (let [streams ^js (.-streams ^js (:jsm ctx))]
      (-> (.add streams (->stream-config config))
          (.then (fn [si] (stream-info->map si)))
          (.catch (fn [e] (throw (api-error e)))))))
  (-stream-info [ctx name]
    (let [streams ^js (.-streams ^js (:jsm ctx))]
      (-> (.info streams name)
          (.then (fn [si] (stream-info->map si)))
          (.catch (fn [e] (throw (api-error e)))))))
  (-delete-stream [ctx name]
    (let [streams ^js (.-streams ^js (:jsm ctx))]
      (-> (.delete streams name)
          (.then (fn [_] nil))
          (.catch (fn [e] (throw (api-error e))))))))
