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
