(ns nats-cljc.jetstream-test
  "Portable JetStream suite (ADR 0017): one `.cljc` source run on the JVM and Node
   (browser is CI-only, ADR 0010). Mirrors `core-test`'s connect/teardown envelope.
   The facade is aliased `jet` rather than `js`, which CLJS reserves for host
   interop."
  (:require #?(:clj  [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is async]])
            [nats-cljc.core :as nats]
            [nats-cljc.jetstream :as jet]
            [nats-cljc.jetstream.error :as jet-err]
            #?(:clj  [nats-cljc.jetstream.impl.jvm :as jet-jvm]
               :cljs [nats-cljc.jetstream.impl.js :as jet-js])
            #?(:cljs [promesa.core :as p]))
  #?(:clj (:import [io.nats.client Connection$Status])))

;; The anonymous server (ci/nats.conf) is the only leg with a jetstream{} block
;; (ADR 0017): the JetStream context is obtained here. TCP on the JVM, ws on CLJS
;; (ADR 0001).
(def ^:private server-url
  #?(:clj  "nats://127.0.0.1:4222"
     :cljs "ws://127.0.0.1:8080"))

;; The token server (ci/nats-token.conf) has NO jetstream{} block, so JetStream is
;; disabled there: it is the leg that proves (jetstream conn) verifies at entry on
;; both platforms (ADR 0017) — without the forced JVM round-trip, the JVM would
;; resolve a context here and only fail on the first operation.
(def ^:private token-server-url
  #?(:clj  "nats://127.0.0.1:4223"
     :cljs "ws://127.0.0.1:8081"))

(def ^:private token "s3cr3t-token")

;; Deep-module unit (ADR 0020), no server: the shared err_code → :type table is the
;; portable normalization seam both legs route server-issued JetStream errors
;; through. It maps the entry point's code and defaults the rest to the operational
;; catch-all; the numbers are identical on both legs, so one table covers both.
(deftest api-error-code-normalizes
  (is (= :jetstream-not-enabled (jet-err/api-error-type 10039))
      "err_code 10039 normalizes to :jetstream-not-enabled")
  (is (= :jetstream-api-error (jet-err/api-error-type 99999))
      "an unseeded code defaults to the operational catch-all :jetstream-api-error"))

;; Deep-module unit (ADR 0017), no server: the verify round-trip is a request, so
;; only a no-responder means "JetStream is off"; a transient timeout or a
;; closing/closed connection pass through as their core :type, identically on both
;; legs. These exercise the per-leg classifiers that carry that rule, since the
;; transient/closing edges are impractical to reproduce e2e.
#?(:clj
   (deftest jvm-verify-io-classifies-by-server-flag
     ;; jnats collapses a transient INFO timeout and a true no-responder into one
     ;; indistinguishable IOException; ServerInfo's round-trip-free `jetstream` flag
     ;; is the disambiguator (ADR 0017).
     (is (= :jetstream-not-enabled (jet-jvm/verify-io-type false))
         "server advertises no JetStream ⇒ the unanswered INFO means JetStream is off")
     (is (= :timeout (jet-jvm/verify-io-type true))
         "server advertises JetStream but the INFO went unanswered ⇒ transient, not a permanent disable")))

#?(:clj
   (deftest jvm-closing-classifies-by-status
     ;; The closing IOException (jnats' ensureNotClosing, thrown by the cheap
     ;; jetStream()/jetStreamManagement() construction before the round-trip) is
     ;; keyed on connection status, not message text (the op-state-error precedent).
     (is (= :connection-closed (jet-jvm/closing-type Connection$Status/CLOSED))
         "a closed connection ⇒ :connection-closed")
     (is (= :drained (jet-jvm/closing-type Connection$Status/CONNECTED))
         "a draining (not-yet-closed) connection ⇒ :drained")))

#?(:cljs
   (deftest cljs-verify-error-normalizes-by-name
     ;; nats.js rejects the INFO round-trip with distinct named errors; verify-error
     ;; normalizes each to its portable :type, and leaves anything else for the
     ;; slice that exercises it.
     (let [t (fn [name] (:type (ex-data (jet-js/verify-error #js {:name name}))))]
       (is (= :jetstream-not-enabled (t "JetStreamNotEnabled"))
           "JetStreamNotEnabled (no-responder) ⇒ :jetstream-not-enabled")
       (is (= :timeout (t "TimeoutError"))
           "a bare TimeoutError ⇒ :timeout (a transient blip, not a permanent disable)")
       (is (= :connection-closed (t "ClosedConnectionError"))
           "ClosedConnectionError ⇒ :connection-closed")
       (is (= :drained (t "DrainingConnectionError"))
           "DrainingConnectionError ⇒ :drained"))
     (let [e #js {:name "SomethingElse"}]
       (is (identical? e (jet-js/verify-error e))
           "an unmapped error passes through unchanged"))))

;; Test-only teardown, as in core-test: close the native client directly so its
;; threads / ws socket don't outlive the test.
(defn- close! [conn] (.close (:client conn)))

;; The connect / settle / teardown envelope (core-test's `with-conn`): JVM blocks
;; on connect, runs `(f conn)`, closes in a finally; CLJS awaits the promise
;; `(f conn)` returns, closes, then calls cljs.test's `done`.
#?(:clj
   (defn- with-conn [opts f]
     (let [conn @(nats/connect opts)]
       (try (f conn) (finally (close! conn)))))
   :cljs
   (defn- with-conn [opts done f]
     (-> (nats/connect opts)
         (p/then (fn [conn]
                   (-> (p/resolved nil)
                       (p/then (fn [_] (f conn)))
                       (p/finally (fn [_ _] (close! conn))))))
         (p/catch (fn [e] (is false (str "connect failed: " e))))
         (p/finally (fn [_ _] (done))))))

;; Capture the value a native promise REJECTS with at the non-blocking async-reject
;; seam (core-test's helper): `.whenComplete` hands back the BARE ex-info ADR 0006's
;; portable `(:type (ex-data e))` contract targets, not deref's ExecutionException
;; wrapper.
#?(:clj
   (defn- reject-reason [^java.util.concurrent.CompletableFuture cf]
     (let [a (promise)]
       (.whenComplete cf (reify java.util.function.BiConsumer
                           (accept [_ _ e] (deliver a e))))
       (deref a 5000 ::timeout))))

;; AC1 (ADR 0017): (jetstream conn) resolves to a single JetStream context against
;; a JetStream-enabled server, identically on both legs.
(deftest jetstream-resolves-to-a-context
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (jet/jetstream conn) 5000 ::timeout)]
           (is (not= ::timeout ctx) "(jetstream conn) resolves within 5s")
           (is (some? ctx) "(jetstream conn) resolves to a non-nil JetStream context"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (-> (jet/jetstream conn)
                    (p/then (fn [ctx]
                              (is (some? ctx)
                                  "(jetstream conn) resolves to a non-nil JetStream context")))
                    ;; Attribute an unexpected rejection to AC1, not to with-conn's
                    ;; outer "connect failed" catch (the connection already settled).
                    (p/catch (fn [e]
                               (is false (str "(jetstream conn) rejected unexpectedly: " e))))))))))

;; AC2 (ADR 0017/0020): against a JetStream-disabled server, (jetstream conn)
;; rejects with :jetstream-not-enabled at the handle, identically on both legs. On
;; the JVM this can only pass if the verify round-trip is forced — jnats'
;; jetStream()/jetStreamManagement() are cheap local constructions that would
;; otherwise resolve a context against a server that has no JetStream.
(deftest jetstream-not-enabled-rejects
  #?(:clj
     (with-conn {:servers [token-server-url] :auth {:token token}}
       (fn [conn]
         (let [e (reject-reason (jet/jetstream conn))]
           (is (= :jetstream-not-enabled (:type (ex-data e)))
               "(jetstream conn) rejects with :jetstream-not-enabled on a JS-disabled server"))))
     :cljs
     (async done
            (with-conn {:servers [token-server-url] :auth {:token token}} done
              (fn [conn]
                (-> (jet/jetstream conn)
                    (p/then (fn [_] (is false "expected (jetstream conn) to reject with :jetstream-not-enabled")))
                    (p/catch (fn [e]
                               (is (= :jetstream-not-enabled (:type (ex-data e)))
                                   "(jetstream conn) rejects with :jetstream-not-enabled on a JS-disabled server")))))))))
