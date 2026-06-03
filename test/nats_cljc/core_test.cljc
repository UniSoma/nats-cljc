(ns nats-cljc.core-test
  (:require #?(:clj  [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is async]])
            [nats-cljc.core :as nats]
            [nats-cljc.codec :as codec]
            [nats-cljc.error :as error]
            [nats-cljc.protocol :as proto]
            ;; The server-driven status types (:lame-duck / :servers-changed) have
            ;; no portable client-side trigger, so they are asserted at the real
            ;; per-platform normalization seam (impl/deliver-status!) rather than
            ;; against a live server signal/cluster (ADR 0009/0010).
            #?(:clj  [nats-cljc.impl.jvm :as impl]
               :cljs [nats-cljc.impl.js :as impl])
            #?(:cljs [promesa.core :as p])))

;; Transport differs per platform (ADR 0001): TCP on the JVM, WebSocket on CLJS.
;; Only the URL forks; the connect/publish/subscribe calls below are identical.
(def ^:private server-url
  #?(:clj  "nats://127.0.0.1:4222"
     :cljs "ws://127.0.0.1:8080"))

;; Auth legs talk to auth-configured servers on distinct ports. NATS forces
;; anonymous, token, and operator/JWT auth onto separate servers, but static
;; users combine — so the user/password and nkey legs share one server
;; (ci/nats-users.conf). Credentials match ci/nats-token.conf and
;; ci/nats-users.conf.
(def ^:private token-server-url
  #?(:clj  "nats://127.0.0.1:4223"
     :cljs "ws://127.0.0.1:8081"))

(def ^:private token "s3cr3t-token")

;; Static-users server (ci/nats-users.conf): serves both the user/password and
;; the nkey legs. The nkey public key (in the conf) was generated once with nsc;
;; the matching seed and the user/password live here. Both clients derive the
;; public key from the seed; :nkey is the asserted identity (advanced-auth slice).
(def ^:private users-server-url
  #?(:clj  "nats://127.0.0.1:4224"
     :cljs "ws://127.0.0.1:8082"))

(def ^:private user "app")
(def ^:private pass "app-pass")

(def ^:private nkey "UCJMN7UXB6Q4V45Q3GIN5F444OGBFBTQ5GT7476YGRWYMUXC7TQLHYKU")
(def ^:private seed "SUAJVMLYQQWDDPLAK7IMHQJEGUWZNC66BRX5XN5ZPBTJ4C3BUHGAKVF5BM")

;; A well-formed user public key that does NOT match `seed` — exercises the
;; client-side :nkey/seed mismatch guard.
(def ^:private wrong-nkey "UBYCVQ2HDNHAGBAQG53QM7HELWN33SI2EO45D4RPZUCP47TPZD7TGP4M")

;; JWT (operator-mode) leg. The jwt server (ci/nats-jwt.conf) serves both the
;; {:jwt :seed} and {:creds} shapes; these credentials were generated once with
;; nsc (see docs/agents/running-tests.md) for account AuthAccount / user authuser.
(def ^:private jwt-server-url
  #?(:clj  "nats://127.0.0.1:4225"
     :cljs "ws://127.0.0.1:8083"))

(def ^:private user-jwt "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJZRTJRVjRaWlJPWk02U1RDSURYU0pEVzM0SEFGTkdHM0syRTRWMlUyNTJZVVBPTUJEWVRRIiwiaWF0IjoxNzgwMTgzMTMwLCJpc3MiOiJBQVFNRU9QNzZTMlBJVEVWTEI3RU5KUE9WVzdPQ1pWU0kzT1lNVUZUWk8yT0U3SVpRUTdZT0NXTCIsIm5hbWUiOiJhdXRodXNlciIsInN1YiI6IlVCWUNWUTJIRE5IQUdCQVFHNTNRTTdIRUxXTjMzU0kyRU80NUQ0UlBaVUNQNDdUUFpEN1RHUDRNIiwibmF0cyI6eyJwdWIiOnt9LCJzdWIiOnt9LCJzdWJzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.LRumrP4ruGQYD6Jq6NvMDDZTu97LibjjIM8DDdes-4DlCq_jcxK9Gof1F0ZWBArGk49G7sflgrjs4AcKPz68Ag")
(def ^:private jwt-seed "SUAD4J47I7H2GS22XV6DILB2VQ2OSVED6NGAJ4KLFMNN5SQUKANHXDX6NI")

;; The {:creds ...} shape takes the credentials file *content* as a string (the
;; browser has no filesystem). A creds file is just the user JWT + seed bundled
;; in NATS' delimited format, so build it from the fixtures above.
(def ^:private creds
  (str "-----BEGIN NATS USER JWT-----\n"
       user-jwt "\n"
       "------END NATS USER JWT------\n\n"
       "-----BEGIN USER NKEY SEED-----\n"
       jwt-seed "\n"
       "------END USER NKEY SEED------\n"))

(def ^:private subject "tracer.roundtrip")
(def ^:private payload {:hello "world" :n 42 :nested [1 2 {:k :v}]})

;; Per-call :codec override (ADR 0011). Distinct subjects per direction so the
;; shared server doesn't cross-feed.
(def ^:private codec-pub-sub-subject "codec.override.pubsub")
(def ^:private codec-request-subject "codec.override.request")

;; Delivery-semantics subjects (ADR 0007). Distinct per behavior so the shared
;; servers don't cross-feed between tests.
(def ^:private order-subject "delivery.order")
(def ^:private backpressure-subject "delivery.backpressure")
(def ^:private indep-a-subject "delivery.indep.a")
(def ^:private indep-b-subject "delivery.indep.b")

;; Queue-group subjects (ADR 0007). Distinct per behavior so the shared servers
;; don't cross-feed between tests.
(def ^:private queue-subject "delivery.queue")
(def ^:private queue-mixed-subject "delivery.queue.mixed")

;; Unsubscribe subjects (ADR 0012). Distinct per behavior so the shared server
;; doesn't cross-feed between tests.
(def ^:private unsub-subject "unsub.abrupt")
(def ^:private unsub-max-subject "unsub.max")

;; Request/reply subject (ADR 0002/0006). The responder subscribes here; the
;; requester's reply arrives on a per-request inbox the client manages.
(def ^:private request-subject "rr.request")

;; A subject no test ever subscribes — exercises the :no-responders failure mode.
(def ^:private no-responders-subject "rr.no-responders")

;; A subject a responder subscribes but never answers — exercises the :timeout
;; failure mode (responders exist, yet none reply within :timeout-ms).
(def ^:private silent-subject "rr.silent")

;; Headers subjects: case-sensitive string names -> one-or-more string values,
;; present only when set. Distinct per behavior so the shared server doesn't
;; cross-feed between tests.
(def ^:private headers-scalar-subject "headers.scalar")
(def ^:private headers-vector-subject "headers.vector")
(def ^:private headers-case-subject "headers.case")
(def ^:private headers-absent-subject "headers.absent")
(def ^:private headers-trim-subject "headers.trim")

;; Error-model subjects (ADR 0006). Each canonical Error :type is reproduced and
;; asserted with identical shape on both legs. Distinct per behavior so the
;; shared server doesn't cross-feed between tests.
(def ^:private throw-subject "err.handler-throw")
(def ^:private throw-fallback-subject "err.handler-throw.fallback")
(def ^:private codec-error-subject "err.codec")
(def ^:private slow-subject "err.slow")
(def ^:private payload-subject "err.payload")
(def ^:private closed-pub-subject "err.closed.pub")
(def ^:private drain-window-subject "err.drain")

;; A server nothing listens on — exercises the :connect-failed dial failure.
(def ^:private dead-server-url
  #?(:clj  "nats://127.0.0.1:1"
     :cljs "ws://127.0.0.1:1"))

;; Restricted user (ci/nats-users.conf): denied subscribe to "forbidden.>", so a
;; subscribe there draws a server-side Permissions Violation — the only portable
;; trigger for the connection-level :permissions-violation error.
(def ^:private restricted-user "restricted")
(def ^:private restricted-pass "restricted-pass")
(def ^:private forbidden-subject "forbidden.secret")

;; Test-only teardown: close the native client so jnats' non-daemon threads (and
;; the CLJS ws connection) don't outlive the test. A public close/drain is its
;; own slice; here we reach the record's client field directly.
(defn- close! [conn] (.close (:client conn)))

;; A `:bytes` subscriber observes the exact wire bytes a publisher produced;
;; UTF-8-decode them (via codec's public bridge) so the per-call-override
;; assertion reads as a string.
(def ^:private raw->str codec/bytes->str)

;; Test-only trigger for a real link drop: both native clients expose a public
;; force-reconnect that genuinely drops the socket (firing the native
;; DISCONNECTED / "disconnect" event) before re-establishing it. The reconnect
;; itself belongs to a sibling slice; here it is just how we provoke a faithful
;; :disconnected without cycling the server.
(defn- force-drop! [conn]
  #?(:clj  (.forceReconnect (:client conn))
     :cljs (.reconnect ^js (:client conn))))

;; Status events arrive on the native client's own schedule (a jnats listener
;; thread; the CLJS status() loop turn). A status collector + a JVM poll-until
;; helper let the assertions wait for a type rather than race it.
#?(:clj
   (defn- wait-for
     "Poll `pred` until truthy or `timeout-ms` elapses; return the last value."
     [pred timeout-ms]
     (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
       (loop []
         (or (pred)
             (when (< (System/currentTimeMillis) deadline)
               (Thread/sleep 20)
               (recur))))))
   :cljs
   (defn- wait-for
     "Promise resolving to true once `pred` is truthy (polling every 25ms), or
      false at `timeout-ms` — the async-friendly twin of the JVM poll."
     [pred timeout-ms]
     (p/create
      (fn [resolve _reject]
        (let [deadline (+ (js/Date.now) timeout-ms)]
          (letfn [(check []
                    (cond
                      (pred)                     (resolve true)
                      (< (js/Date.now) deadline) (js/setTimeout check 25)
                      :else                      (resolve false)))]
            (check)))))))

(defn- status-collector
  "An :on-status handler closing over an atom of the status maps it receives."
  []
  (let [seen (atom [])]
    [seen (fn [s] (swap! seen conj s))]))

(defn- error-collector
  "An :on-error handler closing over an atom of the (bare) errors it receives —
   the async-failure twin of `status-collector` (ADR 0006)."
  []
  (let [seen (atom [])]
    [seen (fn [e] (swap! seen conj e))]))

;; Portable ordering check over a status-collector's `seen` (CLJS vectors have no
;; .indexOf): true only when a `:type a` event appears before a `:type b` one —
;; so it also asserts both are present. Used for intra-platform ordering only
;; (the :on-status contract normalizes shape, not cross-platform cadence).
(defn- precedes? [seen a b]
  (let [types (mapv :type seen)
        idx   (fn [t] (first (keep-indexed (fn [i x] (when (= x t) i)) types)))
        ia    (idx a)
        ib    (idx b)]
    (boolean (and ia ib (< ia ib)))))

;; Test-only check that a subscription has been ended (by drain/close), through
;; the portable Subscription record's -active? — no native-handle reach-in.
(defn- sub-ended? [sub]
  (not (proto/-active? sub)))

;; The suite's connect / settle / teardown envelope — the connect+teardown
;; scaffolding every happy-path test repeated per leg. The per-platform fork stays
;; at the call site (each test still writes its own blocking vs async body); only
;; this boilerplate is shared. JVM blocks on connect, runs `(f conn)`, and closes
;; in a `finally`. CLJS connects, awaits the promise `(f conn)` returns, closes,
;; then calls cljs.test's `done`; a connect rejection fails the test via `is`
;; rather than hanging it. Tests that expect connect ITSELF to reject
;; (connect-failed, mismatched-nkey), never connect (the unit classifiers), or need
;; gated teardown (slow-consumer, drain-window) keep their own shape.
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

;; The five happy-path :auth shapes: each connects against the server configured
;; for it (token/user-pass/nkey share fixtures; jwt/creds share the operator
;; server). This table is the only thing that varies across the five deftests
;; below — they stay separately named (one named failure per shape) and share the
;; connect/teardown envelope. `[server auth message]` per case.
(def ^:private auth-cases
  {:token     [token-server-url {:token token}                 ":auth {:token ...} connects against a token-configured server"]
   :user-pass [users-server-url {:user user :pass pass}        ":auth {:user ... :pass ...} connects against a user/password-configured server"]
   :nkey      [users-server-url {:nkey nkey :seed seed}        ":auth {:nkey ... :seed ...} connects against an nkey-configured server"]
   :jwt       [jwt-server-url   {:jwt user-jwt :seed jwt-seed} ":auth {:jwt ... :seed ...} connects against a jwt-configured server"]
   :creds     [jwt-server-url   {:creds creds}                 ":auth {:creds ...} connects using credentials passed as string content"]})

(deftest connect-resolves-to-a-connection
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn] (is (some? conn) "connect resolves to a non-nil Connection")))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn] (is (some? conn) "connect resolves to a non-nil Connection"))))))

;; The five happy-path auth shapes (one named deftest each, sharing `auth-cases` +
;; the connect/teardown envelope): connecting against the matching server resolves
;; to a Connection.
(deftest auth-with-token-connects
  (let [[server auth msg] (:token auth-cases)]
    #?(:clj
       (with-conn {:servers [server] :auth auth} (fn [conn] (is (some? conn) msg)))
       :cljs
       (async done
              (with-conn {:servers [server] :auth auth} done (fn [conn] (is (some? conn) msg)))))))

(deftest auth-with-user-pass-connects
  (let [[server auth msg] (:user-pass auth-cases)]
    #?(:clj
       (with-conn {:servers [server] :auth auth} (fn [conn] (is (some? conn) msg)))
       :cljs
       (async done
              (with-conn {:servers [server] :auth auth} done (fn [conn] (is (some? conn) msg)))))))

(deftest auth-with-nkey-connects
  (let [[server auth msg] (:nkey auth-cases)]
    #?(:clj
       (with-conn {:servers [server] :auth auth} (fn [conn] (is (some? conn) msg)))
       :cljs
       (async done
              (with-conn {:servers [server] :auth auth} done (fn [conn] (is (some? conn) msg)))))))

(deftest auth-with-jwt-connects
  (let [[server auth msg] (:jwt auth-cases)]
    #?(:clj
       (with-conn {:servers [server] :auth auth} (fn [conn] (is (some? conn) msg)))
       :cljs
       (async done
              (with-conn {:servers [server] :auth auth} done (fn [conn] (is (some? conn) msg)))))))

(deftest auth-with-creds-connects
  (let [[server auth msg] (:creds auth-cases)]
    #?(:clj
       (with-conn {:servers [server] :auth auth} (fn [conn] (is (some? conn) msg)))
       :cljs
       (async done
              (with-conn {:servers [server] :auth auth} done (fn [conn] (is (some? conn) msg)))))))

(deftest auth-with-mismatched-nkey-rejects
  #?(:clj
     (let [fut (nats/connect {:servers [users-server-url]
                              :auth    {:nkey wrong-nkey :seed seed}})
           t   (try @fut nil
                    (catch java.util.concurrent.ExecutionException e
                      (:type (ex-data (.getCause e)))))]
       (is (= :auth-invalid t) "a mismatched :nkey/:seed pair rejects connect with :auth-invalid"))
     :cljs
     (async done
            (-> (nats/connect {:servers [users-server-url]
                               :auth    {:nkey wrong-nkey :seed seed}})
                (p/then (fn [_] (is false "expected the mismatched nkey to reject connect")))
                (p/catch (fn [e]
                           (is (= :auth-invalid (:type (ex-data e)))
                               "a mismatched :nkey/:seed pair rejects connect with :auth-invalid")))
                (p/finally (fn [_ _] (done)))))))

(deftest publish-subscribe-round-trip
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [received (promise)
               sub      (nats/subscribe conn subject #(deliver received %))
               pub-ret  (nats/publish conn subject payload)]
           (is (some? sub) "subscribe returns a Subscription synchronously")
           (is (nil? pub-ret) "publish returns nil")
           (let [msg (deref received 5000 ::timeout)]
             (is (not= ::timeout msg) "handler is invoked within 5s")
             (is (= subject (:subject msg)) "handler receives the subject")
             (is (= payload (:data msg)) "handler receives EDN-decoded :data")))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [received (p/deferred)
                      sub      (nats/subscribe conn subject #(p/resolve! received %))
                      pub-ret  (nats/publish conn subject payload)]
                  (is (some? sub) "subscribe returns a Subscription synchronously")
                  (is (nil? pub-ret) "publish returns nil")
                  (-> (p/timeout received 5000)
                      (p/then (fn [msg]
                                (is (= subject (:subject msg)) "handler receives the subject")
                                (is (= payload (:data msg)) "handler receives EDN-decoded :data"))))))))))

;; AC1: a per-call :codec overrides the connection default on BOTH publish and
;; subscribe. The connection defaults to :edn; the subscriber overrides to :bytes
;; (so it captures the raw wire bytes) and the publisher overrides to :string.
;; Only when both overrides take effect do the delivered bytes read as "hi" — the
;; :edn default would have produced the quoted "\"hi\"".
(deftest per-call-codec-overrides-publish-and-subscribe
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [received (promise)
               _        (nats/subscribe conn codec-pub-sub-subject #(deliver received %) {:codec :bytes})
               _        (nats/publish conn codec-pub-sub-subject "hi" {:codec :string})]
           (let [msg (deref received 5000 ::timeout)]
             (is (not= ::timeout msg) "handler is invoked within 5s")
             (is (= "hi" (raw->str (:data msg)))
                 "per-call :string on publish + :bytes on subscribe override the :edn default")))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [received (p/deferred)]
                  (nats/subscribe conn codec-pub-sub-subject #(p/resolve! received %) {:codec :bytes})
                  (nats/publish conn codec-pub-sub-subject "hi" {:codec :string})
                  (-> (p/timeout received 5000)
                      (p/then (fn [msg]
                                (is (= "hi" (raw->str (:data msg)))
                                    "per-call :string on publish + :bytes on subscribe override the :edn default"))))))))))

(deftest status-connected-delivered
  (let [[seen on-status] (status-collector)
        opts {:servers [server-url] :on-status on-status}]
    #?(:clj
       (with-conn opts
         (fn [_conn]
           (is (wait-for #(some #{{:type :connected}} @seen) 2000)
               ":connected reaches :on-status as a {:type ...} map")))
       :cljs
       (async done
              (with-conn opts done
                (fn [_conn]
                  (is (some #{{:type :connected}} @seen)
                      ":connected reaches :on-status as a {:type ...} map")))))))

(deftest close-settles-fires-closed-and-ends-subs
  (let [[seen on-status] (status-collector)
        opts {:servers [server-url] :on-status on-status}]
    #?(:clj
       (with-conn opts
         (fn [conn]
           (let [sub       (nats/subscribe conn subject (fn [_] nil))
                 close-ret (nats/close conn)]
             (is (not= ::timeout (deref close-ret 2000 ::timeout))
                 "close returns a promise that settles")
             (is (wait-for #(some #{{:type :closed}} @seen) 2000)
                 ":closed reaches :on-status")
             (is (wait-for #(sub-ended? sub) 2000)
                 "close ends the connection's subscriptions"))))
       :cljs
       (async done
              (with-conn opts done
                (fn [conn]
                  (let [sub       (nats/subscribe conn subject (fn [_] nil))
                        close-ret (nats/close conn)]
                    (is (some? close-ret) "close returns a promise")
                    (-> close-ret
                        (p/then (fn [_] (p/delay 100)))
                        (p/then (fn [_]
                                  (is (some #{{:type :closed}} @seen)
                                      ":closed reaches :on-status")
                                  (is (sub-ended? sub)
                                      "close ends the connection's subscriptions")))))))))))

(deftest disconnected-fires-on-drop
  (let [[seen on-status] (status-collector)
        opts {:servers [server-url] :on-status on-status}]
    #?(:clj
       (with-conn opts
         (fn [conn]
           (force-drop! conn)
           (is (wait-for #(some #{{:type :disconnected}} @seen) 5000)
               ":disconnected reaches :on-status on a real link drop")))
       :cljs
       (async done
              (with-conn opts done
                (fn [conn]
                  (force-drop! conn)
                  (-> (wait-for #(some #{{:type :disconnected}} @seen) 5000)
                      (p/then (fn [hit?]
                                (is hit? ":disconnected reaches :on-status on a real link drop"))))))))))

;; One real drop drives the whole disconnect->reconnecting->reconnected cycle, so
;; both reconnect events are asserted from a single connection. We wait for
;; :reconnected (the end of the cycle), then assert :reconnecting preceded it —
;; intra-platform ordering only, per the shape-not-cadence contract (ADR 0006):
;; the JVM synthesizes one :reconnecting per loss, nats.js emits one per dial
;; attempt, so the count is not asserted, only that the shape arrives in order.
(deftest reconnect-cycle-fires-reconnecting-then-reconnected
  (let [[seen on-status] (status-collector)
        opts {:servers   [server-url]
              :reconnect {:max 5 :wait-ms 50 :jitter-ms 10}
              :on-status on-status}]
    #?(:clj
       (with-conn opts
         (fn [conn]
           (force-drop! conn)
           (is (wait-for #(some #{{:type :reconnected}} @seen) 5000)
               ":reconnected reaches :on-status after a real drop when :reconnect is configured")
           (is (precedes? @seen :reconnecting :reconnected)
               ":reconnecting reaches :on-status and precedes :reconnected in the cycle")))
       :cljs
       (async done
              (with-conn opts done
                (fn [conn]
                  (force-drop! conn)
                  (-> (wait-for #(some #{{:type :reconnected}} @seen) 5000)
                      (p/then (fn [hit?]
                                (is hit? ":reconnected reaches :on-status after a real drop when :reconnect is configured")
                                (is (precedes? @seen :reconnecting :reconnected)
                                    ":reconnecting reaches :on-status and precedes :reconnected in the cycle"))))))))))

;; :reconnect {:max -1} is the unlimited sentinel — honored natively on both
;; platforms (jnats .maxReconnects(-1); nats.js maxReconnectAttempts -1). Can't
;; assert "never gives up", so this just locks that -1 passes through and connects.
(deftest reconnect-unlimited-connects
  #?(:clj
     (with-conn {:servers [server-url] :reconnect {:max -1}}
       (fn [conn] (is (some? conn) ":reconnect {:max -1} (unlimited) connects without error")))
     :cljs
     (async done
            (with-conn {:servers [server-url] :reconnect {:max -1}} done
              (fn [conn] (is (some? conn) ":reconnect {:max -1} (unlimited) connects without error"))))))

;; Translation pin (CLJS only): :reconnect {:max 0} must disable reconnection via
;; nats.js' `reconnect` boolean, NOT via maxReconnectAttempts 0 (which leaves
;; nats.js reconnecting). -1 is unlimited; a positive max is an attempt count.
;; This is the deterministic guard for finding #1 — a live drop can't test it,
;; since force-drop! forces a reconnect and bypasses the disable setting.
#?(:cljs
   (deftest reconnect-max-0-disables-via-reconnect-boolean
     (is (= {:reconnect false} (impl/with-reconnect {} {:max 0}))
         ":max 0 sets nats.js' `reconnect` false, not maxReconnectAttempts 0")
     (is (= {:maxReconnectAttempts -1} (impl/with-reconnect {} {:max -1}))
         ":max -1 (unlimited) passes through as maxReconnectAttempts -1")
     (is (= {:maxReconnectAttempts 5} (impl/with-reconnect {} {:max 5}))
         "a positive :max passes through as maxReconnectAttempts")
     (is (= {} (impl/with-reconnect {} {}))
         "an absent :max leaves nats.js' own default in place")))

;; Regression for the synthesis gate (JVM only — nats.js has no such gate):
;; disabling reconnection (:reconnect {:max 0} → reconnect? false) must NOT
;; silence the status spine. Driving the real ConnectionListener with reconnect?
;; false and asserting a non-reconnect event still arrives locks that delivery is
;; never short-circuited by the gate.
#?(:clj
   (deftest status-delivered-when-reconnect-disabled
     (let [[seen on-status]                     (status-collector)
           ^io.nats.client.ConnectionListener l (impl/status-listener on-status false)]
       (.connectionEvent l nil io.nats.client.ConnectionListener$Events/CLOSED)
       (is (some #{{:type :closed}} @seen)
           "status events are delivered even when reconnection is disabled (reconnect? false)"))))

;; The server-driven types have no portable client-side trigger (a lame-duck
;; needs a server signal; a server-list change needs a cluster), so they are
;; driven through the real per-platform wiring with the native event jnats/nats.js
;; would emit: the production ConnectionListener on the JVM, and deliver-status!
;; on CLJS (the status() pump is already live-exercised by disconnected-fires-on-drop;
;; a fake async-iterable would be disproportionate). The native event forks per
;; platform; the canonical {:type ...} result does not.
(deftest lame-duck-normalized
  (let [[seen on-status] (status-collector)]
    #?(:clj  (let [^io.nats.client.ConnectionListener l (impl/status-listener on-status true)]
               (.connectionEvent l nil io.nats.client.ConnectionListener$Events/LAME_DUCK))
       :cljs (impl/deliver-status! on-status #js {:type "ldm"}))
    (is (some #{{:type :lame-duck}} @seen)
        "a server-driven lame-duck event normalizes to {:type :lame-duck} on :on-status")))

(deftest servers-changed-normalized
  (let [[seen on-status] (status-collector)]
    #?(:clj  (let [^io.nats.client.ConnectionListener l (impl/status-listener on-status true)]
               (.connectionEvent l nil io.nats.client.ConnectionListener$Events/DISCOVERED_SERVERS))
       :cljs (impl/deliver-status! on-status #js {:type "update"}))
    (is (some #{{:type :servers-changed}} @seen)
        "a server-driven server-list change normalizes to {:type :servers-changed} on :on-status")))

(deftest flush-settles
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (nats/publish conn subject payload)
         (let [flush-ret (nats/flush conn)]
           (is (not= ::timeout (deref flush-ret 5000 ::timeout))
               "flush returns a promise that settles"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (nats/publish conn subject payload)
                (let [flush-ret (nats/flush conn)]
                  (is (some? flush-ret) "flush returns a promise")
                  ;; reaching this then means the promise settled
                  (-> flush-ret
                      (p/then (fn [_] (is true "flush settles"))))))))))

(deftest drain-connection-settles-and-ends-subs
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [sub       (nats/subscribe conn subject (fn [_] nil))
               drain-ret (nats/drain conn)]
           (is (not= ::timeout (deref drain-ret 5000 ::timeout))
               "drain returns a promise that settles")
           (is (wait-for #(sub-ended? sub) 2000)
               "drain ends the connection's subscriptions"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [sub       (nats/subscribe conn subject (fn [_] nil))
                      drain-ret (nats/drain conn)]
                  (is (some? drain-ret) "drain returns a promise")
                  (-> drain-ret
                      (p/then (fn [_] (wait-for #(sub-ended? sub) 2000)))
                      (p/then (fn [ended?]
                                (is ended? "drain ends the connection's subscriptions"))))))))))

(deftest drain-subscription-settles-and-ends-only-it
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [sub-a     (nats/subscribe conn (str subject ".a") (fn [_] nil))
               sub-b     (nats/subscribe conn (str subject ".b") (fn [_] nil))
               drain-ret (nats/drain sub-a)]
           (is (not= ::timeout (deref drain-ret 5000 ::timeout))
               "subscription drain returns a promise that settles")
           (is (wait-for #(sub-ended? sub-a) 2000)
               "draining a subscription ends it")
           (is (not (sub-ended? sub-b))
               "draining one subscription leaves the connection's others active"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [sub-a     (nats/subscribe conn (str subject ".a") (fn [_] nil))
                      sub-b     (nats/subscribe conn (str subject ".b") (fn [_] nil))
                      drain-ret (nats/drain sub-a)]
                  (is (some? drain-ret) "subscription drain returns a promise")
                  (-> drain-ret
                      (p/then (fn [_] (wait-for #(sub-ended? sub-a) 2000)))
                      (p/then (fn [ended?]
                                (is ended? "draining a subscription ends it")
                                (is (not (sub-ended? sub-b))
                                    "draining one subscription leaves the connection's others active"))))))))))

;; Unsubscribe (ADR 0012) is the abrupt sibling of drain: it stops the
;; subscription synchronously, returning nil, and DROPS not-yet-delivered
;; messages (where drain flushes the backlog). Deliver one message to prove the
;; sub is live, unsubscribe, then publish more and flush — same-connection
;; ordering guarantees the server processes the UNSUB before the later PUB, so a
;; still-1 count after the flush settles is a deterministic "no further delivery".
(deftest unsubscribe-stops-delivery-abruptly-and-returns-nil
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [received (atom [])
               sub      (nats/subscribe conn unsub-subject (fn [msg] (swap! received conj (:data msg))))]
           (nats/publish conn unsub-subject :one)
           (is (wait-for #(= 1 (count @received)) 2000) "the first message is delivered")
           (is (nil? (nats/unsubscribe sub)) "unsubscribe returns nil synchronously")
           (is (wait-for #(sub-ended? sub) 2000) "unsubscribe ends the subscription")
           (nats/publish conn unsub-subject :two)
           @(nats/flush conn)
           (is (= [:one] @received) "no message is delivered after unsubscribe"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [received (atom [])
                      sub      (nats/subscribe conn unsub-subject (fn [msg] (swap! received conj (:data msg))))]
                  (nats/publish conn unsub-subject :one)
                  (-> (wait-for #(= 1 (count @received)) 2000)
                      (p/then (fn [hit?]
                                (is hit? "the first message is delivered")
                                (is (nil? (nats/unsubscribe sub)) "unsubscribe returns nil synchronously")
                                (wait-for #(sub-ended? sub) 2000)))
                      (p/then (fn [ended?]
                                (is ended? "unsubscribe ends the subscription")
                                (nats/publish conn unsub-subject :two)
                                (nats/flush conn)))
                      (p/then (fn [_]
                                (is (= [:one] @received) "no message is delivered after unsubscribe"))))))))))

;; `(unsubscribe sub max)` auto-unsubscribes once the subscription has received
;; `max` messages over its lifetime (ADR 0012). Arm max=3 before any publish,
;; fire ten on the same connection (so SUB, the maxed UNSUB, and the PUBs are
;; ordered server-side), and exactly three are delivered before the sub ends —
;; the rest are dropped. native-confirmed identical on both clients.
(deftest unsubscribe-max-auto-unsubscribes-after-n
  (let [max 3 n 10]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [received (atom [])
                 sub      (nats/subscribe conn unsub-max-subject (fn [msg] (swap! received conj (:data msg))))]
             (is (nil? (nats/unsubscribe sub max)) "unsubscribe with max returns nil synchronously")
             (dotimes [i n] (nats/publish conn unsub-max-subject i))
             @(nats/flush conn)
             (is (wait-for #(= max (count @received)) 2000)
                 "exactly max messages are delivered")
             (is (wait-for #(sub-ended? sub) 2000)
                 "reaching max ends the subscription")
             (is (= [0 1 2] @received) "the first max messages are delivered, the rest dropped"))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [received (atom [])
                        sub      (nats/subscribe conn unsub-max-subject (fn [msg] (swap! received conj (:data msg))))]
                    (is (nil? (nats/unsubscribe sub max)) "unsubscribe with max returns nil synchronously")
                    (dotimes [i n] (nats/publish conn unsub-max-subject i))
                    (-> (nats/flush conn)
                        (p/then (fn [_] (wait-for #(= max (count @received)) 2000)))
                        (p/then (fn [hit?]
                                  (is hit? "exactly max messages are delivered")
                                  (wait-for #(sub-ended? sub) 2000)))
                        (p/then (fn [ended?]
                                  (is ended? "reaching max ends the subscription")
                                  (is (= [0 1 2] @received) "the first max messages are delivered, the rest dropped")))))))))))

;; An invalid `max` is caller misuse the facade rejects synchronously with a
;; portable `:type :invalid-max` on every platform (parallel to
;; :invalid-max-pending), before touching the native sub. Two ways to be invalid:
;; non-positive (0/negative would arm a zero/sentinel native auto-unsubscribe and
;; stop the sub at the wrong time) and above Integer/MAX_VALUE (the JVM native
;; overload takes an `int`, so a larger value would throw an uncaught
;; ArithmeticException there while succeeding on JS — reject it the same on both).
(deftest unsubscribe-invalid-max-rejected
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [sub (nats/subscribe conn unsub-max-subject (fn [_]))]
           (is (= :invalid-max
                  (try (nats/unsubscribe sub 0)
                       :no-throw
                       (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
               "max 0 is rejected as :invalid-max, not armed as a zero auto-unsubscribe")
           (is (= :invalid-max
                  (try (nats/unsubscribe sub (inc 2147483647))
                       :no-throw
                       (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
               "max above Integer/MAX_VALUE is rejected as :invalid-max, not thrown as ArithmeticException"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [sub (nats/subscribe conn unsub-max-subject (fn [_]))]
                  (is (= :invalid-max
                         (try (nats/unsubscribe sub 0)
                              :no-throw
                              (catch :default e (:type (ex-data e)))))
                      "max 0 is rejected as :invalid-max, not armed as a zero auto-unsubscribe")
                  (is (= :invalid-max
                         (try (nats/unsubscribe sub (inc 2147483647))
                              :no-throw
                              (catch :default e (:type (ex-data e)))))
                      "max above Integer/MAX_VALUE is rejected the same as on JVM")))))))

;; Unsubscribe is idempotent (ADR 0012): unsubscribing an already-ended
;; subscription — ended by a prior unsubscribe, a drain, or the connection
;; closing — returns nil and does nothing, rather than erroring. nats.js is
;; already a no-op on a closed sub; jnats' Dispatcher.unsubscribe throws
;; IllegalStateException, which the JVM impl swallows. Three teardown paths, all
;; the same silent no-op.
(deftest unsubscribe-is-idempotent
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [sub-a (nats/subscribe conn (str unsub-subject ".idem.a") (fn [_]))
               sub-b (nats/subscribe conn (str unsub-subject ".idem.b") (fn [_]))]
           (nats/unsubscribe sub-a)
           (is (nil? (nats/unsubscribe sub-a)) "unsubscribe after a prior unsubscribe is a no-op")
           @(nats/drain sub-b)
           (is (nil? (nats/unsubscribe sub-b)) "unsubscribe after drain is a no-op")
           @(nats/close conn)
           (is (nil? (nats/unsubscribe sub-a)) "unsubscribe after connection close is a no-op"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [sub-a (nats/subscribe conn (str unsub-subject ".idem.a") (fn [_]))
                      sub-b (nats/subscribe conn (str unsub-subject ".idem.b") (fn [_]))]
                  (nats/unsubscribe sub-a)
                  (is (nil? (nats/unsubscribe sub-a)) "unsubscribe after a prior unsubscribe is a no-op")
                  (-> (nats/drain sub-b)
                      (p/then (fn [_]
                                (is (nil? (nats/unsubscribe sub-b)) "unsubscribe after drain is a no-op")
                                (nats/close conn)))
                      (p/then (fn [_]
                                (is (nil? (nats/unsubscribe sub-a)) "unsubscribe after connection close is a no-op"))))))))))

;; Delivery semantics (ADR 0007). With a SINGLE publisher, a subscription sees that
;; publisher's messages in publish order, one at a time — that per-publisher order is
;; the guarantee core NATS gives (it does NOT order across different publishers, so
;; there is deliberately no cross-publisher ordering test: NATS leaves that
;; interleaving unspecified, so any assertion on it would be flaky). SUB and the PUBs
;; share one connection, so the server registers the subscription before any message
;; arrives — no flush needed.
(deftest single-subscription-delivers-in-order
  (let [n 50]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [order (atom [])]
             (nats/subscribe conn order-subject (fn [msg] (swap! order conj (:data msg))))
             (dotimes [i n] (nats/publish conn order-subject i))
             (is (wait-for #(= n (count @order)) 5000) "all messages delivered")
             (is (= (vec (range n)) @order)
                 "a single subscription delivers one publisher's messages in publish order"))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [order (atom [])]
                    (nats/subscribe conn order-subject (fn [msg] (swap! order conj (:data msg))))
                    (dotimes [i n] (nats/publish conn order-subject i))
                    (-> (wait-for #(= n (count @order)) 5000)
                        (p/then (fn [hit?]
                                  (is hit? "all messages delivered")
                                  (is (= (vec (range n)) @order)
                                      "a single subscription delivers one publisher's messages in publish order")))))))))))

;; Promise-return backpressure (ADR 0007): a handler that returns a pending
;; promise suspends delivery of the next message until it settles; a non-promise
;; return delivers immediately. The handler returns a gate promise only for the
;; first message, so the second must wait until the gate settles. The gate is a
;; platform-native promise the test controls — a CompletableFuture on the JVM, a
;; promesa deferred on CLJS — exactly the shape a real async handler would return.
(deftest pending-promise-handler-applies-backpressure
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [order (atom [])
               gate  (java.util.concurrent.CompletableFuture.)]
           (nats/subscribe conn backpressure-subject
                           (fn [msg]
                             (swap! order conj (:data msg))
                             (when (= 1 (:data msg)) gate)))
           (nats/publish conn backpressure-subject 1)
           (nats/publish conn backpressure-subject 2)
           (is (wait-for #(= [1] @order) 5000) "the first message is delivered")
           (Thread/sleep 300)
           (is (= [1] @order)
               "a pending-promise handler suspends delivery of the next message")
           (.complete gate nil)
           (is (wait-for #(= [1 2] @order) 5000)
               "the next message is delivered once the promise settles"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [order (atom [])
                      gate  (p/deferred)]
                  (nats/subscribe conn backpressure-subject
                                  (fn [msg]
                                    (swap! order conj (:data msg))
                                    (when (= 1 (:data msg)) gate)))
                  (nats/publish conn backpressure-subject 1)
                  (nats/publish conn backpressure-subject 2)
                  (-> (wait-for #(= [1] @order) 5000)
                      (p/then (fn [hit?]
                                (is hit? "the first message is delivered")))
                      (p/then (fn [_] (p/delay 300)))
                      (p/then (fn [_]
                                (is (= [1] @order)
                                    "a pending-promise handler suspends delivery of the next message")
                                (p/resolve! gate nil)))
                      (p/then (fn [_] (wait-for #(= [1 2] @order) 5000)))
                      (p/then (fn [hit?]
                                (is hit? "the next message is delivered once the promise settles"))))))))))

;; No cross-subscription coupling (ADR 0007): backpressure is per-subscription,
;; and a handler that returns a pending promise must never block the underlying
;; client thread or event loop. Sub-A is held on a pending-promise handler while
;; sub-B keeps delivering — so a backpressured subscription neither stalls another
;; nor freezes the loop. The suite asserts nothing about A-vs-B ordering: there is
;; no cross-subscription ordering guarantee to assume.
(deftest subscriptions-are-independent
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [a-order (atom [])
               b-order (atom [])
               gate    (java.util.concurrent.CompletableFuture.)]
           (nats/subscribe conn indep-a-subject
                           (fn [msg]
                             (swap! a-order conj (:data msg))
                             (when (= :a1 (:data msg)) gate)))
           (nats/subscribe conn indep-b-subject
                           (fn [msg] (swap! b-order conj (:data msg))))
           (nats/publish conn indep-a-subject :a1)
           (nats/publish conn indep-a-subject :a2)
           (nats/publish conn indep-b-subject :b1)
           (is (wait-for #(and (= [:a1] @a-order) (= [:b1] @b-order)) 5000)
               "a second subscription delivers while the first is backpressured (no cross-subscription coupling)")
           (.complete gate nil)
           (is (wait-for #(= [:a1 :a2] @a-order) 5000)
               "the backpressured subscription resumes once its promise settles"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [a-order (atom [])
                      b-order (atom [])
                      gate    (p/deferred)]
                  (nats/subscribe conn indep-a-subject
                                  (fn [msg]
                                    (swap! a-order conj (:data msg))
                                    (when (= :a1 (:data msg)) gate)))
                  (nats/subscribe conn indep-b-subject
                                  (fn [msg] (swap! b-order conj (:data msg))))
                  (nats/publish conn indep-a-subject :a1)
                  (nats/publish conn indep-a-subject :a2)
                  (nats/publish conn indep-b-subject :b1)
                  (-> (wait-for #(and (= [:a1] @a-order) (= [:b1] @b-order)) 5000)
                      (p/then (fn [hit?]
                                (is hit? "a second subscription delivers while the first is backpressured (no cross-subscription coupling)")
                                (p/resolve! gate nil)))
                      (p/then (fn [_] (wait-for #(= [:a1 :a2] @a-order) 5000)))
                      (p/then (fn [hit?]
                                (is hit? "the backpressured subscription resumes once its promise settles"))))))))))

;; Queue groups / competing consumers (ADR 0007). Subscriptions sharing a :queue
;; group on one subject compete: the server load-balances each matching message to
;; exactly one member, so the group's combined delivery is the full stream with no
;; duplication (a disjoint share). All subs and the pubs share one connection, so
;; the server registers the subscriptions before any message arrives — no flush.
(deftest queue-group-load-balances
  (let [n 50]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [a (atom [])
                 b (atom [])]
             (nats/subscribe conn queue-subject (fn [msg] (swap! a conj (:data msg))) {:queue "workers"})
             (nats/subscribe conn queue-subject (fn [msg] (swap! b conj (:data msg))) {:queue "workers"})
             (dotimes [i n] (nats/publish conn queue-subject i))
             (is (wait-for #(= n (+ (count @a) (count @b))) 5000)
                 "every published message reaches the queue group")
             (is (= (vec (range n)) (vec (sort (concat @a @b))))
                 "each message reaches exactly one member — combined delivery is the full set, no duplication")
             (is (and (seq @a) (seq @b))
                 "both members receive a share (the server load-balances)"))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [a (atom [])
                        b (atom [])]
                    (nats/subscribe conn queue-subject (fn [msg] (swap! a conj (:data msg))) {:queue "workers"})
                    (nats/subscribe conn queue-subject (fn [msg] (swap! b conj (:data msg))) {:queue "workers"})
                    (dotimes [i n] (nats/publish conn queue-subject i))
                    (-> (wait-for #(= n (+ (count @a) (count @b))) 5000)
                        (p/then (fn [hit?]
                                  (is hit? "every published message reaches the queue group")
                                  (is (= (vec (range n)) (vec (sort (concat @a @b))))
                                      "each message reaches exactly one member — combined delivery is the full set, no duplication")
                                  (is (and (seq @a) (seq @b))
                                      "both members receive a share (the server load-balances)")))))))))))

;; A queue group only competes within itself: a plain (non-queue) subscription on
;; the same subject is a separate interest, so it still receives every message
;; while the group splits the same stream (ADR 0007). One connection registers all
;; three subscriptions before any publish — no flush needed.
(deftest non-queue-subscription-receives-all-alongside-a-queue-group
  (let [n 50]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [a   (atom [])
                 b   (atom [])
                 all (atom [])]
             (nats/subscribe conn queue-mixed-subject (fn [msg] (swap! a conj (:data msg))) {:queue "workers"})
             (nats/subscribe conn queue-mixed-subject (fn [msg] (swap! b conj (:data msg))) {:queue "workers"})
             (nats/subscribe conn queue-mixed-subject (fn [msg] (swap! all conj (:data msg))))
             (dotimes [i n] (nats/publish conn queue-mixed-subject i))
             (is (wait-for #(and (= n (count @all)) (= n (+ (count @a) (count @b)))) 5000)
                 "the plain subscription and the queue group both finish")
             (is (= (vec (range n)) (vec (sort @all)))
                 "a non-queue subscription on the same subject receives every message")
             (is (= (vec (range n)) (vec (sort (concat @a @b))))
                 "the queue group still splits the same stream into a disjoint share"))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [a   (atom [])
                        b   (atom [])
                        all (atom [])]
                    (nats/subscribe conn queue-mixed-subject (fn [msg] (swap! a conj (:data msg))) {:queue "workers"})
                    (nats/subscribe conn queue-mixed-subject (fn [msg] (swap! b conj (:data msg))) {:queue "workers"})
                    (nats/subscribe conn queue-mixed-subject (fn [msg] (swap! all conj (:data msg))))
                    (dotimes [i n] (nats/publish conn queue-mixed-subject i))
                    (-> (wait-for #(and (= n (count @all)) (= n (+ (count @a) (count @b)))) 5000)
                        (p/then (fn [hit?]
                                  (is hit? "the plain subscription and the queue group both finish")
                                  (is (= (vec (range n)) (vec (sort @all)))
                                      "a non-queue subscription on the same subject receives every message")
                                  (is (= (vec (range n)) (vec (sort (concat @a @b))))
                                      "the queue group still splits the same stream into a disjoint share")))))))))))

;; Request/reply over the core round-trip (ADR 0002/0006). A responder subscribes
;; to `request-subject` and answers via `reply` (sugar that publishes to the
;; request's `:reply` inbox); the requester's `request` resolves to the decoded
;; reply message. `reply` returns nil; every delivered/resolved message carries a
;; `:reply` key (nil when there is no reply subject).
(deftest request-reply-round-trip
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [replied (promise)]
           (nats/subscribe conn request-subject
                           (fn [msg]
                             (deliver replied (nats/reply conn msg {:pong (:n (:data msg))}))))
           (let [resp (deref (nats/request conn request-subject {:n 7} {:timeout-ms 5000}) 5000 ::timeout)]
             (is (not= ::timeout resp) "request resolves within 5s")
             (is (= {:pong 7} (:data resp)) "request resolves to the decoded reply payload")
             (is (contains? resp :reply) "resolved message always carries a :reply key")
             (is (nil? (deref replied 5000 ::unset)) "reply returns nil")))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [replied (atom ::unset)]
                  (nats/subscribe conn request-subject
                                  (fn [msg]
                                    (reset! replied (nats/reply conn msg {:pong (:n (:data msg))}))))
                  (-> (p/timeout (nats/request conn request-subject {:n 7} {:timeout-ms 5000}) 5000)
                      (p/then (fn [resp]
                                (is (= {:pong 7} (:data resp)) "request resolves to the decoded reply payload")
                                (is (contains? resp :reply) "resolved message always carries a :reply key")
                                (is (nil? @replied) "reply returns nil"))))))))))

;; AC1: a per-call :codec overrides the connection default on request, and reply
;; gains an opts arity so the response leg matches. The connection defaults to
;; :edn; the requester sends "hi" with :codec :string and the responder both
;; subscribes and replies with :codec :string. Two legs are asserted so neither
;; encode override can silently regress:
;;   - request-encode: the responder captures and asserts it received "hi".
;;     :string encodes it unquoted; the :edn default would have sent "\"hi\"",
;;     which the subscriber's :string decodes to the literal "\"hi\"" — failing
;;     the assertion. (A value like 42 is useless here: (pr-str 42) = (str 42),
;;     so :edn and :string are byte-identical and the override leg goes untested.)
;;   - reply-encode + request-decode: the requester reads back the plain "pong".
;;     The :edn default would have decoded the reply bytes to the symbol `pong`,
;;     or encoded the reply as "\"pong\"".
;; The responder asserts on the main thread (via a promise/deferred), not inside
;; the handler, so the report lands on the test thread.
(deftest per-call-codec-overrides-request-and-reply
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [received (promise)]
           (nats/subscribe conn codec-request-subject
                           (fn [msg]
                             (deliver received (:data msg))
                             (nats/reply conn msg "pong" {:codec :string}))
                           {:codec :string})
           (let [resp (deref (nats/request conn codec-request-subject "hi" {:codec :string :timeout-ms 5000}) 5000 ::timeout)]
             (is (not= ::timeout resp) "request resolves within 5s")
             (is (= "hi" (deref received 5000 ::timeout))
                 "the per-call :string on request encodes the body unquoted (the :edn default would have sent \"\\\"hi\\\"\")")
             (is (= "pong" (:data resp))
                 "request and reply honor the per-call :string over the :edn default")))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [received (p/deferred)]
                  (nats/subscribe conn codec-request-subject
                                  (fn [msg]
                                    (p/resolve! received (:data msg))
                                    (nats/reply conn msg "pong" {:codec :string}))
                                  {:codec :string})
                  (-> (p/timeout (nats/request conn codec-request-subject "hi" {:codec :string :timeout-ms 5000}) 5000)
                      (p/then (fn [resp]
                                (is (= "pong" (:data resp))
                                    "request and reply honor the per-call :string over the :edn default")))
                      (p/then (fn [_] (p/timeout received 5000)))
                      (p/then (fn [recvd]
                                (is (= "hi" recvd)
                                    "the per-call :string on request encodes the body unquoted (the :edn default would have sent \"\\\"hi\\\"\")"))))))))))

;; No-responders failure mode (ADR 0006): a request to a subject nobody
;; subscribes rejects fast with a normalized `:type :no-responders`, distinct
;; from a timeout — the server reports it as soon as it sees no subscribers.
(deftest request-no-responders-rejects
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [t (try (deref (nats/request conn no-responders-subject {:n 1} {:timeout-ms 2000}) 5000 ::timeout)
                      nil
                      (catch java.util.concurrent.ExecutionException e
                        (:type (ex-data (.getCause e)))))]
           (is (= :no-responders t)
               "a request to a subject with no subscribers rejects with :no-responders"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (-> (nats/request conn no-responders-subject {:n 1} {:timeout-ms 2000})
                    (p/then (fn [_] (is false "expected the request to reject with :no-responders")))
                    (p/catch (fn [e]
                               (is (= :no-responders (:type (ex-data e)))
                                   "a request to a subject with no subscribers rejects with :no-responders")))))))))

;; Timeout failure mode (ADR 0006): responders exist (a subscriber is registered,
;; confirmed via flush) but none answer within :timeout-ms, so the request
;; rejects with a normalized `:type :timeout` — distinct from :no-responders.
(deftest request-timeout-rejects
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (nats/subscribe conn silent-subject (fn [_msg] nil))
         @(nats/flush conn)
         (let [t (try (deref (nats/request conn silent-subject {:n 1} {:timeout-ms 300}) 5000 ::timeout)
                      nil
                      (catch java.util.concurrent.ExecutionException e
                        (:type (ex-data (.getCause e)))))]
           (is (= :timeout t)
               "a request whose responders never answer within :timeout-ms rejects with :timeout"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (nats/subscribe conn silent-subject (fn [_msg] nil))
                (-> (nats/flush conn)
                    (p/then (fn [_] (nats/request conn silent-subject {:n 1} {:timeout-ms 300})))
                    (p/then (fn [_] (is false "expected the request to reject with :timeout")))
                    (p/catch (fn [e]
                               (is (= :timeout (:type (ex-data e)))
                                   "a request whose responders never answer within :timeout-ms rejects with :timeout")))))))))

;; Headers round-trip (CONTEXT: Headers). A scalar header value is accepted on
;; publish and arrives normalized to a one-element vector of strings under
;; :headers — the same shape on every platform.
(deftest headers-scalar-delivers-as-one-element-vector
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [received (promise)]
           (nats/subscribe conn headers-scalar-subject #(deliver received %))
           (nats/publish conn headers-scalar-subject payload {:headers {"X-Trace" "abc"}})
           (let [msg (deref received 5000 ::timeout)]
             (is (not= ::timeout msg) "handler is invoked within 5s")
             (is (= {"X-Trace" ["abc"]} (:headers msg))
                 "a scalar header value is delivered as a one-element vector of strings")))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [received (p/deferred)]
                  (nats/subscribe conn headers-scalar-subject #(p/resolve! received %))
                  (nats/publish conn headers-scalar-subject payload {:headers {"X-Trace" "abc"}})
                  (-> (p/timeout received 5000)
                      (p/then (fn [msg]
                                (is (= {"X-Trace" ["abc"]} (:headers msg))
                                    "a scalar header value is delivered as a one-element vector of strings"))))))))))

;; When nothing was published under :headers, the delivered message carries no
;; :headers key at all — absence, not an empty map (CONTEXT: Headers).
(deftest headers-absent-when-none-set
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [received (promise)]
           (nats/subscribe conn headers-absent-subject #(deliver received %))
           (nats/publish conn headers-absent-subject payload)
           (let [msg (deref received 5000 ::timeout)]
             (is (not= ::timeout msg) "handler is invoked within 5s")
             (is (not (contains? msg :headers))
                 ":headers is absent from the delivered map when none were set")))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [received (p/deferred)]
                  (nats/subscribe conn headers-absent-subject #(p/resolve! received %))
                  (nats/publish conn headers-absent-subject payload)
                  (-> (p/timeout received 5000)
                      (p/then (fn [msg]
                                (is (not (contains? msg :headers))
                                    ":headers is absent from the delivered map when none were set"))))))))))

;; Header names are case-sensitive: two names differing only in case are distinct
;; entries that survive the round-trip without collapsing (CONTEXT: Headers).
(deftest headers-names-are-case-sensitive
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [received (promise)]
           (nats/subscribe conn headers-case-subject #(deliver received %))
           (nats/publish conn headers-case-subject payload {:headers {"X-Trace" "upper" "x-trace" "lower"}})
           (let [msg (deref received 5000 ::timeout)]
             (is (not= ::timeout msg) "handler is invoked within 5s")
             (is (= {"X-Trace" ["upper"] "x-trace" ["lower"]} (:headers msg))
                 "names differing only in case are preserved as distinct entries")))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [received (p/deferred)]
                  (nats/subscribe conn headers-case-subject #(p/resolve! received %))
                  (nats/publish conn headers-case-subject payload {:headers {"X-Trace" "upper" "x-trace" "lower"}})
                  (-> (p/timeout received 5000)
                      (p/then (fn [msg]
                                (is (= {"X-Trace" ["upper"] "x-trace" ["lower"]} (:headers msg))
                                    "names differing only in case are preserved as distinct entries"))))))))))

;; A vector-valued header carries multiple values for one name; they arrive
;; unchanged as a vector of strings, in order (CONTEXT: Headers).
(deftest headers-vector-delivers-unchanged
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [received (promise)]
           (nats/subscribe conn headers-vector-subject #(deliver received %))
           (nats/publish conn headers-vector-subject payload {:headers {"X-Tag" ["a" "b"]}})
           (let [msg (deref received 5000 ::timeout)]
             (is (not= ::timeout msg) "handler is invoked within 5s")
             (is (= {"X-Tag" ["a" "b"]} (:headers msg))
                 "a vector-valued header is delivered unchanged as a vector of strings")))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [received (p/deferred)]
                  (nats/subscribe conn headers-vector-subject #(p/resolve! received %))
                  (nats/publish conn headers-vector-subject payload {:headers {"X-Tag" ["a" "b"]}})
                  (-> (p/timeout received 5000)
                      (p/then (fn [msg]
                                (is (= {"X-Tag" ["a" "b"]} (:headers msg))
                                    "a vector-valued header is delivered unchanged as a vector of strings"))))))))))

;; Surrounding whitespace on a header value is insignificant and stripped on
;; delivery, identically on every platform (CONTEXT: Headers). nats.js trims
;; natively; decode-msg owns the rule so the JVM leg agrees rather than relying
;; on the underlying client's behavior.
(deftest headers-whitespace-stripped-on-delivery
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [received (promise)]
           (nats/subscribe conn headers-trim-subject #(deliver received %))
           (nats/publish conn headers-trim-subject payload {:headers {"X-Trace" "  abc  "}})
           (let [msg (deref received 5000 ::timeout)]
             (is (not= ::timeout msg) "handler is invoked within 5s")
             (is (= {"X-Trace" ["abc"]} (:headers msg))
                 "surrounding whitespace is stripped from the delivered value")))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [received (p/deferred)]
                  (nats/subscribe conn headers-trim-subject #(p/resolve! received %))
                  (nats/publish conn headers-trim-subject payload {:headers {"X-Trace" "  abc  "}})
                  (-> (p/timeout received 5000)
                      (p/then (fn [msg]
                                (is (= {"X-Trace" ["abc"]} (:headers msg))
                                    "surrounding whitespace is stripped from the delivered value"))))))))))

;; A non-string header value is rejected with a portable `:type :invalid-header`
;; ex-info on every platform, rather than leaking the underlying clients'
;; divergence: jnats silently drops a nil value and publishes headerless, while
;; nats.js throws (CONTEXT: Headers). The throw is synchronous in `publish`,
;; before anything reaches the wire.
(deftest headers-non-string-value-rejected
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (is (= :invalid-header
                (try (nats/publish conn "headers.invalid" payload {:headers {"X-Trace" nil}})
                     :no-throw
                     (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
             "a nil header value is rejected as :invalid-header, not silently dropped")))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (is (= :invalid-header
                       (try (nats/publish conn "headers.invalid" payload {:headers {"X-Trace" nil}})
                            :no-throw
                            (catch :default e (:type (ex-data e)))))
                    "a nil header value is rejected as :invalid-header, not silently dropped"))))))

;; A non-positive :max-pending is caller misuse — it would otherwise arm a zero
;; (or sentinel-unbounded) native cap and silently deafen the subscription — so
;; subscribe rejects it synchronously with a portable `:type :invalid-max-pending`
;; on every platform (parallel to :invalid-header), before any native subscribe.
(deftest subscribe-non-positive-max-pending-rejected
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (is (= :invalid-max-pending
                (try (nats/subscribe conn "mp.invalid" (fn [_]) {:max-pending 0})
                     :no-throw
                     (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
             "max-pending 0 is rejected as :invalid-max-pending, not armed as a zero cap")))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (is (= :invalid-max-pending
                       (try (nats/subscribe conn "mp.invalid" (fn [_]) {:max-pending 0})
                            :no-throw
                            (catch :default e (:type (ex-data e)))))
                    "max-pending 0 is rejected as :invalid-max-pending, not armed as a zero cap"))))))

;; ===========================================================================
;; Error model (ADR 0006): every canonical Error :type reproduced and asserted
;; with identical shape on both legs. One-shot ops reject their promise; async
;; failures reach a sink (sub :on-error, else connection :on-status :error).
;; ===========================================================================

;; :connect-failed (ADR 0006): the server-side dial attempt fails — here against
;; a port nothing listens on — so connect rejects its promise with a normalized
;; `:type :connect-failed`, distinct from the client-side :auth-invalid.
(deftest connect-failed-rejects
  #?(:clj
     (let [t (try @(nats/connect {:servers [dead-server-url] :reconnect {:max 0}})
                  nil
                  (catch java.util.concurrent.ExecutionException e
                    (:type (ex-data (.getCause e)))))]
       (is (= :connect-failed t)
           "connecting to a dead server rejects with :connect-failed"))
     :cljs
     (async done
            (-> (nats/connect {:servers [dead-server-url] :reconnect {:max 0}})
                (p/then (fn [conn] (is false "expected the dial to reject with :connect-failed") (close! conn)))
                (p/catch (fn [e]
                           (is (= :connect-failed (:type (ex-data e)))
                               "connecting to a dead server rejects with :connect-failed")))
                (p/finally (fn [_ _] (done)))))))

;; :max-payload-exceeded (ADR 0006): a publish whose bytes exceed the server's
;; max_payload throws synchronously — fire-and-forget has no promise to reject —
;; with a normalized `:type :max-payload-exceeded`. ~1.1 MB vs the default 1 MB.
(deftest max-payload-exceeded-throws
  (let [big (apply str (repeat 1100000 \x))]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (is (= :max-payload-exceeded
                  (try (nats/publish conn payload-subject big)
                       :no-throw
                       (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
               "an oversized publish throws :max-payload-exceeded")))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (is (= :max-payload-exceeded
                         (try (nats/publish conn payload-subject big)
                              :no-throw
                              (catch :default e (:type (ex-data e)))))
                      "an oversized publish throws :max-payload-exceeded")))))))

;; :connection-closed (ADR 0006): operating on a closed connection. publish has
;; no promise so it throws synchronously; request rejects its promise. Both carry
;; a normalized `:type :connection-closed` — a retry-able signal, distinct from
;; :drained (the drain-window refusal).
(deftest connection-closed-normalized
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         @(nats/close conn)
         (is (= :connection-closed
                (try (nats/publish conn closed-pub-subject {:n 1})
                     :no-throw
                     (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
             "publish on a closed connection throws :connection-closed")
         (is (= :connection-closed
                (try (deref (nats/request conn closed-pub-subject {:n 1} {:timeout-ms 500}) 2000 ::timeout)
                     nil
                     (catch java.util.concurrent.ExecutionException e (:type (ex-data (.getCause e))))))
             "request on a closed connection rejects with :connection-closed")))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (-> (nats/close conn)
                    (p/then (fn [_]
                              (is (= :connection-closed
                                     (try (nats/publish conn closed-pub-subject {:n 1})
                                          :no-throw
                                          (catch :default e (:type (ex-data e)))))
                                  "publish on a closed connection throws :connection-closed")
                              (nats/request conn closed-pub-subject {:n 1} {:timeout-ms 500})))
                    (p/then (fn [_] (is false "expected request to reject with :connection-closed")))
                    (p/catch (fn [e]
                               (is (= :connection-closed (:type (ex-data e)))
                                   "request on a closed connection rejects with :connection-closed")))))))))

;; handler-throw → :on-error (ADR 0006/0007, AC#2): a handler that throws is
;; caught and routed to the subscription's :on-error — the raw thrown value,
;; passed through unchanged with no canonical :type — without killing the
;; subscription, so a later message still arrives. :boom throws; :ok is delivered.
(deftest handler-throw-routes-to-on-error
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [[errs on-error] (error-collector)
               got        (atom [])]
           (nats/subscribe conn throw-subject
                           (fn [msg]
                             (if (= :boom (:data msg))
                               (throw (ex-info "boom" {:kaboom true}))
                               (swap! got conj (:data msg))))
                           {:on-error on-error})
           (nats/publish conn throw-subject :boom)
           (nats/publish conn throw-subject :ok)
           (is (wait-for #(and (seq @errs) (= [:ok] @got)) 5000)
               "the throw reaches :on-error and the next message is still delivered")
           (is (= {:kaboom true} (ex-data (first @errs)))
               "the raw thrown value reaches :on-error unchanged"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [[errs on-error] (error-collector)
                      got        (atom [])]
                  (nats/subscribe conn throw-subject
                                  (fn [msg]
                                    (if (= :boom (:data msg))
                                      (throw (ex-info "boom" {:kaboom true}))
                                      (swap! got conj (:data msg))))
                                  {:on-error on-error})
                  (nats/publish conn throw-subject :boom)
                  (nats/publish conn throw-subject :ok)
                  (-> (wait-for #(and (seq @errs) (= [:ok] @got)) 5000)
                      (p/then (fn [hit?]
                                (is hit? "the throw reaches :on-error and the next message is still delivered")
                                (is (= {:kaboom true} (ex-data (first @errs)))
                                    "the raw thrown value reaches :on-error unchanged"))))))))))

;; handler-throw → :on-status :error fallback (ADR 0006, AC#2): with NO per-sub
;; :on-error, the same thrown value reaches the connection's :on-status — as the
;; lone non-bare lifecycle event `{:type :error :error <ex-info>}` — and the
;; subscription still survives.
(deftest handler-throw-falls-back-to-on-status
  (let [[seen on-status] (status-collector)
        opts {:servers [server-url] :on-status on-status}]
    #?(:clj
       (with-conn opts
         (fn [conn]
           (let [got (atom [])]
             (nats/subscribe conn throw-fallback-subject
                             (fn [msg]
                               (if (= :boom (:data msg))
                                 (throw (ex-info "boom" {:kaboom true}))
                                 (swap! got conj (:data msg)))))
             (nats/publish conn throw-fallback-subject :boom)
             (nats/publish conn throw-fallback-subject :ok)
             (is (wait-for #(and (some (comp #{:error} :type) @seen) (= [:ok] @got)) 5000)
                 "with no :on-error the throw reaches :on-status and the sub survives")
             (is (= {:kaboom true} (ex-data (:error (first (filter (comp #{:error} :type) @seen)))))
                 "the :error event wraps the thrown value under :error"))))
       :cljs
       (async done
              (with-conn opts done
                (fn [conn]
                  (let [got (atom [])]
                    (nats/subscribe conn throw-fallback-subject
                                    (fn [msg]
                                      (if (= :boom (:data msg))
                                        (throw (ex-info "boom" {:kaboom true}))
                                        (swap! got conj (:data msg)))))
                    (nats/publish conn throw-fallback-subject :boom)
                    (nats/publish conn throw-fallback-subject :ok)
                    (-> (wait-for #(and (some (comp #{:error} :type) @seen) (= [:ok] @got)) 5000)
                        (p/then (fn [hit?]
                                  (is hit? "with no :on-error the throw reaches :on-status and the sub survives")
                                  (is (= {:kaboom true} (ex-data (:error (first (filter (comp #{:error} :type) @seen)))))
                                      "the :error event wraps the thrown value under :error")))))))))))

;; decode-failure → :codec-error (ADR 0006, AC#3): a subscriber decoding with
;; :edn receives raw non-EDN bytes (a lone "{", published via :string). decode-msg
;; throws synchronously — the handler never sees garbage — and the failure is
;; routed to the sub's :on-error as a normalized :codec-error, without killing the
;; subscription: a following valid message is still delivered.
(deftest decode-failure-routes-to-on-error
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [[errs on-error] (error-collector)
               got        (atom [])]
           (nats/subscribe conn codec-error-subject
                           (fn [msg] (swap! got conj (:data msg)))
                           {:on-error on-error})
           (nats/publish conn codec-error-subject "{" {:codec :string})
           (nats/publish conn codec-error-subject {:ok 1})
           (is (wait-for #(and (seq @errs) (= [{:ok 1}] @got)) 5000)
               "the decode failure reaches :on-error and the next message survives")
           (is (= :codec-error (:type (ex-data (first @errs))))
               "the failure is a normalized :codec-error ex-info"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [[errs on-error] (error-collector)
                      got        (atom [])]
                  (nats/subscribe conn codec-error-subject
                                  (fn [msg] (swap! got conj (:data msg)))
                                  {:on-error on-error})
                  (nats/publish conn codec-error-subject "{" {:codec :string})
                  (nats/publish conn codec-error-subject {:ok 1})
                  (-> (wait-for #(and (seq @errs) (= [{:ok 1}] @got)) 5000)
                      (p/then (fn [hit?]
                                (is hit? "the decode failure reaches :on-error and the next message survives")
                                (is (= :codec-error (:type (ex-data (first @errs))))
                                    "the failure is a normalized :codec-error ex-info"))))))))))

;; :slow-consumer + :max-pending (ADR 0006/0007, AC#4): a handler held on a
;; pending promise lets a flood pile up past :max-pending 1, so the overflow
;; reaches the subscription's :on-error (per-sub — never :on-status) as a
;; :slow-consumer carrying its subject and threshold. The signal is portable; the
;; drop is native (JVM drops, CLJS buffers unbounded) — so only the signal is asserted.
(deftest slow-consumer-routes-to-on-error
  #?(:clj
     (let [conn @(nats/connect {:servers [server-url]})
           [errs on-error] (error-collector)
           gate (java.util.concurrent.CompletableFuture.)]
       (try
         (nats/subscribe conn slow-subject (fn [_] gate)
                         {:on-error on-error :max-pending 1})
         (dotimes [_ 20] (nats/publish conn slow-subject {:n 1}))
         @(nats/flush conn)
         (is (wait-for #(some (comp #{:slow-consumer} :type ex-data) @errs) 5000)
             ":max-pending overflow reaches :on-error as :slow-consumer")
         (let [d (some-> (filter (comp #{:slow-consumer} :type ex-data) @errs) first ex-data)]
           (is (= slow-subject (:subject d)) "the :slow-consumer names its subject")
           (is (= 1 (:max-pending d)) "the :slow-consumer carries its :max-pending threshold"))
         (finally
           (.complete gate nil)
           (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (let [[errs on-error] (error-collector)
                                gate (p/deferred)]
                            (nats/subscribe conn slow-subject (fn [_] gate)
                                            {:on-error on-error :max-pending 1})
                            (dotimes [_ 20] (nats/publish conn slow-subject {:n 1}))
                            (-> (nats/flush conn)
                                (p/then (fn [_] (wait-for #(some (comp #{:slow-consumer} :type ex-data) @errs) 5000)))
                                (p/then (fn [hit?]
                                          (is hit? ":max-pending overflow reaches :on-error as :slow-consumer")
                                          (let [d (some-> (filter (comp #{:slow-consumer} :type ex-data) @errs) first ex-data)]
                                            (is (= slow-subject (:subject d)) "the :slow-consumer names its subject")
                                            (is (= 1 (:max-pending d)) "the :slow-consumer carries its :max-pending threshold"))))
                                (p/finally (fn [_ _] (p/resolve! gate nil) (close! conn)))))))
                (p/catch (fn [e] (is false (str "slow-consumer test failed: " e))))
                (p/finally (fn [_ _] (done)))))))

;; :permissions-violation (ADR 0006, AC#1): a connection-level failure with no
;; per-sub identity — jnats' ErrorListener / nats.js' status stream hand back no
;; subscription — so a forbidden subscribe (the restricted user is denied
;; "forbidden.>") reaches :on-status as an :error event ONLY, never a per-sub
;; :on-error, carrying a :permissions-violation ex-info.
(deftest permissions-violation-reaches-on-status
  (let [[seen on-status] (status-collector)
        opts {:servers   [users-server-url]
              :auth      {:user restricted-user :pass restricted-pass}
              :on-status on-status}]
    #?(:clj
       (with-conn opts
         (fn [conn]
           (nats/subscribe conn forbidden-subject (fn [_] nil))
           (is (wait-for #(some (comp #{:error} :type) @seen) 5000)
               "a forbidden subscribe reaches :on-status as an :error event")
           (is (= :permissions-violation
                  (:type (ex-data (:error (first (filter (comp #{:error} :type) @seen))))))
               "the :error event carries a :permissions-violation ex-info")))
       :cljs
       (async done
              (with-conn opts done
                (fn [conn]
                  (nats/subscribe conn forbidden-subject (fn [_] nil))
                  (-> (wait-for #(some (comp #{:error} :type) @seen) 5000)
                      (p/then (fn [hit?]
                                (is hit? "a forbidden subscribe reaches :on-status as an :error event")
                                (is (= :permissions-violation
                                       (:type (ex-data (:error (first (filter (comp #{:error} :type) @seen))))))
                                    "the :error event carries a :permissions-violation ex-info"))))))))))

;; :protocol-error (ADR 0006, AC#1): the server emits it for malformed protocol
;; exchanges the client never produces, so there is no clean e2e trigger. Both
;; legs feed their native server-error string (jnats' ErrorListener string /
;; nats.js' status error message) to one shared classifier (`error/server-error-type`),
;; so it is asserted there: exact "Permissions Violation" → :permissions-violation,
;; anything else (and an absent string) → :protocol-error.
(deftest server-error-classifier-maps-protocol-error
  (is (= :permissions-violation
         (error/server-error-type "Permissions Violation for Subscription to \"forbidden.x\""))
      "a Permissions Violation classifies as :permissions-violation")
  (is (= :protocol-error (error/server-error-type "Unknown Protocol Operation"))
      "any other server error classifies as :protocol-error")
  (is (= :protocol-error (error/server-error-type nil))
      "an absent error string classifies as :protocol-error"))

;; :drained (ADR 0006, AC#1): an op refused during the drain WINDOW — distinct
;; from :connection-closed, which is retry-able. A handler held on a pending
;; promise keeps the connection draining; a request issued in that window rejects
;; with :drained (after drain completes the same op would be :connection-closed).
(deftest request-during-drain-window-rejects-drained
  #?(:clj
     (let [conn @(nats/connect {:servers [server-url]})
           gate (java.util.concurrent.CompletableFuture.)]
       (try
         (nats/subscribe conn drain-window-subject (fn [_] gate))
         (nats/publish conn drain-window-subject {:n 1})
         @(nats/flush conn)
         (Thread/sleep 200)
         (let [drain-ret (nats/drain conn)]
           (Thread/sleep 100)
           (let [t (try (deref (nats/request conn drain-window-subject {:n 2} {:timeout-ms 500}) 2000 ::timeout)
                        nil
                        (catch java.util.concurrent.ExecutionException e (:type (ex-data (.getCause e)))))]
             (is (= :drained t)
                 "a request in the drain window rejects with :drained, not :connection-closed"))
           (.complete gate nil)
           (deref drain-ret 3000 ::timeout))
         (finally
           (.complete gate nil)
           (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (let [gate (p/deferred)]
                            (nats/subscribe conn drain-window-subject (fn [_] gate))
                            (nats/publish conn drain-window-subject {:n 1})
                            (-> (nats/flush conn)
                                (p/then (fn [_] (p/delay 200)))
                                (p/then (fn [_]
                                          (let [drain-ret (nats/drain conn)]
                                            (-> (p/delay 100)
                                                (p/then (fn [_] (nats/request conn drain-window-subject {:n 2} {:timeout-ms 500})))
                                                (p/then (fn [_] (is false "expected the request to reject with :drained")))
                                                (p/catch (fn [e]
                                                           (is (= :drained (:type (ex-data e)))
                                                               "a request in the drain window rejects with :drained, not :connection-closed")))
                                                (p/then (fn [_] (p/resolve! gate nil)))
                                                (p/then (fn [_] drain-ret))))))
                                (p/finally (fn [_ _] (p/resolve! gate nil) (close! conn)))))))
                (p/catch (fn [e] (is false (str "drain-window test failed: " e))))
                (p/finally (fn [_ _] (done)))))))
