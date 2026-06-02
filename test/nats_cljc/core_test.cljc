(ns nats-cljc.core-test
  (:require #?(:clj  [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is async]])
            [nats-cljc.core :as nats]
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

;; Test-only teardown: close the native client so jnats' non-daemon threads (and
;; the CLJS ws connection) don't outlive the test. A public close/drain is its
;; own slice; here we reach the record's client field directly.
(defn- close! [conn] (.close (:client conn)))

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

(deftest connect-resolves-to-a-connection
  #?(:clj
     (let [conn @(nats/connect {:servers [server-url]})]
       (try
         (is (some? conn) "connect resolves to a non-nil Connection")
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (is (some? conn) "connect resolves to a non-nil Connection")
                          (close! conn)))
                (p/catch (fn [e] (is false (str "connect failed: " e))))
                (p/finally (fn [_ _] (done)))))))

(deftest auth-with-token-connects
  #?(:clj
     (let [conn @(nats/connect {:servers [token-server-url]
                                :auth    {:token token}})]
       (try
         (is (some? conn) ":auth {:token ...} connects against a token-configured server")
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [token-server-url]
                               :auth    {:token token}})
                (p/then (fn [conn]
                          (is (some? conn) ":auth {:token ...} connects against a token-configured server")
                          (close! conn)))
                (p/catch (fn [e] (is false (str "token auth connect failed: " e))))
                (p/finally (fn [_ _] (done)))))))

(deftest auth-with-user-pass-connects
  #?(:clj
     (let [conn @(nats/connect {:servers [users-server-url]
                                :auth    {:user user :pass pass}})]
       (try
         (is (some? conn) ":auth {:user ... :pass ...} connects against a user/password-configured server")
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [users-server-url]
                               :auth    {:user user :pass pass}})
                (p/then (fn [conn]
                          (is (some? conn) ":auth {:user ... :pass ...} connects against a user/password-configured server")
                          (close! conn)))
                (p/catch (fn [e] (is false (str "user/pass auth connect failed: " e))))
                (p/finally (fn [_ _] (done)))))))

(deftest auth-with-nkey-connects
  #?(:clj
     (let [conn @(nats/connect {:servers [users-server-url]
                                :auth    {:nkey nkey :seed seed}})]
       (try
         (is (some? conn) ":auth {:nkey ... :seed ...} connects against an nkey-configured server")
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [users-server-url]
                               :auth    {:nkey nkey :seed seed}})
                (p/then (fn [conn]
                          (is (some? conn) ":auth {:nkey ... :seed ...} connects against an nkey-configured server")
                          (close! conn)))
                (p/catch (fn [e] (is false (str "nkey auth connect failed: " e))))
                (p/finally (fn [_ _] (done)))))))

(deftest auth-with-jwt-connects
  #?(:clj
     (let [conn @(nats/connect {:servers [jwt-server-url]
                                :auth    {:jwt user-jwt :seed jwt-seed}})]
       (try
         (is (some? conn) ":auth {:jwt ... :seed ...} connects against a jwt-configured server")
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [jwt-server-url]
                               :auth    {:jwt user-jwt :seed jwt-seed}})
                (p/then (fn [conn]
                          (is (some? conn) ":auth {:jwt ... :seed ...} connects against a jwt-configured server")
                          (close! conn)))
                (p/catch (fn [e] (is false (str "jwt auth connect failed: " e))))
                (p/finally (fn [_ _] (done)))))))

(deftest auth-with-creds-connects
  #?(:clj
     (let [conn @(nats/connect {:servers [jwt-server-url]
                                :auth    {:creds creds}})]
       (try
         (is (some? conn) ":auth {:creds ...} connects using credentials passed as string content")
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [jwt-server-url]
                               :auth    {:creds creds}})
                (p/then (fn [conn]
                          (is (some? conn) ":auth {:creds ...} connects using credentials passed as string content")
                          (close! conn)))
                (p/catch (fn [e] (is false (str "creds auth connect failed: " e))))
                (p/finally (fn [_ _] (done)))))))

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
     (let [conn     @(nats/connect {:servers [server-url]})
           received (promise)
           sub      (nats/subscribe conn subject #(deliver received %))
           pub-ret  (nats/publish conn subject payload)]
       (try
         (is (some? sub) "subscribe returns a Subscription synchronously")
         (is (nil? pub-ret) "publish returns nil")
         (let [msg (deref received 5000 ::timeout)]
           (is (not= ::timeout msg) "handler is invoked within 5s")
           (is (= subject (:subject msg)) "handler receives the subject")
           (is (= payload (:data msg)) "handler receives EDN-decoded :data"))
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (let [received (p/deferred)
                                sub      (nats/subscribe conn subject #(p/resolve! received %))
                                pub-ret  (nats/publish conn subject payload)]
                            (is (some? sub) "subscribe returns a Subscription synchronously")
                            (is (nil? pub-ret) "publish returns nil")
                            (-> (p/timeout received 5000)
                                (p/then (fn [msg]
                                          (is (= subject (:subject msg)) "handler receives the subject")
                                          (is (= payload (:data msg)) "handler receives EDN-decoded :data")))
                                (p/finally (fn [_ _] (close! conn)))))))
                (p/catch (fn [e] (is false (str "round-trip failed: " e))))
                (p/finally (fn [_ _] (done)))))))

(deftest status-connected-delivered
  #?(:clj
     (let [[seen on-status] (status-collector)
           conn @(nats/connect {:servers   [server-url]
                                :on-status on-status})]
       (try
         (is (wait-for #(some #{{:type :connected}} @seen) 2000)
             ":connected reaches :on-status as a {:type ...} map")
         (finally (close! conn))))
     :cljs
     (async done
            (let [[seen on-status] (status-collector)]
              (-> (nats/connect {:servers   [server-url]
                                 :on-status on-status})
                  (p/then (fn [conn]
                            (is (some #{{:type :connected}} @seen)
                                ":connected reaches :on-status as a {:type ...} map")
                            (close! conn)))
                  (p/catch (fn [e] (is false (str "connect failed: " e))))
                  (p/finally (fn [_ _] (done))))))))

(deftest close-settles-fires-closed-and-ends-subs
  #?(:clj
     (let [[seen on-status] (status-collector)
           conn      @(nats/connect {:servers [server-url] :on-status on-status})
           sub       (nats/subscribe conn subject (fn [_] nil))
           close-ret (nats/close conn)]
       (is (not= ::timeout (deref close-ret 2000 ::timeout))
           "close returns a promise that settles")
       (is (wait-for #(some #{{:type :closed}} @seen) 2000)
           ":closed reaches :on-status")
       (is (wait-for #(sub-ended? sub) 2000)
           "close ends the connection's subscriptions"))
     :cljs
     (async done
            (let [[seen on-status] (status-collector)]
              (-> (nats/connect {:servers [server-url] :on-status on-status})
                  (p/then (fn [conn]
                            (let [sub       (nats/subscribe conn subject (fn [_] nil))
                                  close-ret (nats/close conn)]
                              (is (some? close-ret) "close returns a promise")
                              (-> close-ret
                                  (p/then (fn [_] (p/delay 100)))
                                  (p/then (fn [_]
                                            (is (some #{{:type :closed}} @seen)
                                                ":closed reaches :on-status")
                                            (is (sub-ended? sub)
                                                "close ends the connection's subscriptions")))))))
                  (p/catch (fn [e] (is false (str "close test failed: " e))))
                  (p/finally (fn [_ _] (done))))))))

(deftest disconnected-fires-on-drop
  #?(:clj
     (let [[seen on-status] (status-collector)
           conn @(nats/connect {:servers [server-url] :on-status on-status})]
       (try
         (force-drop! conn)
         (is (wait-for #(some #{{:type :disconnected}} @seen) 5000)
             ":disconnected reaches :on-status on a real link drop")
         (finally (close! conn))))
     :cljs
     (async done
            (let [[seen on-status] (status-collector)]
              (-> (nats/connect {:servers [server-url] :on-status on-status})
                  (p/then (fn [conn]
                            (force-drop! conn)
                            (-> (wait-for #(some #{{:type :disconnected}} @seen) 5000)
                                (p/then (fn [hit?]
                                          (is hit? ":disconnected reaches :on-status on a real link drop")
                                          (close! conn))))))
                  (p/catch (fn [e] (is false (str "disconnect test failed: " e))))
                  (p/finally (fn [_ _] (done))))))))

;; One real drop drives the whole disconnect->reconnecting->reconnected cycle, so
;; both reconnect events are asserted from a single connection. We wait for
;; :reconnected (the end of the cycle), then assert :reconnecting preceded it —
;; intra-platform ordering only, per the shape-not-cadence contract (ADR 0006):
;; the JVM synthesizes one :reconnecting per loss, nats.js emits one per dial
;; attempt, so the count is not asserted, only that the shape arrives in order.
(deftest reconnect-cycle-fires-reconnecting-then-reconnected
  #?(:clj
     (let [[seen on-status] (status-collector)
           conn @(nats/connect {:servers   [server-url]
                                :reconnect {:max 5 :wait-ms 50 :jitter-ms 10}
                                :on-status on-status})]
       (try
         (force-drop! conn)
         (is (wait-for #(some #{{:type :reconnected}} @seen) 5000)
             ":reconnected reaches :on-status after a real drop when :reconnect is configured")
         (is (precedes? @seen :reconnecting :reconnected)
             ":reconnecting reaches :on-status and precedes :reconnected in the cycle")
         (finally (close! conn))))
     :cljs
     (async done
            (let [[seen on-status] (status-collector)]
              (-> (nats/connect {:servers   [server-url]
                                 :reconnect {:max 5 :wait-ms 50 :jitter-ms 10}
                                 :on-status on-status})
                  (p/then (fn [conn]
                            (force-drop! conn)
                            (-> (wait-for #(some #{{:type :reconnected}} @seen) 5000)
                                (p/then (fn [hit?]
                                          (is hit? ":reconnected reaches :on-status after a real drop when :reconnect is configured")
                                          (is (precedes? @seen :reconnecting :reconnected)
                                              ":reconnecting reaches :on-status and precedes :reconnected in the cycle")
                                          (close! conn))))))
                  (p/catch (fn [e] (is false (str "reconnect test failed: " e))))
                  (p/finally (fn [_ _] (done))))))))

;; :reconnect {:max -1} is the unlimited sentinel — honored natively on both
;; platforms (jnats .maxReconnects(-1); nats.js maxReconnectAttempts -1). Can't
;; assert "never gives up", so this just locks that -1 passes through and connects.
(deftest reconnect-unlimited-connects
  #?(:clj
     (let [conn @(nats/connect {:servers [server-url] :reconnect {:max -1}})]
       (is (some? conn) ":reconnect {:max -1} (unlimited) connects without error")
       (close! conn))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url] :reconnect {:max -1}})
                (p/then (fn [conn]
                          (is (some? conn) ":reconnect {:max -1} (unlimited) connects without error")
                          (close! conn)))
                (p/catch (fn [e] (is false (str "unlimited reconnect connect failed: " e))))
                (p/finally (fn [_ _] (done)))))))

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
     (let [conn @(nats/connect {:servers [server-url]})]
       (try
         (nats/publish conn subject payload)
         (let [flush-ret (nats/flush conn)]
           (is (not= ::timeout (deref flush-ret 5000 ::timeout))
               "flush returns a promise that settles"))
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (nats/publish conn subject payload)
                          (let [flush-ret (nats/flush conn)]
                            (is (some? flush-ret) "flush returns a promise")
                            ;; reaching this then means the promise settled
                            (-> flush-ret
                                (p/then (fn [_] (is true "flush settles")))
                                (p/finally (fn [_ _] (close! conn)))))))
                (p/catch (fn [e] (is false (str "flush test failed: " e))))
                (p/finally (fn [_ _] (done)))))))

(deftest drain-connection-settles-and-ends-subs
  #?(:clj
     (let [conn      @(nats/connect {:servers [server-url]})
           sub       (nats/subscribe conn subject (fn [_] nil))
           drain-ret (nats/drain conn)]
       (try
         (is (not= ::timeout (deref drain-ret 5000 ::timeout))
             "drain returns a promise that settles")
         (is (wait-for #(sub-ended? sub) 2000)
             "drain ends the connection's subscriptions")
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (let [sub       (nats/subscribe conn subject (fn [_] nil))
                                drain-ret (nats/drain conn)]
                            (is (some? drain-ret) "drain returns a promise")
                            (-> drain-ret
                                (p/then (fn [_] (wait-for #(sub-ended? sub) 2000)))
                                (p/then (fn [ended?]
                                          (is ended? "drain ends the connection's subscriptions")))
                                (p/finally (fn [_ _] (close! conn)))))))
                (p/catch (fn [e] (is false (str "drain test failed: " e))))
                (p/finally (fn [_ _] (done)))))))

(deftest drain-subscription-settles-and-ends-only-it
  #?(:clj
     (let [conn      @(nats/connect {:servers [server-url]})
           sub-a     (nats/subscribe conn (str subject ".a") (fn [_] nil))
           sub-b     (nats/subscribe conn (str subject ".b") (fn [_] nil))
           drain-ret (nats/drain sub-a)]
       (try
         (is (not= ::timeout (deref drain-ret 5000 ::timeout))
             "subscription drain returns a promise that settles")
         (is (wait-for #(sub-ended? sub-a) 2000)
             "draining a subscription ends it")
         (is (not (sub-ended? sub-b))
             "draining one subscription leaves the connection's others active")
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (let [sub-a     (nats/subscribe conn (str subject ".a") (fn [_] nil))
                                sub-b     (nats/subscribe conn (str subject ".b") (fn [_] nil))
                                drain-ret (nats/drain sub-a)]
                            (is (some? drain-ret) "subscription drain returns a promise")
                            (-> drain-ret
                                (p/then (fn [_] (wait-for #(sub-ended? sub-a) 2000)))
                                (p/then (fn [ended?]
                                          (is ended? "draining a subscription ends it")
                                          (is (not (sub-ended? sub-b))
                                              "draining one subscription leaves the connection's others active")))
                                (p/finally (fn [_ _] (close! conn)))))))
                (p/catch (fn [e] (is false (str "subscription drain test failed: " e))))
                (p/finally (fn [_ _] (done)))))))

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
       (let [conn  @(nats/connect {:servers [server-url]})
             order (atom [])]
         (try
           (nats/subscribe conn order-subject (fn [msg] (swap! order conj (:data msg))))
           (dotimes [i n] (nats/publish conn order-subject i))
           (is (wait-for #(= n (count @order)) 5000) "all messages delivered")
           (is (= (vec (range n)) @order)
               "a single subscription delivers one publisher's messages in publish order")
           (finally (close! conn))))
       :cljs
       (async done
              (-> (nats/connect {:servers [server-url]})
                  (p/then (fn [conn]
                            (let [order (atom [])]
                              (nats/subscribe conn order-subject (fn [msg] (swap! order conj (:data msg))))
                              (dotimes [i n] (nats/publish conn order-subject i))
                              (-> (wait-for #(= n (count @order)) 5000)
                                  (p/then (fn [hit?]
                                            (is hit? "all messages delivered")
                                            (is (= (vec (range n)) @order)
                                                "a single subscription delivers one publisher's messages in publish order")))
                                  (p/finally (fn [_ _] (close! conn)))))))
                  (p/catch (fn [e] (is false (str "ordering test failed: " e))))
                  (p/finally (fn [_ _] (done))))))))

;; Promise-return backpressure (ADR 0007): a handler that returns a pending
;; promise suspends delivery of the next message until it settles; a non-promise
;; return delivers immediately. The handler returns a gate promise only for the
;; first message, so the second must wait until the gate settles. The gate is a
;; platform-native promise the test controls — a CompletableFuture on the JVM, a
;; promesa deferred on CLJS — exactly the shape a real async handler would return.
(deftest pending-promise-handler-applies-backpressure
  #?(:clj
     (let [conn  @(nats/connect {:servers [server-url]})
           order (atom [])
           gate  (java.util.concurrent.CompletableFuture.)]
       (try
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
             "the next message is delivered once the promise settles")
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
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
                                          (is hit? "the next message is delivered once the promise settles")))
                                (p/finally (fn [_ _] (close! conn)))))))
                (p/catch (fn [e] (is false (str "backpressure test failed: " e))))
                (p/finally (fn [_ _] (done)))))))

;; No cross-subscription coupling (ADR 0007): backpressure is per-subscription,
;; and a handler that returns a pending promise must never block the underlying
;; client thread or event loop. Sub-A is held on a pending-promise handler while
;; sub-B keeps delivering — so a backpressured subscription neither stalls another
;; nor freezes the loop. The suite asserts nothing about A-vs-B ordering: there is
;; no cross-subscription ordering guarantee to assume.
(deftest subscriptions-are-independent
  #?(:clj
     (let [conn    @(nats/connect {:servers [server-url]})
           a-order (atom [])
           b-order (atom [])
           gate    (java.util.concurrent.CompletableFuture.)]
       (try
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
             "the backpressured subscription resumes once its promise settles")
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
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
                                          (is hit? "the backpressured subscription resumes once its promise settles")))
                                (p/finally (fn [_ _] (close! conn)))))))
                (p/catch (fn [e] (is false (str "independence test failed: " e))))
                (p/finally (fn [_ _] (done)))))))

;; Queue groups / competing consumers (ADR 0007). Subscriptions sharing a :queue
;; group on one subject compete: the server load-balances each matching message to
;; exactly one member, so the group's combined delivery is the full stream with no
;; duplication (a disjoint share). All subs and the pubs share one connection, so
;; the server registers the subscriptions before any message arrives — no flush.
(deftest queue-group-load-balances
  (let [n 50]
    #?(:clj
       (let [conn @(nats/connect {:servers [server-url]})
             a    (atom [])
             b    (atom [])]
         (try
           (nats/subscribe conn queue-subject (fn [msg] (swap! a conj (:data msg))) {:queue "workers"})
           (nats/subscribe conn queue-subject (fn [msg] (swap! b conj (:data msg))) {:queue "workers"})
           (dotimes [i n] (nats/publish conn queue-subject i))
           (is (wait-for #(= n (+ (count @a) (count @b))) 5000)
               "every published message reaches the queue group")
           (is (= (vec (range n)) (vec (sort (concat @a @b))))
               "each message reaches exactly one member — combined delivery is the full set, no duplication")
           (is (and (seq @a) (seq @b))
               "both members receive a share (the server load-balances)")
           (finally (close! conn))))
       :cljs
       (async done
              (-> (nats/connect {:servers [server-url]})
                  (p/then (fn [conn]
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
                                                "both members receive a share (the server load-balances)")))
                                  (p/finally (fn [_ _] (close! conn)))))))
                  (p/catch (fn [e] (is false (str "queue group test failed: " e))))
                  (p/finally (fn [_ _] (done))))))))

;; A queue group only competes within itself: a plain (non-queue) subscription on
;; the same subject is a separate interest, so it still receives every message
;; while the group splits the same stream (ADR 0007). One connection registers all
;; three subscriptions before any publish — no flush needed.
(deftest non-queue-subscription-receives-all-alongside-a-queue-group
  (let [n 50]
    #?(:clj
       (let [conn @(nats/connect {:servers [server-url]})
             a    (atom [])
             b    (atom [])
             all  (atom [])]
         (try
           (nats/subscribe conn queue-mixed-subject (fn [msg] (swap! a conj (:data msg))) {:queue "workers"})
           (nats/subscribe conn queue-mixed-subject (fn [msg] (swap! b conj (:data msg))) {:queue "workers"})
           (nats/subscribe conn queue-mixed-subject (fn [msg] (swap! all conj (:data msg))))
           (dotimes [i n] (nats/publish conn queue-mixed-subject i))
           (is (wait-for #(and (= n (count @all)) (= n (+ (count @a) (count @b)))) 5000)
               "the plain subscription and the queue group both finish")
           (is (= (vec (range n)) (vec (sort @all)))
               "a non-queue subscription on the same subject receives every message")
           (is (= (vec (range n)) (vec (sort (concat @a @b))))
               "the queue group still splits the same stream into a disjoint share")
           (finally (close! conn))))
       :cljs
       (async done
              (-> (nats/connect {:servers [server-url]})
                  (p/then (fn [conn]
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
                                                "the queue group still splits the same stream into a disjoint share")))
                                  (p/finally (fn [_ _] (close! conn)))))))
                  (p/catch (fn [e] (is false (str "mixed queue/plain test failed: " e))))
                  (p/finally (fn [_ _] (done))))))))

;; Request/reply over the core round-trip (ADR 0002/0006). A responder subscribes
;; to `request-subject` and answers via `reply` (sugar that publishes to the
;; request's `:reply` inbox); the requester's `request` resolves to the decoded
;; reply message. `reply` returns nil; every delivered/resolved message carries a
;; `:reply` key (nil when there is no reply subject).
(deftest request-reply-round-trip
  #?(:clj
     (let [conn    @(nats/connect {:servers [server-url]})
           replied (promise)]
       (try
         (nats/subscribe conn request-subject
                         (fn [msg]
                           (deliver replied (nats/reply conn msg {:pong (:n (:data msg))}))))
         (let [resp (deref (nats/request conn request-subject {:n 7} {:timeout-ms 5000}) 5000 ::timeout)]
           (is (not= ::timeout resp) "request resolves within 5s")
           (is (= {:pong 7} (:data resp)) "request resolves to the decoded reply payload")
           (is (contains? resp :reply) "resolved message always carries a :reply key")
           (is (nil? (deref replied 5000 ::unset)) "reply returns nil"))
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (let [replied (atom ::unset)]
                            (nats/subscribe conn request-subject
                                            (fn [msg]
                                              (reset! replied (nats/reply conn msg {:pong (:n (:data msg))}))))
                            (-> (p/timeout (nats/request conn request-subject {:n 7} {:timeout-ms 5000}) 5000)
                                (p/then (fn [resp]
                                          (is (= {:pong 7} (:data resp)) "request resolves to the decoded reply payload")
                                          (is (contains? resp :reply) "resolved message always carries a :reply key")
                                          (is (nil? @replied) "reply returns nil")))
                                (p/finally (fn [_ _] (close! conn)))))))
                (p/catch (fn [e] (is false (str "request/reply round-trip failed: " e))))
                (p/finally (fn [_ _] (done)))))))

;; No-responders failure mode (ADR 0006): a request to a subject nobody
;; subscribes rejects fast with a normalized `:type :no-responders`, distinct
;; from a timeout — the server reports it as soon as it sees no subscribers.
(deftest request-no-responders-rejects
  #?(:clj
     (let [conn @(nats/connect {:servers [server-url]})
           t    (try (deref (nats/request conn no-responders-subject {:n 1} {:timeout-ms 2000}) 5000 ::timeout)
                     nil
                     (catch java.util.concurrent.ExecutionException e
                       (:type (ex-data (.getCause e)))))]
       (try
         (is (= :no-responders t)
             "a request to a subject with no subscribers rejects with :no-responders")
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (-> (nats/request conn no-responders-subject {:n 1} {:timeout-ms 2000})
                              (p/then (fn [_] (is false "expected the request to reject with :no-responders")))
                              (p/catch (fn [e]
                                         (is (= :no-responders (:type (ex-data e)))
                                             "a request to a subject with no subscribers rejects with :no-responders")))
                              (p/finally (fn [_ _] (close! conn))))))
                (p/catch (fn [e] (is false (str "connect failed: " e))))
                (p/finally (fn [_ _] (done)))))))

;; Timeout failure mode (ADR 0006): responders exist (a subscriber is registered,
;; confirmed via flush) but none answer within :timeout-ms, so the request
;; rejects with a normalized `:type :timeout` — distinct from :no-responders.
(deftest request-timeout-rejects
  #?(:clj
     (let [conn @(nats/connect {:servers [server-url]})]
       (try
         (nats/subscribe conn silent-subject (fn [_msg] nil))
         @(nats/flush conn)
         (let [t (try (deref (nats/request conn silent-subject {:n 1} {:timeout-ms 300}) 5000 ::timeout)
                      nil
                      (catch java.util.concurrent.ExecutionException e
                        (:type (ex-data (.getCause e)))))]
           (is (= :timeout t)
               "a request whose responders never answer within :timeout-ms rejects with :timeout"))
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (nats/subscribe conn silent-subject (fn [_msg] nil))
                          (-> (nats/flush conn)
                              (p/then (fn [_] (nats/request conn silent-subject {:n 1} {:timeout-ms 300})))
                              (p/then (fn [_] (is false "expected the request to reject with :timeout")))
                              (p/catch (fn [e]
                                         (is (= :timeout (:type (ex-data e)))
                                             "a request whose responders never answer within :timeout-ms rejects with :timeout")))
                              (p/finally (fn [_ _] (close! conn))))))
                (p/catch (fn [e] (is false (str "connect failed: " e))))
                (p/finally (fn [_ _] (done)))))))

;; Headers round-trip (CONTEXT: Headers). A scalar header value is accepted on
;; publish and arrives normalized to a one-element vector of strings under
;; :headers — the same shape on every platform.
(deftest headers-scalar-delivers-as-one-element-vector
  #?(:clj
     (let [conn     @(nats/connect {:servers [server-url]})
           received (promise)]
       (try
         (nats/subscribe conn headers-scalar-subject #(deliver received %))
         (nats/publish conn headers-scalar-subject payload {:headers {"X-Trace" "abc"}})
         (let [msg (deref received 5000 ::timeout)]
           (is (not= ::timeout msg) "handler is invoked within 5s")
           (is (= {"X-Trace" ["abc"]} (:headers msg))
               "a scalar header value is delivered as a one-element vector of strings"))
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (let [received (p/deferred)]
                            (nats/subscribe conn headers-scalar-subject #(p/resolve! received %))
                            (nats/publish conn headers-scalar-subject payload {:headers {"X-Trace" "abc"}})
                            (-> (p/timeout received 5000)
                                (p/then (fn [msg]
                                          (is (= {"X-Trace" ["abc"]} (:headers msg))
                                              "a scalar header value is delivered as a one-element vector of strings")))
                                (p/finally (fn [_ _] (close! conn)))))))
                (p/catch (fn [e] (is false (str "headers round-trip failed: " e))))
                (p/finally (fn [_ _] (done)))))))

;; When nothing was published under :headers, the delivered message carries no
;; :headers key at all — absence, not an empty map (CONTEXT: Headers).
(deftest headers-absent-when-none-set
  #?(:clj
     (let [conn     @(nats/connect {:servers [server-url]})
           received (promise)]
       (try
         (nats/subscribe conn headers-absent-subject #(deliver received %))
         (nats/publish conn headers-absent-subject payload)
         (let [msg (deref received 5000 ::timeout)]
           (is (not= ::timeout msg) "handler is invoked within 5s")
           (is (not (contains? msg :headers))
               ":headers is absent from the delivered map when none were set"))
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (let [received (p/deferred)]
                            (nats/subscribe conn headers-absent-subject #(p/resolve! received %))
                            (nats/publish conn headers-absent-subject payload)
                            (-> (p/timeout received 5000)
                                (p/then (fn [msg]
                                          (is (not (contains? msg :headers))
                                              ":headers is absent from the delivered map when none were set")))
                                (p/finally (fn [_ _] (close! conn)))))))
                (p/catch (fn [e] (is false (str "headers round-trip failed: " e))))
                (p/finally (fn [_ _] (done)))))))

;; Header names are case-sensitive: two names differing only in case are distinct
;; entries that survive the round-trip without collapsing (CONTEXT: Headers).
(deftest headers-names-are-case-sensitive
  #?(:clj
     (let [conn     @(nats/connect {:servers [server-url]})
           received (promise)]
       (try
         (nats/subscribe conn headers-case-subject #(deliver received %))
         (nats/publish conn headers-case-subject payload {:headers {"X-Trace" "upper" "x-trace" "lower"}})
         (let [msg (deref received 5000 ::timeout)]
           (is (not= ::timeout msg) "handler is invoked within 5s")
           (is (= {"X-Trace" ["upper"] "x-trace" ["lower"]} (:headers msg))
               "names differing only in case are preserved as distinct entries"))
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (let [received (p/deferred)]
                            (nats/subscribe conn headers-case-subject #(p/resolve! received %))
                            (nats/publish conn headers-case-subject payload {:headers {"X-Trace" "upper" "x-trace" "lower"}})
                            (-> (p/timeout received 5000)
                                (p/then (fn [msg]
                                          (is (= {"X-Trace" ["upper"] "x-trace" ["lower"]} (:headers msg))
                                              "names differing only in case are preserved as distinct entries")))
                                (p/finally (fn [_ _] (close! conn)))))))
                (p/catch (fn [e] (is false (str "headers round-trip failed: " e))))
                (p/finally (fn [_ _] (done)))))))

;; A vector-valued header carries multiple values for one name; they arrive
;; unchanged as a vector of strings, in order (CONTEXT: Headers).
(deftest headers-vector-delivers-unchanged
  #?(:clj
     (let [conn     @(nats/connect {:servers [server-url]})
           received (promise)]
       (try
         (nats/subscribe conn headers-vector-subject #(deliver received %))
         (nats/publish conn headers-vector-subject payload {:headers {"X-Tag" ["a" "b"]}})
         (let [msg (deref received 5000 ::timeout)]
           (is (not= ::timeout msg) "handler is invoked within 5s")
           (is (= {"X-Tag" ["a" "b"]} (:headers msg))
               "a vector-valued header is delivered unchanged as a vector of strings"))
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (let [received (p/deferred)]
                            (nats/subscribe conn headers-vector-subject #(p/resolve! received %))
                            (nats/publish conn headers-vector-subject payload {:headers {"X-Tag" ["a" "b"]}})
                            (-> (p/timeout received 5000)
                                (p/then (fn [msg]
                                          (is (= {"X-Tag" ["a" "b"]} (:headers msg))
                                              "a vector-valued header is delivered unchanged as a vector of strings")))
                                (p/finally (fn [_ _] (close! conn)))))))
                (p/catch (fn [e] (is false (str "headers round-trip failed: " e))))
                (p/finally (fn [_ _] (done)))))))

;; Surrounding whitespace on a header value is insignificant and stripped on
;; delivery, identically on every platform (CONTEXT: Headers). nats.js trims
;; natively; decode-msg owns the rule so the JVM leg agrees rather than relying
;; on the underlying client's behavior.
(deftest headers-whitespace-stripped-on-delivery
  #?(:clj
     (let [conn     @(nats/connect {:servers [server-url]})
           received (promise)]
       (try
         (nats/subscribe conn headers-trim-subject #(deliver received %))
         (nats/publish conn headers-trim-subject payload {:headers {"X-Trace" "  abc  "}})
         (let [msg (deref received 5000 ::timeout)]
           (is (not= ::timeout msg) "handler is invoked within 5s")
           (is (= {"X-Trace" ["abc"]} (:headers msg))
               "surrounding whitespace is stripped from the delivered value"))
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (let [received (p/deferred)]
                            (nats/subscribe conn headers-trim-subject #(p/resolve! received %))
                            (nats/publish conn headers-trim-subject payload {:headers {"X-Trace" "  abc  "}})
                            (-> (p/timeout received 5000)
                                (p/then (fn [msg]
                                          (is (= {"X-Trace" ["abc"]} (:headers msg))
                                              "surrounding whitespace is stripped from the delivered value")))
                                (p/finally (fn [_ _] (close! conn)))))))
                (p/catch (fn [e] (is false (str "headers round-trip failed: " e))))
                (p/finally (fn [_ _] (done)))))))

;; A non-string header value is rejected with a portable `:type :invalid-header`
;; ex-info on every platform, rather than leaking the underlying clients'
;; divergence: jnats silently drops a nil value and publishes headerless, while
;; nats.js throws (CONTEXT: Headers). The throw is synchronous in `publish`,
;; before anything reaches the wire.
(deftest headers-non-string-value-rejected
  #?(:clj
     (let [conn @(nats/connect {:servers [server-url]})]
       (try
         (is (= :invalid-header
                (try (nats/publish conn "headers.invalid" payload {:headers {"X-Trace" nil}})
                     :no-throw
                     (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
             "a nil header value is rejected as :invalid-header, not silently dropped")
         (finally (close! conn))))
     :cljs
     (async done
            (-> (nats/connect {:servers [server-url]})
                (p/then (fn [conn]
                          (is (= :invalid-header
                                 (try (nats/publish conn "headers.invalid" payload {:headers {"X-Trace" nil}})
                                      :no-throw
                                      (catch :default e (:type (ex-data e)))))
                              "a nil header value is rejected as :invalid-header, not silently dropped")
                          (close! conn)))
                (p/catch (fn [e] (is false (str "connect failed: " e))))
                (p/finally (fn [_ _] (done)))))))
