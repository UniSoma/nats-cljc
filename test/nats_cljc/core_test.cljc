(ns nats-cljc.core-test
  (:require #?(:clj  [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is async]])
            [nats-cljc.core :as nats]
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

;; Test-only check that a subscription has been ended (by drain/close). Reaches
;; the native handle subscribe returns — a jnats Subscription / a nats.js Sub.
(defn- sub-ended? [sub]
  #?(:clj  (not (.isActive sub))
     :cljs (.isClosed ^js sub)))

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

(deftest reconnect-config-drives-reconnection
  #?(:clj
     (let [[seen on-status] (status-collector)
           conn @(nats/connect {:servers   [server-url]
                                :reconnect {:max 5 :wait-ms 50 :jitter-ms 10}
                                :on-status on-status})]
       (try
         (force-drop! conn)
         (is (wait-for #(some #{{:type :reconnected}} @seen) 5000)
             ":reconnected reaches :on-status after a real drop when :reconnect is configured")
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
                                          (close! conn))))))
                  (p/catch (fn [e] (is false (str "reconnect test failed: " e))))
                  (p/finally (fn [_ _] (done))))))))

(deftest reconnecting-fires-on-drop
  #?(:clj
     (let [[seen on-status] (status-collector)
           conn @(nats/connect {:servers   [server-url]
                                :reconnect {:max 5 :wait-ms 50 :jitter-ms 10}
                                :on-status on-status})]
       (try
         (force-drop! conn)
         (is (wait-for #(some #{{:type :reconnecting}} @seen) 5000)
             ":reconnecting reaches :on-status while the client re-establishes the link")
         (finally (close! conn))))
     :cljs
     (async done
            (let [[seen on-status] (status-collector)]
              (-> (nats/connect {:servers   [server-url]
                                 :reconnect {:max 5 :wait-ms 50 :jitter-ms 10}
                                 :on-status on-status})
                  (p/then (fn [conn]
                            (force-drop! conn)
                            (-> (wait-for #(some #{{:type :reconnecting}} @seen) 5000)
                                (p/then (fn [hit?]
                                          (is hit? ":reconnecting reaches :on-status while the client re-establishes the link")
                                          (close! conn))))))
                  (p/catch (fn [e] (is false (str "reconnecting test failed: " e))))
                  (p/finally (fn [_ _] (done))))))))

;; The server-driven types have no portable client-side trigger (a lame-duck
;; needs a server signal; a server-list change needs a cluster), so they are
;; asserted at the real normalization seam: feed the native event jnats/nats.js
;; would emit through impl/deliver-status! and check the canonical {:type ...}
;; reaches :on-status. The native event forks per platform; the result does not.
(deftest lame-duck-normalized
  (let [[seen on-status] (status-collector)]
    (impl/deliver-status! on-status
                          #?(:clj  io.nats.client.ConnectionListener$Events/LAME_DUCK
                             :cljs #js {:type "ldm"}))
    (is (some #{{:type :lame-duck}} @seen)
        "a server-driven lame-duck event normalizes to {:type :lame-duck} on :on-status")))

(deftest servers-changed-normalized
  (let [[seen on-status] (status-collector)]
    (impl/deliver-status! on-status
                          #?(:clj  io.nats.client.ConnectionListener$Events/DISCOVERED_SERVERS
                             :cljs #js {:type "update"}))
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
