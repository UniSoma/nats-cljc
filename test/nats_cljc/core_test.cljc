(ns nats-cljc.core-test
  (:require #?(:clj  [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is async]])
            [nats-cljc.core :as nats]
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
