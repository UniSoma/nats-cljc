(ns nats-cljc.service-test
  "Portable Services suite (ADR 0024): one `.cljc` source run on the JVM and Node
   (browser is CI-only, ADR 0010). Mirrors `kv-test`'s connect/teardown envelope.
   Services is pure core request-reply, so the anonymous server (ci/nats.conf)
   suffices — there is no JetStream block to need and no entry verification to
   exercise (ADR 0024). The facade is the only seam: every assertion drives
   `service/create`/`respond`/`stop` and the caller's plain `core/request`, never
   an impl ns or a native object, against the real server."
  (:require #?(:clj  [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is async]])
            [nats-cljc.core :as nats]
            [nats-cljc.service :as service]
            #?(:cljs [promesa.core :as p])))

;; The anonymous server: TCP on the JVM, ws on CLJS (ADR 0001). No JetStream needed
;; (services is pure core request-reply).
(def ^:private server-url
  #?(:clj  "nats://127.0.0.1:4222"
     :cljs "ws://127.0.0.1:8080"))

;; Test-only teardown, as in kv-test: close the native client directly so its
;; threads / ws socket don't outlive the test.
(defn- close! [conn] (.close (:client conn)))

;; Capture the value a native promise REJECTS with at the non-blocking async-reject
;; seam (kv-test's helper): `.whenComplete` hands back the BARE ex-info ADR 0006's
;; portable `(:type (ex-data e))` contract targets, not deref's ExecutionException
;; wrapper.
#?(:clj
   (defn- reject-reason [^java.util.concurrent.CompletableFuture cf]
     (let [a (promise)]
       (.whenComplete cf (reify java.util.function.BiConsumer
                           (accept [_ _ e] (deliver a e))))
       (deref a 5000 ::timeout))))

;; The connect / settle / teardown envelope (kv-test's `with-conn`): JVM blocks on
;; connect, runs `(f conn)`, closes in a finally; CLJS awaits the promise `(f conn)`
;; returns, closes, then calls cljs.test's `done`.
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

;; A Service teardown guard (kv-test's `with-bucket`, lifted to a Service): run `f`,
;; then ALWAYS stop the Service so its subscriptions can't outlive the test, even
;; when the body rejects. Best-effort: a stop failure is swallowed so it can't mask
;; the body's outcome. The Service is passed in already created.
#?(:clj
   (defn- with-service [svc f]
     (try (f)
          (finally (try (deref (service/stop svc) 5000 ::timeout)
                        (catch Throwable _ nil)))))
   :cljs
   (defn- with-service [svc f]
     (p/handle (p/do (f))
               (fn [v e]
                 (p/handle (service/stop svc)
                           (fn [_ _] (if e (throw e) v)))))))

;; create resolves to a running Service on both legs — no context, no entry
;; verification (ADR 0024): the promise resolves as soon as the endpoints are
;; subscribed, against a plain server with no JetStream block.
(deftest create-resolves-to-a-running-service
  (let [config {:name "tracer_svc" :version "0.1.0" :description "tracer service"
                :endpoints [{:name "noop" :subject "tracer.svc.noop"
                             :handler (fn [_] nil)}]}]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [svc (deref (service/create conn config) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (is (not= ::timeout svc) "create resolves within 5s")
                 (is (some? svc) "create resolves to a non-nil Service"))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (service/create conn config)
                      (p/then (fn [svc]
                                (with-service svc
                                  (fn []
                                    (is (some? svc) "create resolves to a non-nil Service")))))
                      (p/catch (fn [e]
                                 (is false (str "create rejected unexpectedly: " e)))))))))))

;; An endpoint handler is an ordinary ADR-0007 Handler and (respond conn msg data)
;; answers the request; the caller's plain core/request resolves with the decoded
;; reply — request decode and response encode both go through the connection's
;; default codec (the EDN round-trip of a map proves it; a string would survive a
;; raw passthrough, a map would not).
(deftest endpoint-handler-responds-through-the-default-codec
  (let [config {:name "echo_svc" :version "0.1.0"
                :endpoints [{:name "echo" :subject "tracer.svc.echo"
                             :handler (fn [_] :placeholder)}]}]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           ;; The handler closes over `conn` to respond; rebuild the config here so
           ;; it can (the facade's respond threads conn, as core/reply does).
           (let [cfg (assoc-in config [:endpoints 0 :handler]
                               (fn [msg] (service/respond conn msg {:echo (:data msg) :ok true})))
                 svc (deref (service/create conn cfg) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (let [reply (deref (nats/request conn "tracer.svc.echo" {:n 7} {:timeout-ms 5000}) 5000 ::timeout)]
                   (is (not= ::timeout reply) "the request resolves within 5s")
                   (is (= {:echo {:n 7} :ok true} (:data reply))
                       "core/request resolves with the codec-decoded reply the handler responded")
                   (is (some? (:subject reply))
                       "the reply is a real message (carries a subject)")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [cfg (assoc-in config [:endpoints 0 :handler]
                                      (fn [msg] (service/respond conn msg {:echo (:data msg) :ok true})))]
                    (-> (service/create conn cfg)
                        (p/then (fn [svc]
                                  (with-service svc
                                    (fn []
                                      (-> (nats/request conn "tracer.svc.echo" {:n 7} {:timeout-ms 5000})
                                          (p/then (fn [reply]
                                                    (is (= {:echo {:n 7} :ok true} (:data reply))
                                                        "core/request resolves with the codec-decoded reply the handler responded")
                                                    (is (some? (:subject reply))
                                                        "the reply is a real message (carries a subject)"))))))))
                        (p/catch (fn [e]
                                   (is false (str "request rejected unexpectedly: " e))))))))))))

;; An endpoint's :subject defaults to its :name when omitted: the handler must be
;; reachable on the :name as a subject when no :subject is given.
(deftest endpoint-subject-defaults-to-name
  ;; An endpoint name is a single subject token (both natives reject dots in a
  ;; name), so the default-to-name listen subject is that token. A request to the
  ;; token reaches the endpoint when no explicit :subject is given.
  (let [config {:name "default_subj_svc" :version "0.1.0"
                :endpoints [{:name "byname"
                             :handler (fn [_] :placeholder)}]}]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [cfg (assoc-in config [:endpoints 0 :handler]
                               (fn [msg] (service/respond conn msg {:ok true})))
                 svc (deref (service/create conn cfg) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (let [reply (deref (nats/request conn "byname" {} {:timeout-ms 5000}) 5000 ::timeout)]
                   (is (= {:ok true} (:data reply))
                       "the endpoint listens on its :name when :subject is omitted")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [cfg (assoc-in config [:endpoints 0 :handler]
                                      (fn [msg] (service/respond conn msg {:ok true})))]
                    (-> (service/create conn cfg)
                        (p/then (fn [svc]
                                  (with-service svc
                                    (fn []
                                      (-> (nats/request conn "byname" {} {:timeout-ms 5000})
                                          (p/then (fn [reply]
                                                    (is (= {:ok true} (:data reply))
                                                        "the endpoint listens on its :name when :subject is omitted"))))))))
                        (p/catch (fn [e]
                                   (is false (str "request rejected unexpectedly: " e))))))))))))

;; An explicit :subject is honored (the endpoint listens there, NOT on its :name),
;; and a :queue-group is accepted; a request to the explicit subject is answered.
(deftest explicit-subject-and-queue-group-are-honored
  (let [config {:name "explicit_svc" :version "0.1.0"
                :endpoints [{:name "worker" :subject "tracer.svc.explicit"
                             :queue-group "tracer-workers"
                             :handler (fn [_] :placeholder)}]}]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [cfg (assoc-in config [:endpoints 0 :handler]
                               (fn [msg] (service/respond conn msg {:ok true})))
                 svc (deref (service/create conn cfg) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (let [reply (deref (nats/request conn "tracer.svc.explicit" {} {:timeout-ms 5000}) 5000 ::timeout)
                       on-name (reject-reason (nats/request conn "worker" {} {:timeout-ms 800}))]
                   (is (= {:ok true} (:data reply))
                       "the endpoint listens on its explicit :subject (queue-group accepted)")
                   (is (= :no-responders (:type (ex-data on-name)))
                       "the endpoint does NOT also listen on its :name when :subject is explicit")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [cfg (assoc-in config [:endpoints 0 :handler]
                                      (fn [msg] (service/respond conn msg {:ok true})))]
                    (-> (service/create conn cfg)
                        (p/then (fn [svc]
                                  (with-service svc
                                    (fn []
                                      (-> (nats/request conn "tracer.svc.explicit" {} {:timeout-ms 5000})
                                          (p/then (fn [reply]
                                                    (is (= {:ok true} (:data reply))
                                                        "the endpoint listens on its explicit :subject (queue-group accepted)")))
                                          (p/then (fn [_]
                                                    (-> (nats/request conn "worker" {} {:timeout-ms 800})
                                                        (p/then (fn [_] (is false "expected no responder on :name when :subject is explicit")))
                                                        (p/catch (fn [e]
                                                                   (is (= :no-responders (:type (ex-data e)))
                                                                       "the endpoint does NOT also listen on its :name when :subject is explicit")))))))))))
                        (p/catch (fn [e]
                                   (is false (str "request rejected unexpectedly: " e))))))))))))

;; (stop svc) resolves and tears the Service down: after it settles the endpoint is
;; gone, so a request to its subject rejects with :no-responders on both legs.
(deftest stop-tears-the-service-down
  (let [config {:name "stop_svc" :version "0.1.0"
                :endpoints [{:name "ping" :subject "tracer.svc.stop"
                             :handler (fn [_] :placeholder)}]}]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [cfg (assoc-in config [:endpoints 0 :handler]
                               (fn [msg] (service/respond conn msg {:ok true})))
                 svc (deref (service/create conn cfg) 5000 ::timeout)]
             (deref (nats/request conn "tracer.svc.stop" {} {:timeout-ms 5000}) 5000 ::timeout)
             (is (not= ::timeout (deref (service/stop svc) 5000 ::timeout))
                 "stop resolves within 5s")
             (let [after (reject-reason (nats/request conn "tracer.svc.stop" {} {:timeout-ms 800}))]
               (is (= :no-responders (:type (ex-data after)))
                   "after stop the endpoint is gone — a request rejects with :no-responders")))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [cfg (assoc-in config [:endpoints 0 :handler]
                                      (fn [msg] (service/respond conn msg {:ok true})))]
                    (-> (service/create conn cfg)
                        (p/then (fn [svc]
                                  (-> (nats/request conn "tracer.svc.stop" {} {:timeout-ms 5000})
                                      (p/then (fn [_] (service/stop svc))))))
                        (p/then (fn [_]
                                  (-> (nats/request conn "tracer.svc.stop" {} {:timeout-ms 800})
                                      (p/then (fn [_] (is false "expected :no-responders after stop")))
                                      (p/catch (fn [e]
                                                 (is (= :no-responders (:type (ex-data e)))
                                                     "after stop the endpoint is gone — a request rejects with :no-responders"))))))
                        (p/catch (fn [e]
                                   (is false (str "stop teardown failed unexpectedly: " e))))))))))))

;; (respond-error conn msg code description data?) reaches the caller as a reply
;; whose (service/error msg) is {:code … :description …}, with the data? body
;; decoded as a normal reply; a SUCCESS reply reads (service/error) => nil. And
;; core/request RESOLVES (does not reject) on a service-error reply — an application
;; error is data the caller branches on, not a thrown transport failure (ADR 0025).
(deftest respond-error-reaches-the-caller-as-a-reply-payload
  (let [config {:name "err_svc" :version "0.1.0"
                :endpoints [{:name "bad"  :subject "errs.svc.bad"  :handler (fn [_] :placeholder)}
                            {:name "good" :subject "errs.svc.good" :handler (fn [_] :placeholder)}]}]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [cfg (-> config
                         (assoc-in [:endpoints 0 :handler]
                                   (fn [msg] (service/respond-error conn msg 400 "bad input" {:why :nope})))
                         (assoc-in [:endpoints 1 :handler]
                                   (fn [msg] (service/respond conn msg {:ok true}))))
                 svc (deref (service/create conn cfg) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (let [err (deref (nats/request conn "errs.svc.bad" {} {:timeout-ms 5000}) 5000 ::timeout)
                       ok  (deref (nats/request conn "errs.svc.good" {} {:timeout-ms 5000}) 5000 ::timeout)]
                   (is (not= ::timeout err) "core/request RESOLVES on a service-error reply (does not reject/time out)")
                   (is (= {:code 400 :description "bad input"} (service/error err))
                       "(service/error reply) reads the {:code :description} the handler responded")
                   (is (= {:why :nope} (:data err)) "the data? body rides the error reply, codec-decoded")
                   (is (nil? (service/error ok)) "(service/error reply) is nil on a success reply")
                   (is (= {:ok true} (:data ok)) "a success reply decodes normally")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [cfg (-> config
                                (assoc-in [:endpoints 0 :handler]
                                          (fn [msg] (service/respond-error conn msg 400 "bad input" {:why :nope})))
                                (assoc-in [:endpoints 1 :handler]
                                          (fn [msg] (service/respond conn msg {:ok true}))))]
                    (-> (service/create conn cfg)
                        (p/then (fn [svc]
                                  (with-service svc
                                    (fn []
                                      (p/let [err (nats/request conn "errs.svc.bad" {} {:timeout-ms 5000})
                                              ok  (nats/request conn "errs.svc.good" {} {:timeout-ms 5000})]
                                        (is (= {:code 400 :description "bad input"} (service/error err))
                                            "(service/error reply) reads the {:code :description} the handler responded")
                                        (is (= {:why :nope} (:data err)) "the data? body rides the error reply, codec-decoded")
                                        (is (nil? (service/error ok)) "(service/error reply) is nil on a success reply")
                                        (is (= {:ok true} (:data ok)) "a success reply decodes normally"))))))
                        (p/catch (fn [e]
                                   (is false (str "request rejected unexpectedly: " e))))))))))))

;; A handler that THROWS or returns a REJECTED promise auto-replies code 500 with
;; the exception's description (ADR 0025) — core/request still resolves, and
;; (service/error reply) is {:code 500 :description <non-empty>}. The exact
;; description text differs per leg (jnats' Throwable.toString vs the JS error
;; message), so assert the code and a non-blank description, not exact equality.
(deftest a-thrown-or-rejected-handler-auto-replies-500
  (let [config {:name "auto500_svc" :version "0.1.0"
                :endpoints [{:name "throws"  :subject "errs.svc.throws"
                             :handler (fn [_] (throw (ex-info "kaboom" {})))}
                            {:name "rejects" :subject "errs.svc.rejects"
                             :handler (fn [_]
                                        #?(:clj  (doto (java.util.concurrent.CompletableFuture.)
                                                   (.completeExceptionally (ex-info "kaboom" {})))
                                           :cljs (p/rejected (ex-info "kaboom" {}))))}]}]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [svc (deref (service/create conn config) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (doseq [subject ["errs.svc.throws" "errs.svc.rejects"]]
                   (let [reply (deref (nats/request conn subject {} {:timeout-ms 5000}) 5000 ::timeout)
                         err   (service/error reply)]
                     (is (not= ::timeout reply) (str subject " resolves (never hangs to timeout)"))
                     (is (= 500 (:code err)) (str subject " auto-replies code 500"))
                     (is (and (string? (:description err)) (seq (:description err)))
                         (str subject " carries the exception's description")))))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (service/create conn config)
                      (p/then (fn [svc]
                                (with-service svc
                                  (fn []
                                    (p/let [throws  (nats/request conn "errs.svc.throws" {} {:timeout-ms 5000})
                                            rejects (nats/request conn "errs.svc.rejects" {} {:timeout-ms 5000})]
                                      (doseq [reply [throws rejects]]
                                        (let [err (service/error reply)]
                                          (is (= 500 (:code err)) "auto-replies code 500")
                                          (is (and (string? (:description err)) (seq (:description err)))
                                              "carries the exception's description"))))))))
                      (p/catch (fn [e]
                                 (is false (str "request rejected unexpectedly: " e)))))))))))

;; A request to a subject NO Service hosts rejects with the normalized
;; :no-responders Error — services does not change the canonical Error set; an
;; absent responder is still transport, not a service-error payload (ADR 0025).
(deftest request-to-an-unhosted-subject-rejects-no-responders
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [e (reject-reason (nats/request conn "errs.svc.nobody" {} {:timeout-ms 800}))]
           (is (= :no-responders (:type (ex-data e)))
               "no Service hosts the subject — the request rejects with :no-responders"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (-> (nats/request conn "errs.svc.nobody" {} {:timeout-ms 800})
                    (p/then (fn [_] (is false "expected :no-responders for an unhosted subject")))
                    (p/catch (fn [e]
                               (is (= :no-responders (:type (ex-data e)))
                                   "no Service hosts the subject — the request rejects with :no-responders")))))))))
