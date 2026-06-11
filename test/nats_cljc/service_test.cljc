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
            [nats-cljc.service.impl.config :as config]
            #?@(:cljs [[nats-cljc.test-support :as ts]
                       [promesa.core :as p]])))

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

;; Capture the validation `:type` a synchronous deep-module guard throws, portably
;; (kv-test's helper) — the no-server pre-flight tests below assert on it.
(defn- thrown-type [thunk]
  (try (thunk) nil
       (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e (:type (ex-data e)))))

;; Capture the whole ex-data a synchronous deep-module guard throws, so a test can
;; assert the offending value the validation `:type` carries (`:key`/`:version`/`:name`).
(defn- thrown-data [thunk]
  (try (thunk) nil
       (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e (ex-data e))))

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

;; (stop svc) DRAINS in-flight requests (ADR 0024): a request being handled when
;; stop is called still receives its reply, never dropped mid-request. Verified with
;; a deliberately slow handler — fire the request, wait until the handler is provably
;; in flight (it flips a flag on entry), call stop, and assert the ORIGINAL caller
;; still resolves with the handler's reply. The slow handler holds the dispatcher
;; in-flight by RETURNING a promise (ADR 0007 backpressure) that responds only after
;; a delay, so stop lands while the request is mid-handle.
(deftest stop-drains-an-in-flight-request
  (let [config {:name "drain_svc" :version "0.1.0"
                :endpoints [{:name "slow" :subject "tracer.svc.drain"
                             :handler (fn [_] :placeholder)}]}]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [in-flight (promise)
                 cfg (assoc-in config [:endpoints 0 :handler]
                               (fn [msg]
                                 (java.util.concurrent.CompletableFuture/runAsync
                                  (reify Runnable
                                    (run [_]
                                      (deliver in-flight true)
                                      (Thread/sleep 1500)
                                      (service/respond conn msg {:ok true}))))))
                 svc (deref (service/create conn cfg) 5000 ::timeout)
                 reply-fut (nats/request conn "tracer.svc.drain" {} {:timeout-ms 8000})]
             (is (= true (deref in-flight 5000 ::timeout)) "the handler is provably in flight")
             (let [stop-fut (service/stop svc)
                   reply    (deref reply-fut 8000 ::timeout)]
               (is (not= ::timeout reply) "the in-flight request still resolves (was not dropped by stop)")
               (is (= {:ok true} (:data reply))
                   "the caller receives the drained handler's reply")
               (is (nil? (deref stop-fut 5000 ::timeout)) "stop resolves to nil after draining")))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [in-flight (atom false)
                        cfg (assoc-in config [:endpoints 0 :handler]
                                      (fn [msg]
                                        (reset! in-flight true)
                                        (-> (p/delay 1500)
                                            (p/then (fn [_] (service/respond conn msg {:ok true}))))))]
                    (-> (service/create conn cfg)
                        (p/then (fn [svc]
                                  (let [reply-fut (nats/request conn "tracer.svc.drain" {} {:timeout-ms 8000})]
                                    (-> (ts/wait-for #(deref in-flight) 5000)
                                        (p/then (fn [up?]
                                                  (is (true? up?) "the handler is provably in flight")
                                                  (let [stop-fut (service/stop svc)]
                                                    (p/let [reply reply-fut
                                                            stopped stop-fut]
                                                      (is (= {:ok true} (:data reply))
                                                          "the caller receives the drained handler's reply")
                                                      (is (nil? stopped) "stop resolves to nil after draining")))))))))
                        (p/catch (fn [e]
                                   (is false (str "drain test failed unexpectedly: " e))))))))))))

;; The Service handle carries a :stopped promise that resolves to nil once the
;; Service stops for any reason (ADR 0024) — the lifecycle parallel of the Watch
;; handle's :initialized. It is unresolved while the Service runs, and resolves to
;; nil after stop, observed off the handle as (:stopped svc).
(deftest stopped-promise-resolves-to-nil-on-stop
  (let [config {:name "stopped_svc" :version "0.1.0"
                :endpoints [{:name "noop" :subject "tracer.svc.stopped"
                             :handler (fn [_] :placeholder)}]}]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [cfg (assoc-in config [:endpoints 0 :handler]
                               (fn [msg] (service/respond conn msg {:ok true})))
                 svc (deref (service/create conn cfg) 5000 ::timeout)]
             (is (not (.isDone ^java.util.concurrent.CompletableFuture (:stopped svc)))
                 "the :stopped promise is unresolved while the Service runs")
             (deref (service/stop svc) 5000 ::timeout)
             (is (nil? (deref (:stopped svc) 5000 ::timeout))
                 "the :stopped promise resolves to nil once the Service stops"))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [cfg (assoc-in config [:endpoints 0 :handler]
                                      (fn [msg] (service/respond conn msg {:ok true})))]
                    (-> (service/create conn cfg)
                        (p/then (fn [svc]
                                  (-> (service/stop svc)
                                      (p/then (fn [_] (:stopped svc)))
                                      (p/then (fn [stopped]
                                                (is (nil? stopped)
                                                    "the :stopped promise resolves to nil once the Service stops"))))))
                        (p/catch (fn [e]
                                   (is false (str ":stopped test failed unexpectedly: " e))))))))))))

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

;; Deep-module unit (ADR 0015/0024), no server: the strict-from-day-one config
;; guards are the portable pre-flight half of a Service, raising their validation
;; `:type`s before any native or wire call — thrown identically on both legs. A
;; fully-specified config (incl. endpoints) passes unchanged; each malformed shape
;; raises its own `:type` carrying the offending value.
(deftest deep-module-config-validation
  (let [ok {:name "echo_svc" :version "1.2.3"
            :endpoints [{:name "a" :handler (fn [_] nil)}
                        {:name "b" :handler (fn [_] nil)}]}]
    (is (= ok (config/validate-config ok))
        "a fully-specified recognized config passes validation unchanged")
    (is (= ok (config/validate-config ok))
        "validation does not mutate the config it returns"))
  ;; absent :name / :version reuse :missing-required-key, carrying the offending :key
  (is (= :missing-required-key (thrown-type #(config/validate-config {:version "1.0.0"})))
      "a config omitting :name is :missing-required-key, not :invalid-name {:name nil}")
  (is (= :name (:key (thrown-data #(config/validate-config {:version "1.0.0"}))))
      ":missing-required-key for an absent :name carries the offending :key")
  (is (= :missing-required-key (thrown-type #(config/validate-config {:name "svc"})))
      "a config omitting :version is :missing-required-key")
  (is (= :version (:key (thrown-data #(config/validate-config {:name "svc"}))))
      ":missing-required-key for an absent :version carries the offending :key")
  ;; malformed service / endpoint :name reuses :invalid-name
  (is (= :invalid-name (thrown-type #(config/validate-config {:name "bad.name" :version "1.0.0"})))
      "a service name carrying a subject delimiter is :invalid-name")
  (is (= :invalid-name (thrown-type #(config/validate-config {:name "bad name" :version "1.0.0"})))
      "a service name carrying whitespace is :invalid-name")
  (is (= :invalid-name (thrown-type #(config/validate-config
                                      {:name "svc" :version "1.0.0"
                                       :endpoints [{:name "bad.ep" :handler (fn [_] nil)}]})))
      "a malformed endpoint :name is :invalid-name")
  (is (= "ok_svc-1" (config/validate-name "ok_svc-1"))
      "a well-formed name (alphanumerics, dash, underscore) passes unchanged")
  ;; non-semver :version raises the new :invalid-version carrying the offending :version
  (is (= :invalid-version (thrown-type #(config/validate-config {:name "svc" :version "nope"})))
      "a non-semver :version is :invalid-version")
  (is (= "nope" (:version (thrown-data #(config/validate-config {:name "svc" :version "nope"}))))
      ":invalid-version carries the offending :version")
  ;; two endpoints sharing a :name raise the new :duplicate-endpoint carrying the :name
  (is (= :duplicate-endpoint
         (thrown-type #(config/validate-config
                        {:name "svc" :version "1.0.0"
                         :endpoints [{:name "dup" :handler (fn [_] nil)}
                                     {:name "dup" :handler (fn [_] nil)}]})))
      "two endpoints sharing a :name is :duplicate-endpoint")
  (is (= "dup"
         (:name (thrown-data #(config/validate-config
                               {:name "svc" :version "1.0.0"
                                :endpoints [{:name "dup" :handler (fn [_] nil)}
                                            {:name "dup" :handler (fn [_] nil)}]}))))
      ":duplicate-endpoint carries the offending :name")
  ;; empty / absent :endpoints is legal — must not trip validation
  (is (= {:name "svc" :version "1.0.0"}
         (config/validate-config {:name "svc" :version "1.0.0"}))
      "a config with absent :endpoints passes validation")
  (is (= {:name "svc" :version "1.0.0" :endpoints []}
         (config/validate-config {:name "svc" :version "1.0.0" :endpoints []}))
      "a config with empty :endpoints passes validation"))

;; Deep-module unit (ADR 0015/0024), no server: the semver accept-set is PINNED on
;; the borderline pair both natives agree on at the 3.4.0 floor — "1.0" rejected (no
;; PATCH), "1.2.3-rc1+build" accepted (prerelease + build). The native pin lives in
;; the regex's comment, verified live against jnats' validateSemVer AND
;; @nats-io/services' parseSemVer; this asserts the portable guard tracks it.
(deftest deep-module-semver-accept-set
  (is (true? (config/valid-version? "1.2.3-rc1+build"))
      "the prerelease+build borderline is accepted — matches both natives at 3.4.0")
  (is (false? (config/valid-version? "1.0"))
      "the two-segment borderline is rejected — matches both natives at 3.4.0")
  (is (true? (config/valid-version? "1.2.3")) "a plain MAJOR.MINOR.PATCH is accepted")
  (is (true? (config/valid-version? "0.1.0")) "a 0.x version is accepted")
  (is (false? (config/valid-version? "v1.2.3")) "a leading 'v' is rejected — the regex is anchored")
  (is (false? (config/valid-version? "1.2.3.4")) "a four-segment version is rejected")
  (is (false? (config/valid-version? "")) "an empty version is rejected")
  (is (false? (config/valid-version? nil)) "a nil version is rejected"))

;; Facade rejection shape (ADR 0015/0024): the pre-flight rejects the create promise
;; — not throws synchronously — carrying the validation ex-info, identically on both
;; legs and before any native/wire call (no server is needed to observe it). The
;; canonical seam is the public facade, so this drives service/create directly.
(deftest create-rejects-the-promise-on-invalid-config
  (let [bad {:name "svc"}]  ;; missing :version
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [e (reject-reason (service/create conn bad))]
             (is (= :missing-required-key (:type (ex-data e)))
                 "create rejects its promise with the validation ex-info")
             (is (= :version (:key (ex-data e)))
                 "the rejection carries the offending :key"))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (service/create conn bad)
                      (p/then (fn [_] (is false "expected create to reject on a config missing :version")))
                      (p/catch (fn [e]
                                 (is (= :missing-required-key (:type (ex-data e)))
                                     "create rejects its promise with the validation ex-info")
                                 (is (= :version (:key (ex-data e)))
                                     "the rejection carries the offending :key"))))))))))
