(ns nats-cljc.service-test
  "Portable Services suite (ADR 0024): one `.cljc` source run on the JVM and Node
   (browser is CI-only, ADR 0010). Mirrors `kv-test`'s connect/teardown envelope.
   Services is pure core request-reply, so the anonymous server (ci/nats.conf)
   suffices — there is no JetStream block to need and no entry verification to
   exercise (ADR 0024). The facade is the only seam: every assertion drives
   `service/create`/`respond`/`stop` and the caller's plain `core/request`, never
   an impl ns or a native object, against the real server. The one deliberate
   exception is the semver native-gate probe (see
   `semver-borderlines-match-the-native-gate`): the portable pre-flight fires
   before the native ever sees a version, so native agreement can only be proven
   by probing each native's own gate beneath the facade."
  (:require #?(:clj  [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is async]])
            [nats-cljc.core :as nats]
            [nats-cljc.service :as service]
            #?@(:cljs [[nats-cljc.test-support :as ts]
                       [promesa.core :as p]
                       ["@nats-io/nats-core/internal" :refer [parseSemVer]]])))

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

;; The Service binds ONE codec at create — the connection default unless `:codec` in
;; the create config overrides it — applied to BOTH request decode and response
;; encode on every endpoint (ADR 0011). A single respond / respond-error may override
;; the codec per call. Connection default is :edn; the Service overrides to :string.
;;
;; The :string / :edn round-trip is the discriminator. The caller sends the map
;; {:n 7} with the connection default :edn, so the request bytes are the EDN text
;; "{:n 7}". If the Service decoded with :string it sees the raw STRING "{:n 7}" (not
;; the map), echoes it, and :string-encodes it back to the raw bytes `{:n 7}`. The
;; caller then reads the reply two ways: decoding with :string yields the string
;; "{:n 7}" (proving the Service's bound :string decoded the request — it echoed a
;; string, not a map), and decoding with :edn yields the MAP {:n 7} (proving the
;; Service's bound :string ENCODED the reply — had it wrongly used the :edn default,
;; the bytes would be the quoted "\"{:n 7}\"" and the :edn decode would give the
;; string, not the map). That :edn-decode = map assertion is the red-before-green
;; lever: the pre-slice respond, encoding with the connection :edn default, fails it.
;;
;; The override endpoint flips it back per call: the Service is :string, but its
;; handler responds the string "hi" with a per-call {:codec :edn} override, so the
;; reply bytes are the EDN text "\"hi\"". The caller :edn-decodes that to the string
;; "hi"; had the override been ignored and the Service's :string used, the bytes
;; would be the bare `hi` and the :edn decode would give the SYMBOL hi — so the
;; string "hi" proves just that one reply honored the override. respond-error does
;; the same for its data? body.
(deftest codec-binds-at-create-and-overrides-per-respond
  (let [config {:name "codec_svc" :version "0.1.0"
                :codec :string
                :endpoints [{:name "echo" :subject "codec.svc.echo"     :handler (fn [_] :placeholder)}
                            {:name "ovr"  :subject "codec.svc.ovr"      :handler (fn [_] :placeholder)}
                            {:name "ovre" :subject "codec.svc.ovre"     :handler (fn [_] :placeholder)}]}]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           ;; conn default codec is :edn (no :codec on connect).
           (let [cfg (-> config
                         ;; Service codec :string: echo back the decoded request as the Service saw it.
                         (assoc-in [:endpoints 0 :handler]
                                   (fn [msg] (service/respond conn msg (:data msg))))
                         ;; Per-call :edn override on respond.
                         (assoc-in [:endpoints 1 :handler]
                                   (fn [msg] (service/respond conn msg "hi" {:codec :edn})))
                         ;; Per-call :edn override on respond-error's data.
                         (assoc-in [:endpoints 2 :handler]
                                   (fn [msg] (service/respond-error conn msg 400 "nope" "bad" {:codec :edn}))))
                 svc (deref (service/create conn cfg) 5000 ::timeout)]
             (with-service svc
               (fn []
                 ;; echo: caller sends the map {:n 7} with the connection :edn default.
                 (let [as-string (deref (nats/request conn "codec.svc.echo" {:n 7} {:timeout-ms 5000 :codec :string}) 5000 ::timeout)
                       as-edn    (deref (nats/request conn "codec.svc.echo" {:n 7} {:timeout-ms 5000}) 5000 ::timeout)]
                   (is (= "{:n 7}" (:data as-string))
                       "request decode honored the Service's bound :string (it echoed the raw string, not the map)")
                   (is (= {:n 7} (:data as-edn))
                       "response encode honored the Service's bound :string (the reply bytes :edn-decode to the map)"))
                 ;; ovr: per-call :edn override on respond, read back with the connection :edn default.
                 (let [reply (deref (nats/request conn "codec.svc.ovr" {} {:timeout-ms 5000}) 5000 ::timeout)]
                   (is (= "hi" (:data reply))
                       "respond's per-call :codec override encoded just this reply in :edn"))
                 ;; ovre: per-call :edn override on respond-error's data.
                 (let [err (deref (nats/request conn "codec.svc.ovre" {} {:timeout-ms 5000}) 5000 ::timeout)]
                   (is (= {:code 400 :description "nope"} (service/error err))
                       "the error reply reads back its {:code :description}")
                   (is (= "bad" (:data err))
                       "respond-error's per-call :codec override encoded the error reply's data in :edn")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [cfg (-> config
                                (assoc-in [:endpoints 0 :handler]
                                          (fn [msg] (service/respond conn msg (:data msg))))
                                (assoc-in [:endpoints 1 :handler]
                                          (fn [msg] (service/respond conn msg "hi" {:codec :edn})))
                                (assoc-in [:endpoints 2 :handler]
                                          (fn [msg] (service/respond-error conn msg 400 "nope" "bad" {:codec :edn}))))]
                    (-> (service/create conn cfg)
                        (p/then (fn [svc]
                                  (with-service svc
                                    (fn []
                                      (p/let [as-string (nats/request conn "codec.svc.echo" {:n 7} {:timeout-ms 5000 :codec :string})
                                              as-edn    (nats/request conn "codec.svc.echo" {:n 7} {:timeout-ms 5000})
                                              reply     (nats/request conn "codec.svc.ovr" {} {:timeout-ms 5000})
                                              err       (nats/request conn "codec.svc.ovre" {} {:timeout-ms 5000})]
                                        (is (= "{:n 7}" (:data as-string))
                                            "request decode honored the Service's bound :string (it echoed the raw string, not the map)")
                                        (is (= {:n 7} (:data as-edn))
                                            "response encode honored the Service's bound :string (the reply bytes :edn-decode to the map)")
                                        (is (= "hi" (:data reply))
                                            "respond's per-call :codec override encoded just this reply in :edn")
                                        (is (= {:code 400 :description "nope"} (service/error err))
                                            "the error reply reads back its {:code :description}")
                                        (is (= "bad" (:data err))
                                            "respond-error's per-call :codec override encoded the error reply's data in :edn"))))))
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

;; ADR-0007 serialization gate: an endpoint handler is an ordinary push Handler, so
;; delivery to ONE endpoint is serial and a returned promise applies backpressure —
;; the next request to that endpoint waits for the prior handler's promise to settle.
;; A slow async handler must therefore DELAY the next request to the same endpoint,
;; and the endpoint's :processing-time-ns must reflect the AWAITED handler duration,
;; not just the synchronous callback time. The JVM leg falls out of the dispatcher
;; blocking on the CompletionStage; the JS leg drives the endpoint as an async
;; iterable (road 2) rather than a callback, which nats.js does NOT await.
;;
;; KNOWN-BAD (watch red first): with the JS impl on a `{:handler …}` CALLBACK
;; subscription, nats.js invokes the handler synchronously and does not await its
;; promise — both requests' handlers enter ~together (gap ≈ 0, serial-gap assert
;; fails) and :processing-time-ns reflects only the synchronous callback time (the
;; awaited-duration assert fails). Driving the async iterable makes both go green.
(deftest slow-handler-serializes-and-times-the-awaited-duration
  (let [delay-ms   600
        ;; allow scheduler slop: the gate has held if the 2nd handler entered at
        ;; least most of the delay after the 1st, and the awaited duration shows up.
        floor-ms   (long (* 0.6 delay-ms))
        floor-ns   (* floor-ms 1000000)
        config     {:name "gate_svc" :version "0.1.0"
                    :endpoints [{:name "slow" :subject "tracer.svc.gate"
                                 :handler (fn [_] :placeholder)}]}]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [entries (atom [])
                 cfg (assoc-in config [:endpoints 0 :handler]
                               (fn [msg]
                                 (swap! entries conj (System/currentTimeMillis))
                                 (java.util.concurrent.CompletableFuture/runAsync
                                  (reify Runnable
                                    (run [_]
                                      (Thread/sleep delay-ms)
                                      (service/respond conn msg {:ok true}))))))
                 svc (deref (service/create conn cfg) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (let [a (nats/request conn "tracer.svc.gate" {} {:timeout-ms 10000})
                       b (nats/request conn "tracer.svc.gate" {} {:timeout-ms 10000})]
                   (is (not= ::timeout (deref a 10000 ::timeout)) "request A resolves")
                   (is (not= ::timeout (deref b 10000 ::timeout)) "request B resolves")
                   (let [[e1 e2] @entries]
                     (is (= 2 (count @entries)) "both handler invocations were recorded")
                     (is (>= (- e2 e1) floor-ms)
                         "the 2nd handler did not enter until the 1st's awaited promise settled (serial delivery)"))
                   (let [st (first (deref (service/stats conn {:name "gate_svc"}) 10000 ::timeout))
                         ep (first (:endpoints st))]
                     (is (>= (:processing-time-ns ep) floor-ns)
                         ":processing-time-ns reflects the awaited handler duration, not the synchronous callback time"))))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [entries (atom [])
                        cfg (assoc-in config [:endpoints 0 :handler]
                                      (fn [msg]
                                        (swap! entries conj (js/Date.now))
                                        (-> (p/delay delay-ms)
                                            (p/then (fn [_] (service/respond conn msg {:ok true}))))))]
                    (-> (service/create conn cfg)
                        (p/then (fn [svc]
                                  (with-service svc
                                    (fn []
                                      (let [a (nats/request conn "tracer.svc.gate" {} {:timeout-ms 10000})
                                            b (nats/request conn "tracer.svc.gate" {} {:timeout-ms 10000})]
                                        (p/let [_  a
                                                _  b
                                                ss (service/stats conn {:name "gate_svc"})]
                                          (let [[e1 e2] @entries
                                                ep (first (:endpoints (first ss)))]
                                            (is (= 2 (count @entries)) "both handler invocations were recorded")
                                            (is (>= (- e2 e1) floor-ms)
                                                "the 2nd handler did not enter until the 1st's awaited promise settled (serial delivery)")
                                            (is (>= (:processing-time-ns ep) floor-ns)
                                                ":processing-time-ns reflects the awaited handler duration, not the synchronous callback time"))))))))
                        (p/catch (fn [e]
                                   (is false (str "serialization-gate test failed unexpectedly: " e))))))))))))

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

;; respond-error sends EXACTLY ONE reply on the wire and is NOT terminal — it is
;; core/reply-shaped: the handler keeps running after it, and no auto-500 straggler
;; follows the explicit reply (ADR 0025). Proven below the request machinery (a
;; single-reply core/request would hide a second reply): a hand-rolled request —
;; a native publish with an explicit reply-to, the one native touch core/publish
;; cannot express — and a plain subscription on the reply box collecting EVERY
;; reply that lands there. After the first reply a settle window lets any
;; straggler arrive before the count is asserted.
(deftest respond-error-replies-exactly-once-and-is-not-terminal
  (let [endpoint-subject "term.svc.rerr"
        box-subject      "term.svc.reply-box"
        config {:name "term_svc" :version "0.1.0"
                :endpoints [{:name "rerr" :subject endpoint-subject
                             :handler (fn [_] :placeholder)}]}]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [after (atom false)
                 cfg   (assoc-in config [:endpoints 0 :handler]
                                 (fn [msg]
                                   (service/respond-error conn msg 400 "bad")
                                   (reset! after true)))
                 svc   (deref (service/create conn cfg) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (let [replies (atom [])
                       arrived (promise)
                       _sub    (nats/subscribe conn box-subject
                                               (fn [m]
                                                 (swap! replies conj m)
                                                 (deliver arrived true)))]
                   (.publish ^io.nats.client.Connection (:client conn)
                             endpoint-subject box-subject (.getBytes "{}"))
                   (is (not= ::timeout (deref arrived 5000 ::timeout))
                       "the explicit error reply lands in the reply box")
                   (Thread/sleep 500)
                   (is (= 1 (count @replies)) "exactly one reply reached the wire")
                   (is (= {:code 400 :description "bad"} (service/error (first @replies)))
                       "the one reply is the explicit respond-error, not an auto-500")
                   (is (true? @after)
                       "respond-error is not terminal — handler code after it ran")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [after (atom false)
                        cfg   (assoc-in config [:endpoints 0 :handler]
                                        (fn [msg]
                                          (service/respond-error conn msg 400 "bad")
                                          (reset! after true)))]
                    (-> (service/create conn cfg)
                        (p/then (fn [svc]
                                  (with-service svc
                                    (fn []
                                      (let [replies (atom [])
                                            arrived (p/deferred)
                                            _sub    (nats/subscribe conn box-subject
                                                                    (fn [m]
                                                                      (swap! replies conj m)
                                                                      (p/resolve! arrived true)))]
                                        (.publish ^js (:client conn) endpoint-subject
                                                  (.encode (js/TextEncoder.) "{}")
                                                  #js {:reply box-subject})
                                        (p/let [_ (p/timeout arrived 5000 ::timeout)
                                                _ (p/delay 500)]
                                          (is (= 1 (count @replies)) "exactly one reply reached the wire")
                                          (is (= {:code 400 :description "bad"} (service/error (first @replies)))
                                              "the one reply is the explicit respond-error, not an auto-500")
                                          (is (true? @after)
                                              "respond-error is not terminal — handler code after it ran")))))))
                        (p/catch (fn [e]
                                   (is false (str "exactly-once test failed unexpectedly: " e))))))))))))

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

;; ── Discovery: ping / info / stats (ADR 0024) ────────────────────────────────

;; ping resolves a vector of identity maps; info adds :description + :endpoints;
;; stats adds :started + per-endpoint counters — kebab EDN, the wire `type`
;; discriminator dropped, normalized identically on both legs. Each call is narrowed
;; by :name so the shared :4222 server's other Services don't bleed into the assertion.
(deftest ping-info-stats-resolve-normalized-vectors
  (let [config {:name "disc_shape" :version "2.3.4" :description "shape probe"
                :endpoints [{:name "echo" :subject "disc.shape.echo"
                             :handler (fn [_] :placeholder)}]}
        opts   {:name "disc_shape"}
        ;; The portable assertions on the three results, identical on both legs.
        check  (fn [p i s]
                 (is (= 1 (count p)) "ping narrows to the one Service of that name")
                 (let [pe (first p) ie (first i) se (first s) ep (first (:endpoints se))]
                   (is (= {:name "disc_shape" :version "2.3.4"}
                          (select-keys pe [:name :version])) "ping is an identity map")
                   (is (string? (:id pe)) "ping carries the instance :id")
                   (is (nil? (:type pe)) "the wire `type` discriminator is dropped")
                   (is (= "shape probe" (:description ie)) "info adds :description")
                   (is (= [{:name "echo" :subject "disc.shape.echo" :queue-group "q"}]
                          (mapv #(select-keys % [:name :subject :queue-group]) (:endpoints ie)))
                       "info adds :endpoints")
                   (is (nil? (:type ie)) "info drops `type`")
                   (is (string? (:started se)) "stats adds :started as a string")
                   (is (re-find #"^\d{4}-\d{2}-\d{2}T.*Z$" (:started se))
                       ":started is the canonical UTC timestamp string (same form as KV :created)")
                   (is (= "echo" (:name ep)) "stats carries the per-endpoint counter map")
                   (is (= 0 (:num-requests ep)) "a fresh endpoint reports a zero request count")
                   (is (= 0 (:num-errors ep)) "a fresh endpoint reports a zero error count")
                   (is (int? (:processing-time-ns ep)) ":processing-time-ns is an integer")
                   (is (int? (:average-processing-time-ns ep)) ":average-processing-time-ns is an integer")
                   (is (nil? (:type se)) "stats drops `type`")
                   (is (= (:id pe) (:id ie) (:id se)) "the three views agree on the instance :id")))]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [svc (deref (service/create conn config) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (check (deref (service/ping conn opts) 10000 ::timeout)
                        (deref (service/info conn opts) 10000 ::timeout)
                        (deref (service/stats conn opts) 10000 ::timeout)))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (service/create conn config)
                      (p/then (fn [svc]
                                (with-service svc
                                  (fn []
                                    (p/let [p (service/ping conn opts)
                                            i (service/info conn opts)
                                            s (service/stats conn opts)]
                                      (check p i s))))))
                      (p/catch (fn [e] (is false (str "discovery rejected unexpectedly: " e)))))))))))

;; Service and endpoint :metadata round-trip create → discovery with ONE key shape
;; on both legs: keys and values serialize as wire strings (a keyword contributes
;; its name, no leading colon), and ping/info/stats lift the wire JSON object back
;; as a string-keyed map of strings — identical on the JVM and Node.
(deftest metadata-round-trips-create-to-discovery
  (let [config   {:name "disc_meta" :version "1.0.0"
                  :metadata {:region "eu" "tier" "gold"}
                  :endpoints [{:name "echo" :subject "disc.meta.echo"
                               :metadata {:visibility "public"}
                               :handler (fn [_] :placeholder)}]}
        opts     {:name "disc_meta"}
        svc-meta {"region" "eu" "tier" "gold"}
        check    (fn [p i s]
                   (is (= svc-meta (:metadata (first p)))
                       "ping lifts the service :metadata as a string-keyed map")
                   (is (= svc-meta (:metadata (first i)))
                       "info lifts the service :metadata as a string-keyed map")
                   (is (= svc-meta (:metadata (first s)))
                       "stats lifts the service :metadata as a string-keyed map")
                   (is (= {"visibility" "public"}
                          (:metadata (first (:endpoints (first i)))))
                       "info lifts the endpoint :metadata as a string-keyed map"))]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [svc (deref (service/create conn config) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (check (deref (service/ping conn opts) 10000 ::timeout)
                        (deref (service/info conn opts) 10000 ::timeout)
                        (deref (service/stats conn opts) 10000 ::timeout)))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (service/create conn config)
                      (p/then (fn [svc]
                                (with-service svc
                                  (fn []
                                    (p/let [p (service/ping conn opts)
                                            i (service/info conn opts)
                                            s (service/stats conn opts)]
                                      (check p i s))))))
                      (p/catch (fn [e] (is false (str "discovery rejected unexpectedly: " e)))))))))))

;; Discovery narrows by :name to a specific Service and by :id to a specific
;; instance: a matching :id resolves that one instance, a non-matching :id resolves
;; an empty vector. Identical on both legs.
(deftest discovery-narrows-by-name-and-id
  (let [config {:name "disc_narrow" :version "1.0.0"
                :endpoints [{:name "e" :subject "disc.narrow.e" :handler (fn [_] :placeholder)}]}
        check  (fn [ping-name ping-id ping-wrong]
                 (is (= ["disc_narrow"] (mapv :name ping-name)) ":name narrows to that Service")
                 (is (= 1 (count ping-id)) ":id narrows to that one instance")
                 (is (= (:id (first ping-name)) (:id (first ping-id))) "the :id-narrowed instance matches")
                 (is (= [] ping-wrong) "a non-matching :id resolves an empty vector"))]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [svc (deref (service/create conn config) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (let [pn (deref (service/ping conn {:name "disc_narrow"}) 10000 ::timeout)
                       id (:id (first pn))]
                   (check pn
                          (deref (service/ping conn {:name "disc_narrow" :id id}) 10000 ::timeout)
                          (deref (service/ping conn {:name "disc_narrow" :id "no-such-id"}) 10000 ::timeout))))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (service/create conn config)
                      (p/then (fn [svc]
                                (with-service svc
                                  (fn []
                                    (p/let [pn (service/ping conn {:name "disc_narrow"})
                                            id (:id (first pn))
                                            pi (service/ping conn {:name "disc_narrow" :id id})
                                            pw (service/ping conn {:name "disc_narrow" :id "no-such-id"})]
                                      (check pn pi pw))))))
                      (p/catch (fn [e] (is false (str "discovery rejected unexpectedly: " e)))))))))))

;; :id alone — no :name — narrows to that one instance: with two instances of the
;; same Service running, an :id-only ping resolves exactly the matching instance.
;; The $SRV control subjects only encode name.id, so the legs broadcast and filter
;; client-side; identical on both legs.
(deftest discovery-narrows-by-id-alone
  (let [config {:name "disc_idonly" :version "1.0.0"
                :endpoints [{:name "e" :subject "disc.idonly.e" :handler (fn [_] :placeholder)}]}
        ;; Bound the broadcast so the gather terminates without waiting the full
        ;; default window; high enough to reach both instances plus bystanders.
        opts   (fn [id] {:id id :timeout-ms 1500 :max-results 50})
        check  (fn [both narrowed]
                 (is (= 2 (count both)) "two instances of the Service are up")
                 (is (apply distinct? (mapv :id both)) "the instances have distinct :ids")
                 (is (= [(first both)] narrowed)
                     ":id alone resolves exactly the matching instance"))]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [svc-a (deref (service/create conn config) 5000 ::timeout)
                 svc-b (deref (service/create conn config) 5000 ::timeout)]
             (with-service svc-a
               (fn []
                 (with-service svc-b
                   (fn []
                     (let [both (deref (service/ping conn {:name "disc_idonly"}) 10000 ::timeout)
                           id   (:id (first both))]
                       (check both
                              (deref (service/ping conn (opts id)) 10000 ::timeout))))))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (p/let [svc-a (service/create conn config)
                              svc-b (service/create conn config)]
                        (with-service svc-a
                          (fn []
                            (with-service svc-b
                              (fn []
                                (p/let [both     (service/ping conn {:name "disc_idonly"})
                                        narrowed (service/ping conn (opts (:id (first both))))]
                                  (check both narrowed)))))))
                      (p/catch (fn [e] (is false (str "discovery rejected unexpectedly: " e)))))))))))

;; :max-results and :timeout-ms bound the fan-out so the gather terminates
;; predictably: a tiny :timeout-ms still resolves (does not hang), and :max-results
;; caps the vector length. Asserted against this one Service narrowed by :name.
(deftest max-results-and-timeout-ms-bound-the-fan-out
  (let [config {:name "disc_bound" :version "1.0.0"
                :endpoints [{:name "e" :subject "disc.bound.e" :handler (fn [_] :placeholder)}]}
        check  (fn [timed capped]
                 (is (vector? timed) ":timeout-ms bounds the gather — it resolves a vector, never hangs")
                 (is (<= 1 (count timed)) "the timed gather still reached this Service")
                 (is (>= 1 (count capped)) ":max-results caps the gathered vector length"))]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [svc (deref (service/create conn config) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (check (deref (service/ping conn {:name "disc_bound" :timeout-ms 200}) 5000 ::timeout)
                        (deref (service/ping conn {:name "disc_bound" :max-results 1}) 5000 ::timeout)))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (service/create conn config)
                      (p/then (fn [svc]
                                (with-service svc
                                  (fn []
                                    (p/let [timed  (service/ping conn {:name "disc_bound" :timeout-ms 200})
                                            capped (service/ping conn {:name "disc_bound" :max-results 1})]
                                      (check timed capped))))))
                      (p/catch (fn [e] (is false (str "discovery rejected unexpectedly: " e)))))))))))

;; JVM-only (ADR 0001/0002): the discovery gather runs OFF the caller's thread.
;; With :name narrowing and one live instance below the default :max-results, the
;; jnats fan-out keeps gathering for its full :timeout-ms window before resolving —
;; so if `service/ping` itself returns well inside that window, the caller's thread
;; was never blocked on the gather; the promise still resolves the same normalized
;; vector afterwards.
#?(:clj
   (deftest discovery-gathers-off-the-callers-thread
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [config {:name "disc_offload" :version "1.0.0"
                       :endpoints [{:name "e" :subject "disc.offload.e"
                                    :handler (fn [_] :placeholder)}]}
               svc (deref (service/create conn config) 5000 ::timeout)]
           (with-service svc
             (fn []
               (let [t0 (System/nanoTime)
                     p  (service/ping conn {:name "disc_offload" :timeout-ms 2000})
                     elapsed-ms (/ (- (System/nanoTime) t0) 1e6)]
                 (is (< elapsed-ms 1000)
                     (str "ping returned the caller's thread inside the 2000ms fan-out window"
                          " (took " (long elapsed-ms) "ms)"))
                 (is (= ["disc_offload"]
                        (mapv :name (deref p 5000 ::timeout)))
                     "the off-thread gather still resolves the normalized vector")))))))))

;; A discovery fan-out forced onto a CLOSED connection rejects with the normalized
;; :connection-closed ex-info (ADR 0006) — never the raw host error (a bare jnats
;; IllegalStateException / a nats.js ClosedConnectionError) — so a consumer
;; branches on `(:type (ex-data e))` identically across legs.
(deftest discovery-on-a-closed-connection-rejects-connection-closed
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (close! conn)
         (doseq [[verb f] [["ping" service/ping] ["info" service/info] ["stats" service/stats]]]
           (let [e (reject-reason (f conn {:timeout-ms 500}))]
             (is (= :connection-closed (:type (ex-data e)))
                 (str verb " on a closed connection rejects the normalized :connection-closed"))))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [check (fn [verb f]
                              (-> (f conn {:timeout-ms 500})
                                  (p/then (fn [v] (is false (str verb " resolved unexpectedly: " v))))
                                  (p/catch (fn [e]
                                             (is (= :connection-closed (:type (ex-data e)))
                                                 (str verb " on a closed connection rejects the normalized :connection-closed"))))))]
                  (-> (p/do (close! conn))
                      (p/then (fn [_]
                                (p/do (check "ping" service/ping)
                                      (check "info" service/info)
                                      (check "stats" service/stats)))))))))))

;; Stats counters MOVE: a handled request increments the endpoint's request count,
;; and an UNCAUGHT handler failure — a throw or a rejected promise — increments its
;; error count. An explicit `respond-error` does NOT: it is an ordinary reply
;; carrying an error payload, and both natives tally an endpoint error only on a
;; handler throw, never on the reply itself (ADR 0025). Drive one request at each
;; endpoint, then read stats narrowed by name.
(deftest stats-counters-move-on-handled-and-errored-requests
  (let [check (fn [by]
                (is (= 1 (:num-requests (by "ok")))   "a handled request increments the request count")
                (is (= 0 (:num-errors (by "ok")))     "a success does not increment the error count")
                (is (= 1 (:num-requests (by "rerr")))  "respond-error still counts the request")
                (is (= 0 (:num-errors (by "rerr")))   "an explicit respond-error is NOT an endpoint error — only an uncaught failure counts")
                (is (= 1 (:num-errors (by "boom")))   "a thrown handler increments the endpoint error count"))]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [cfg {:name "disc_counters" :version "1.0.0"
                      :endpoints [{:name "ok"   :subject "disc.cnt.ok"
                                   :handler (fn [m] (service/respond conn m {:ok true}))}
                                  {:name "rerr" :subject "disc.cnt.rerr"
                                   :handler (fn [m] (service/respond-error conn m 400 "bad"))}
                                  {:name "boom" :subject "disc.cnt.boom"
                                   :handler (fn [_] (throw (ex-info "kaboom" {})))}]}
                 svc (deref (service/create conn cfg) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (doseq [s ["disc.cnt.ok" "disc.cnt.rerr" "disc.cnt.boom"]]
                   (deref (nats/request conn s {} {:timeout-ms 5000}) 5000 ::timeout))
                 (let [st (first (deref (service/stats conn {:name "disc_counters"}) 10000 ::timeout))
                       by (into {} (map (juxt :name identity) (:endpoints st)))]
                   (check by)))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (let [cfg {:name "disc_counters" :version "1.0.0"
                             :endpoints [{:name "ok"   :subject "disc.cnt.ok"
                                          :handler (fn [m] (service/respond conn m {:ok true}))}
                                         {:name "rerr" :subject "disc.cnt.rerr"
                                          :handler (fn [m] (service/respond-error conn m 400 "bad"))}
                                         {:name "boom" :subject "disc.cnt.boom"
                                          :handler (fn [_] (throw (ex-info "kaboom" {})))}]}]
                    (-> (service/create conn cfg)
                        (p/then (fn [svc]
                                  (with-service svc
                                    (fn []
                                      (p/let [_  (nats/request conn "disc.cnt.ok" {} {:timeout-ms 5000})
                                              _  (nats/request conn "disc.cnt.rerr" {} {:timeout-ms 5000})
                                              _  (nats/request conn "disc.cnt.boom" {} {:timeout-ms 5000})
                                              ss (service/stats conn {:name "disc_counters"})]
                                        (check (into {} (map (juxt :name identity) (:endpoints (first ss))))))))))
                        (p/catch (fn [e] (is false (str "discovery rejected unexpectedly: " e))))))))))))

;; A zero-endpoint Service is legal and still answers $SRV.* — it is discoverable via
;; ping, info, and stats, with an empty :endpoints vector on info and stats (ADR 0024).
(deftest zero-endpoint-service-is-discoverable
  (let [config {:name "disc_zero" :version "1.0.0"}
        check  (fn [p i s]
                 (is (= ["disc_zero"] (mapv :name p)) "a zero-endpoint Service answers ping")
                 (is (= [] (:endpoints (first i))) "info reports an empty :endpoints vector")
                 (is (= [] (:endpoints (first s))) "stats reports an empty :endpoints vector"))]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [svc (deref (service/create conn config) 5000 ::timeout)]
             (with-service svc
               (fn []
                 (check (deref (service/ping conn {:name "disc_zero"}) 10000 ::timeout)
                        (deref (service/info conn {:name "disc_zero"}) 10000 ::timeout)
                        (deref (service/stats conn {:name "disc_zero"}) 10000 ::timeout)))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (service/create conn config)
                      (p/then (fn [svc]
                                (with-service svc
                                  (fn []
                                    (p/let [p (service/ping conn {:name "disc_zero"})
                                            i (service/info conn {:name "disc_zero"})
                                            s (service/stats conn {:name "disc_zero"})]
                                      (check p i s))))))
                      (p/catch (fn [e] (is false (str "discovery rejected unexpectedly: " e)))))))))))

;; Facade rejection shape (ADR 0015/0024): the config pre-flight rejects the create
;; promise — not throws synchronously — carrying the validation ex-info, identically
;; on both legs and before any native or wire call. Every validation `:type` and the
;; offending value it carries is observed through the public facade only: each
;; malformed config shape drives service/create and the assertions read the
;; rejection's ex-data.
(deftest create-rejects-each-invalid-config-shape
  (let [cases [;; [config, expected :type, carried key, carried value, label]
               [{:version "1.0.0"}
                :missing-required-key :key :name "a config omitting :name"]
               [{:name "svc"}
                :missing-required-key :key :version "a config omitting :version"]
               [{:name "bad.name" :version "1.0.0"}
                :invalid-name :name "bad.name" "a service name carrying a subject delimiter"]
               [{:name "bad name" :version "1.0.0"}
                :invalid-name :name "bad name" "a service name carrying whitespace"]
               [{:name "svc" :version "1.0.0"
                 :endpoints [{:name "bad.ep" :handler (fn [_] nil)}]}
                :invalid-name :name "bad.ep" "a malformed endpoint :name"]
               [{:name "svc" :version "nope"}
                :invalid-version :version "nope" "a non-semver :version"]
               [{:name "svc" :version "1.0.0"
                 :endpoints [{:name "dup" :handler (fn [_] nil)}
                             {:name "dup" :handler (fn [_] nil)}]}
                :duplicate-endpoint :name "dup" "two endpoints sharing a :name"]]]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (doseq [[bad type k v label] cases]
             (let [data (ex-data (reject-reason (service/create conn bad)))]
               (is (= type (:type data))
                   (str label " rejects create with " type))
               (is (= v (get data k))
                   (str label "'s rejection carries " k))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (p/all (mapv (fn [[bad type k v label]]
                                 (-> (service/create conn bad)
                                     (p/then (fn [_] (is false (str label " must reject create"))))
                                     (p/catch (fn [e]
                                                (let [data (ex-data e)]
                                                  (is (= type (:type data))
                                                      (str label " rejects create with " type))
                                                  (is (= v (get data k))
                                                      (str label "'s rejection carries " k)))))))
                               cases))))))))

;; The semver gate, EXECUTED on the borderline pair (ADR 0015/0024): "1.0" (no
;; PATCH segment) must be rejected and "1.2.3-rc1+build" (prerelease + build)
;; accepted, identically by the portable pre-flight and by both natives at the
;; 3.4.0 floor. Through the facade, the pre-flight rejects "1.0" with
;; :invalid-version and admits "1.2.3-rc1+build" all the way to a running Service.
;; Native agreement needs a second seam: the portable pre-flight fires before the
;; native ever sees a version, so each native's accept-set is proven by probing,
;; beneath the facade, the exact gate its service factory runs — jnats'
;; ServiceBuilder calls io.nats.client.support.Validator/validateSemVer (throws
;; IllegalArgumentException on a non-semver), and nats.js' Service constructor
;; calls @nats-io/nats-core/internal's parseSemVer (throws on a non-match). Both
;; probes execute here, per leg.
(deftest semver-borderlines-match-the-native-gate
  ;; this leg's native gate, probed directly on the borderline pair
  #?(:clj
     (do (is (thrown? IllegalArgumentException
                      (io.nats.client.support.Validator/validateSemVer "1.0" "version" true))
             "jnats' validateSemVer rejects the two-segment borderline")
         (is (= "1.2.3-rc1+build"
                (io.nats.client.support.Validator/validateSemVer "1.2.3-rc1+build" "version" true))
             "jnats' validateSemVer accepts the prerelease+build borderline"))
     :cljs
     (do (is (thrown? js/Error (parseSemVer "1.0"))
             "nats.js' parseSemVer rejects the two-segment borderline")
         (is (some? (parseSemVer "1.2.3-rc1+build"))
             "nats.js' parseSemVer accepts the prerelease+build borderline")))
  ;; the same pair through the facade's portable pre-flight
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [data (ex-data (reject-reason
                              (service/create conn {:name "semver_svc" :version "1.0"})))]
           (is (= :invalid-version (:type data))
               "create rejects the two-segment borderline with :invalid-version")
           (is (= "1.0" (:version data))
               "the rejection carries the offending :version"))
         (let [svc (deref (service/create conn {:name "semver_svc"
                                                :version "1.2.3-rc1+build"})
                          5000 ::timeout)]
           (with-service svc
             (fn []
               (is (not= ::timeout svc) "create resolves within 5s")
               (is (some? svc)
                   "the prerelease+build borderline reaches a running Service"))))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (-> (service/create conn {:name "semver_svc" :version "1.0"})
                    (p/then (fn [_] (is false "expected create to reject the two-segment borderline")))
                    (p/catch (fn [e]
                               (let [data (ex-data e)]
                                 (is (= :invalid-version (:type data))
                                     "create rejects the two-segment borderline with :invalid-version")
                                 (is (= "1.0" (:version data))
                                     "the rejection carries the offending :version"))))
                    (p/then (fn [_]
                              (-> (service/create conn {:name "semver_svc"
                                                        :version "1.2.3-rc1+build"})
                                  (p/then (fn [svc]
                                            (with-service svc
                                              (fn []
                                                (is (some? svc)
                                                    "the prerelease+build borderline reaches a running Service")))))
                                  (p/catch (fn [e]
                                             (is false (str "create rejected unexpectedly: " e)))))))))))))
