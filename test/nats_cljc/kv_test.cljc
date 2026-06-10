(ns nats-cljc.kv-test
  "Portable KV suite (ADR 0017/0023): one `.cljc` source run on the JVM and Node
   (browser is CI-only, ADR 0010). Mirrors `jetstream-test`'s connect/teardown
   envelope, with the distinct-subject convention lifted to Buckets — one distinct
   Bucket per test so the shared server never cross-feeds."
  (:require #?(:clj  [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is async]])
            [nats-cljc.core :as nats]
            [nats-cljc.kv :as kv]
            [nats-cljc.kv.impl.bucket :as bucket]
            [nats-cljc.kv.impl.error :as kv-err]
            #?(:cljs [promesa.core :as p])))

;; The anonymous server (ci/nats.conf) is the only leg with a jetstream{} block:
;; the KV context is obtained here. TCP on the JVM, ws on CLJS (ADR 0001).
(def ^:private server-url
  #?(:clj  "nats://127.0.0.1:4222"
     :cljs "ws://127.0.0.1:8080"))

;; The token server (ci/nats-token.conf) has NO jetstream{} block, so JetStream is
;; disabled there: it is the leg that proves (kv conn) verifies at entry on both
;; platforms (ADR 0017) — without the forced round-trip (jnats' keyValueManagement
;; and @nats-io/kv's Kvm are both cheap local constructions), a context would
;; resolve here and only fail on the first operation.
(def ^:private token-server-url
  #?(:clj  "nats://127.0.0.1:4223"
     :cljs "ws://127.0.0.1:8081"))

(def ^:private token "s3cr3t-token")

;; Capture the validation `:type` a synchronous deep-module guard throws, portably.
(defn- thrown-type [thunk]
  (try (thunk) nil
       (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e (:type (ex-data e)))))

;; A canonical, fully-specified portable Bucket config — every supported key set —
;; so validation sees the whole closed set and the live create below exercises each
;; key's native translation. :memory so a skipped teardown vanishes on restart.
(defn- a-config [bucket]
  {:bucket bucket :description "tracer bucket" :history 5 :ttl-ms 60000
   :max-value-size 1024 :max-bucket-size 1048576 :storage :memory :replicas 1
   :compression? false})

;; Deep-module unit (ADR 0015), no server: the closed-key + required-key + name
;; guards are the portable pre-flight half of Bucket config, raising their
;; validation `:type`s before any native call.
(deftest deep-module-bucket-config-validation
  (let [config (a-config "OK")]
    (is (= config (bucket/validate-config config))
        "a fully-specified recognized config passes validation unchanged"))
  (is (= :unknown-config-key (thrown-type #(bucket/validate-config {:bucket "OK" :bogus 1})))
      "an unrecognized key (the map is closed) is :unknown-config-key")
  (is (= [:bogus] (:keys (ex-data (try (bucket/validate-config {:bucket "OK" :bogus 1})
                                       (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e e)))))
      ":unknown-config-key carries the offending :keys")
  (is (= :missing-required-key (thrown-type #(bucket/validate-config {:storage :memory})))
      "a config omitting :bucket is :missing-required-key, not :invalid-name {:name nil}")
  (is (= :invalid-name (thrown-type #(bucket/validate-config {:bucket "bad.name"})))
      "a Bucket name carrying a subject delimiter is :invalid-name")
  (is (= :invalid-name (thrown-type #(bucket/validate-name "bad name")))
      "a Bucket name carrying whitespace is :invalid-name — stricter than a stream name")
  (is (= "ok-bucket_1" (bucket/validate-name "ok-bucket_1"))
      "a well-formed Bucket name (alphanumerics, dash, underscore) passes unchanged"))

;; Deep-module unit (ADR 0023), no server: the KV impl layer owns the mapping from
;; the substrate's stream-flavored failures to KV-flavored canonical `:type`s. The
;; shared err_code table re-faces only the codes with a KV face; everything else
;; falls through to the Phase-2 normalization, so the entry point's 10039 and the
;; operational catch-all surface identically across facades.
(deftest deep-module-kv-error-classifier
  (is (= {:type :bucket-not-found :code 10059 :description "stream not found"}
         (kv-err/api-error-data 10059 "stream not found"))
      "the substrate's not-found 10059 is re-faced :bucket-not-found, never :stream-not-found")
  (is (= :jetstream-not-enabled (:type (kv-err/api-error-data 10039 "not enabled")))
      "a code with no KV face falls through to the shared JetStream table")
  (is (= :jetstream-api-error (:type (kv-err/api-error-data 99999 "anything")))
      "an unseeded code defaults to the operational catch-all :jetstream-api-error"))

;; Test-only teardown, as in jetstream-test: close the native client directly so
;; its threads / ws socket don't outlive the test.
(defn- close! [conn] (.close (:client conn)))

;; The connect / settle / teardown envelope (jetstream-test's `with-conn`): JVM
;; blocks on connect, runs `(f conn)`, closes in a finally; CLJS awaits the promise
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
;; seam (jetstream-test's helper): `.whenComplete` hands back the BARE ex-info ADR
;; 0006's portable `(:type (ex-data e))` contract targets, not deref's
;; ExecutionException wrapper.
#?(:clj
   (defn- reject-reason [^java.util.concurrent.CompletableFuture cf]
     (let [a (promise)]
       (.whenComplete cf (reify java.util.function.BiConsumer
                           (accept [_ _ e] (deliver a e))))
       (deref a 5000 ::timeout))))

;; Bucket teardown guard (jetstream-test's `with-stream`, lifted to Buckets): run
;; `f`, then ALWAYS delete `bucket` — even when an assertion derefs to ::timeout or
;; a promise rejects mid-body — so a memory Bucket can't leak into a re-run.
;; Teardown is best-effort: a delete failure is swallowed so it can't mask the
;; body's outcome. Create the Bucket BEFORE the guard so a failed create never
;; deletes a Bucket that isn't there.
#?(:clj
   (defn- with-bucket [ctx bucket f]
     (try (f)
          (finally (try (deref (kv/delete-bucket ctx bucket) 5000 ::timeout)
                        (catch Throwable _ nil)))))
   :cljs
   (defn- with-bucket [ctx bucket f]
     (p/handle (p/do (f))
               (fn [v e]
                 (p/handle (kv/delete-bucket ctx bucket)
                           (fn [_ _] (if e (throw e) v)))))))

;; (kv conn) resolves to a single KV context against a JetStream-enabled server,
;; identically on both legs (ADR 0017's twin).
(deftest kv-resolves-to-a-context
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (kv/kv conn) 5000 ::timeout)]
           (is (not= ::timeout ctx) "(kv conn) resolves within 5s")
           (is (some? ctx) "(kv conn) resolves to a non-nil KV context"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (-> (kv/kv conn)
                    (p/then (fn [ctx]
                              (is (some? ctx)
                                  "(kv conn) resolves to a non-nil KV context")))
                    ;; Attribute an unexpected rejection here, not to with-conn's
                    ;; outer "connect failed" catch (the connection already settled).
                    (p/catch (fn [e]
                               (is false (str "(kv conn) rejected unexpectedly: " e))))))))))

;; Against a JetStream-disabled server, (kv conn) rejects with
;; :jetstream-not-enabled at the handle, identically on both legs (ADR 0017/0020).
;; Both natives construct their KV management object without touching the server,
;; so this can only pass if the verify round-trip is forced on each leg.
(deftest kv-not-enabled-rejects
  #?(:clj
     (with-conn {:servers [token-server-url] :auth {:token token}}
       (fn [conn]
         (let [e (reject-reason (kv/kv conn))]
           (is (= :jetstream-not-enabled (:type (ex-data e)))
               "(kv conn) rejects with :jetstream-not-enabled on a JS-disabled server"))))
     :cljs
     (async done
            (with-conn {:servers [token-server-url] :auth {:token token}} done
              (fn [conn]
                (-> (kv/kv conn)
                    (p/then (fn [_] (is false "expected (kv conn) to reject with :jetstream-not-enabled")))
                    (p/catch (fn [e]
                               (is (= :jetstream-not-enabled (:type (ex-data e)))
                                   "(kv conn) rejects with :jetstream-not-enabled on a JS-disabled server")))))))))

;; create-bucket resolves to a Bucket handle naming the Bucket it binds, from the
;; fully-specified config — exercising every key's native translation live.
(deftest create-bucket-resolves-to-a-bucket-handle
  (let [bucket "TRACER_KV_CREATE"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx (deref (kv/kv conn) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (let [handle (deref (kv/create-bucket ctx (a-config bucket)) 5000 ::timeout)]
                   (is (not= ::timeout handle) "create-bucket resolves within 5s")
                   (is (= bucket (:bucket handle))
                       "create-bucket resolves to a Bucket handle naming its Bucket")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (with-bucket ctx bucket
                                  (fn []
                                    (-> (kv/create-bucket ctx (a-config bucket))
                                        (p/then (fn [handle]
                                                  (is (= bucket (:bucket handle))
                                                      "create-bucket resolves to a Bucket handle naming its Bucket")))))))))))))))

;; open-bucket resolves to a Bucket handle for an existing Bucket.
(deftest open-bucket-resolves-for-an-existing-bucket
  (let [bucket "TRACER_KV_OPEN"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx (deref (kv/kv conn) 5000 ::timeout)]
             (deref (kv/create-bucket ctx {:bucket bucket :storage :memory}) 5000 ::timeout)
             (with-bucket ctx bucket
               (fn []
                 (let [handle (deref (kv/open-bucket ctx bucket) 5000 ::timeout)]
                   (is (not= ::timeout handle) "open-bucket resolves within 5s")
                   (is (= bucket (:bucket handle))
                       "open-bucket resolves to a Bucket handle naming its Bucket")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory})
                                    (p/then (fn [_]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/open-bucket ctx bucket)
                                                      (p/then (fn [handle]
                                                                (is (= bucket (:bucket handle))
                                                                    "open-bucket resolves to a Bucket handle naming its Bucket"))))))))))))))))))

;; open-bucket on a Bucket that was never created rejects with :bucket-not-found —
;; KV vocabulary, never the stream substrate's :type (ADR 0023). On the leg whose
;; native open merely binds (nats.js), this can only pass if the existence
;; round-trip is forced.
(deftest open-bucket-missing-rejects-bucket-not-found
  (let [bucket "TRACER_KV_OPEN_MISSING"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx (deref (kv/kv conn) 5000 ::timeout)
                 e   (reject-reason (kv/open-bucket ctx bucket))]
             (is (= :bucket-not-found (:type (ex-data e)))
                 "open-bucket on a missing Bucket rejects with :bucket-not-found"))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/open-bucket ctx bucket)
                                    (p/then (fn [_] (is false "expected open-bucket to reject with :bucket-not-found")))
                                    (p/catch (fn [e]
                                               (is (= :bucket-not-found (:type (ex-data e)))
                                                   "open-bucket on a missing Bucket rejects with :bucket-not-found")))))))))))))

;; delete-bucket on a Bucket that was never created rejects with
;; :bucket-not-found — the same KV face as open (ADR 0023), backing the facade's
;; documented contract on both legs.
(deftest delete-bucket-missing-rejects-bucket-not-found
  (let [bucket "TRACER_KV_DELETE_MISSING"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx (deref (kv/kv conn) 5000 ::timeout)
                 e   (reject-reason (kv/delete-bucket ctx bucket))]
             (is (= :bucket-not-found (:type (ex-data e)))
                 "delete-bucket on a missing Bucket rejects with :bucket-not-found"))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/delete-bucket ctx bucket)
                                    (p/then (fn [_] (is false "expected delete-bucket to reject with :bucket-not-found")))
                                    (p/catch (fn [e]
                                               (is (= :bucket-not-found (:type (ex-data e)))
                                                   "delete-bucket on a missing Bucket rejects with :bucket-not-found")))))))))))))

;; delete-bucket removes the Bucket (resolving nil); a subsequent open-bucket
;; rejects with :bucket-not-found, proving the decommission took (ADR 0023).
(deftest delete-bucket-then-open-rejects-bucket-not-found
  (let [bucket "TRACER_KV_DELETE"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx (deref (kv/kv conn) 5000 ::timeout)]
             (deref (kv/create-bucket ctx {:bucket bucket :storage :memory}) 5000 ::timeout)
             (with-bucket ctx bucket
               (fn []
                 (is (nil? (deref (kv/delete-bucket ctx bucket) 5000 ::timeout))
                     "delete-bucket resolves to nil once the Bucket is gone")
                 (let [e (reject-reason (kv/open-bucket ctx bucket))]
                   (is (= :bucket-not-found (:type (ex-data e)))
                       "open-bucket after delete-bucket rejects with :bucket-not-found")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory})
                                    (p/then (fn [_]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/delete-bucket ctx bucket)
                                                      (p/then (fn [v]
                                                                (is (nil? v)
                                                                    "delete-bucket resolves to nil once the Bucket is gone")
                                                                (kv/open-bucket ctx bucket)))
                                                      (p/then (fn [_] (is false "expected open-bucket to reject with :bucket-not-found")))
                                                      (p/catch (fn [e]
                                                                 (is (= :bucket-not-found (:type (ex-data e)))
                                                                     "open-bucket after delete-bucket rejects with :bucket-not-found"))))))))))))))))))
