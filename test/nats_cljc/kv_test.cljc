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
            [nats-cljc.test-support :refer [wait-for]]
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

;; Deep-module unit (ADR 0015), no server: the key-syntax seam every entry op runs
;; pre-flight, before any wire call — the charset both native clients enforce
;; (`[-/_=.a-zA-Z0-9]`), no leading/trailing `.`, and no wildcards — raising the
;; validation `:type :invalid-key` carrying the offending `:key`.
(deftest deep-module-key-validation
  (is (= "a.b-c_d/e=1" (bucket/validate-key "a.b-c_d/e=1"))
      "a well-formed key (full charset, interior dots) passes unchanged")
  (is (= :invalid-key (thrown-type #(bucket/validate-key "bad key")))
      "a key outside the charset (whitespace) is :invalid-key")
  (is (= :invalid-key (thrown-type #(bucket/validate-key ".lead")))
      "a leading dot is :invalid-key")
  (is (= :invalid-key (thrown-type #(bucket/validate-key "trail.")))
      "a trailing dot is :invalid-key")
  (is (= :invalid-key (thrown-type #(bucket/validate-key "wild.*")))
      "the * wildcard is :invalid-key — a KV key addresses exactly one entry")
  (is (= :invalid-key (thrown-type #(bucket/validate-key "wild.>")))
      "the > wildcard is :invalid-key")
  (is (= :invalid-key (thrown-type #(bucket/validate-key "")))
      "an empty key is :invalid-key")
  (is (= :invalid-key (thrown-type #(bucket/validate-key 42)))
      "a non-string key is :invalid-key")
  (is (= "bad key" (:key (ex-data (try (bucket/validate-key "bad key")
                                       (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e e)))))
      ":invalid-key carries the offending :key"))

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

;; Deep-module unit (ADR 0023), no server: the compare-and-set classifier each
;; leg's CAS verbs route their native failures through — the seam that re-faces
;; the substrate's wrong-last-sequence condition as the KV-vocabulary
;; :wrong-revision carrying the :key, shared by create and update (a create IS an
;; update expecting revision 0). Codes without a CAS face fall through to the
;; Bucket-verb normalization, so a CAS verb's other failures keep their faces.
(deftest deep-module-cas-error-classifier
  (is (= {:type :wrong-revision :key "config.app" :code 10071 :description "wrong last sequence: 3"}
         (kv-err/cas-error-data 10071 "wrong last sequence: 3" "config.app"))
      "the substrate's wrong-last-sequence 10071 is re-faced :wrong-revision carrying the :key")
  (is (= {:type :wrong-revision :key "config.app" :code 10164 :description "wrong last sequence"}
         (kv-err/cas-error-data 10164 "wrong last sequence" "config.app"))
      "the unknown-last-sequence sibling 10164 wears the same :wrong-revision face")
  (is (= :bucket-not-found (:type (kv-err/cas-error-data 10059 "stream not found" "k")))
      "a code with no CAS face falls through to the KV Bucket-verb normalization")
  (is (= :jetstream-api-error (:type (kv-err/cas-error-data 99999 "anything" "k")))
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

;; The first entry round trip: put resolves to the new Revision as a bare number,
;; and get resolves to the full Entry map with :value decoded through the Bucket's
;; Codec — the connection default (:edn) here, so a Clojure map survives the wire.
(deftest put-then-get-round-trips-an-entry
  (let [bucket "TRACER_KV_ENTRY_RT"
        value  {:a 1 :b "two"}]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (let [rev (deref (kv/put handle "config.app" value) 5000 ::timeout)]
                   (is (number? rev) "put resolves to the new Revision as a bare number")
                   (is (pos? rev) "the new Revision is positive")
                   (let [entry (deref (kv/get handle "config.app") 5000 ::timeout)]
                     (is (= bucket (:bucket entry)) "the Entry names its Bucket")
                     (is (= "config.app" (:key entry)) "the Entry names its key")
                     (is (= value (:value entry)) ":value is decoded through the Bucket's Codec (connection default)")
                     (is (= rev (:revision entry)) "the Entry's :revision is the Revision put resolved to")
                     (is (= :put (:operation entry)) "a written Entry carries the :put :operation")
                     (is (string? (:created entry)) ":created is the canonical timestamp string"))))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory})
                                    (p/then (fn [handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/put handle "config.app" value)
                                                      (p/then (fn [rev]
                                                                (is (number? rev) "put resolves to the new Revision as a bare number")
                                                                (is (pos? rev) "the new Revision is positive")
                                                                (-> (kv/get handle "config.app")
                                                                    (p/then (fn [entry]
                                                                              (is (= bucket (:bucket entry)) "the Entry names its Bucket")
                                                                              (is (= "config.app" (:key entry)) "the Entry names its key")
                                                                              (is (= value (:value entry)) ":value is decoded through the Bucket's Codec (connection default)")
                                                                              (is (= rev (:revision entry)) "the Entry's :revision is the Revision put resolved to")
                                                                              (is (= :put (:operation entry)) "a written Entry carries the :put :operation")
                                                                              (is (string? (:created entry)) ":created is the canonical timestamp string")))))))))))))))))))))

;; Key absence is a normal domain outcome, not an Error (ADR 0023): get on a key
;; never written resolves to nil, so callers branch with if-let — while a STORED
;; nil stays a full Entry `{:value nil ...}`, distinguishable from absence.
(deftest get-absent-resolves-nil-and-stored-nil-stays-an-entry
  (let [bucket "TRACER_KV_ABSENT"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (is (nil? (deref (kv/get handle "never.written") 5000 ::timeout))
                     "get on an absent key resolves to nil")
                 (deref (kv/put handle "stored.nil" nil) 5000 ::timeout)
                 (let [entry (deref (kv/get handle "stored.nil") 5000 ::timeout)]
                   (is (some? entry) "a stored nil resolves to an Entry, not nil")
                   (is (nil? (:value entry)) "the stored nil Entry carries :value nil")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory})
                                    (p/then (fn [handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/get handle "never.written")
                                                      (p/then (fn [entry]
                                                                (is (nil? entry) "get on an absent key resolves to nil")
                                                                (kv/put handle "stored.nil" nil)))
                                                      (p/then (fn [_] (kv/get handle "stored.nil")))
                                                      (p/then (fn [entry]
                                                                (is (some? entry) "a stored nil resolves to an Entry, not nil")
                                                                (is (nil? (:value entry)) "the stored nil Entry carries :value nil"))))))))))))))))))

;; The per-Bucket Codec seam: a :codec override at create/open governs every read
;; and write through that handle (no per-op override), while a handle opened
;; without one binds the connection default — whose decode failure on the raw
;; bytes the override wrote rejects the one-shot get with :codec-error.
(deftest codec-override-governs-the-bucket-handle
  (let [bucket "TRACER_KV_CODEC"
        raw    "{"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx      (deref (kv/kv conn) 5000 ::timeout)
                 s-handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory}
                                                   {:codec :string})
                                 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (deref (kv/put s-handle "raw.key" raw) 5000 ::timeout)
                 (is (= raw (:value (deref (kv/get s-handle "raw.key") 5000 ::timeout)))
                     "a create-time :codec override governs the handle's writes and reads")
                 (let [o-handle (deref (kv/open-bucket ctx bucket {:codec :string}) 5000 ::timeout)]
                   (is (= raw (:value (deref (kv/get o-handle "raw.key") 5000 ::timeout)))
                       "an open-time :codec override governs the handle's reads"))
                 (let [d-handle (deref (kv/open-bucket ctx bucket) 5000 ::timeout)
                       e        (reject-reason (kv/get d-handle "raw.key"))]
                   (is (= :codec-error (:type (ex-data e)))
                       "decode failure in a one-shot get rejects with :codec-error — the connection default applies absent an override")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory}
                                                      {:codec :string})
                                    (p/then (fn [s-handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/put s-handle "raw.key" raw)
                                                      (p/then (fn [_] (kv/get s-handle "raw.key")))
                                                      (p/then (fn [entry]
                                                                (is (= raw (:value entry))
                                                                    "a create-time :codec override governs the handle's writes and reads")
                                                                (kv/open-bucket ctx bucket {:codec :string})))
                                                      (p/then (fn [o-handle] (kv/get o-handle "raw.key")))
                                                      (p/then (fn [entry]
                                                                (is (= raw (:value entry))
                                                                    "an open-time :codec override governs the handle's reads")
                                                                (kv/open-bucket ctx bucket)))
                                                      (p/then (fn [d-handle]
                                                                (-> (kv/get d-handle "raw.key")
                                                                    (p/then (fn [_] (is false "expected the default-codec get to reject with :codec-error")))
                                                                    (p/catch (fn [e]
                                                                               (is (= :codec-error (:type (ex-data e)))
                                                                                   "decode failure in a one-shot get rejects with :codec-error — the connection default applies absent an override")))))))))))))))))))))

;; The facade rejects a malformed key pre-flight on every entry op that takes one
;; (ADR 0015): the deep-module guard above pins the syntax rules; this proves both
;; verbs route through it, rejecting with :invalid-key carrying :key.
(deftest entry-ops-reject-malformed-keys
  (let [bucket "TRACER_KV_BADKEY"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (let [e (reject-reason (kv/put handle "bad key" 1))]
                   (is (= :invalid-key (:type (ex-data e)))
                       "put with a malformed key rejects with :invalid-key")
                   (is (= "bad key" (:key (ex-data e)))
                       "the rejection carries the offending :key"))
                 (let [e (reject-reason (kv/get handle ".bad"))]
                   (is (= :invalid-key (:type (ex-data e)))
                       "get with a malformed key rejects with :invalid-key")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory})
                                    (p/then (fn [handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/put handle "bad key" 1)
                                                      (p/then (fn [_] (is false "expected put to reject with :invalid-key")))
                                                      (p/catch (fn [e]
                                                                 (is (= :invalid-key (:type (ex-data e)))
                                                                     "put with a malformed key rejects with :invalid-key")
                                                                 (is (= "bad key" (:key (ex-data e)))
                                                                     "the rejection carries the offending :key")))
                                                      (p/then (fn [_]
                                                                (-> (kv/get handle ".bad")
                                                                    (p/then (fn [_] (is false "expected get to reject with :invalid-key")))
                                                                    (p/catch (fn [e]
                                                                               (is (= :invalid-key (:type (ex-data e)))
                                                                                   "get with a malformed key rejects with :invalid-key")))))))))))))))))))))

;; The exact normalized Bucket status a-config + one put should yield on EVERY
;; leg, minus the observed :bytes (server-version-dependent overhead, asserted
;; pos? separately): the config keys reuse the bucket-config names where they
;; overlap, plus the observed :values entry count. `(dissoc status :bytes)`
;; compared against this map pins both the exact key set and each value — the
;; shape-parity contract (ADR 0006 spirit: shape parity, not cadence parity).
(defn- expected-status [bucket]
  {:bucket bucket :description "tracer bucket" :history 5 :ttl-ms 60000
   :max-value-size 1024 :max-bucket-size 1048576 :storage :memory :replicas 1
   :compression? false :values 1})

;; The shared status-map assertions both legs run over a `bucket-status` /
;; `list-buckets` element for the a-config Bucket holding one entry.
(defn- assert-status [bucket status via]
  (is (= (expected-status bucket) (dissoc status :bytes))
      (str via " resolves to the pinned normalized status map (key set and values)"))
  (is (pos? (:bytes status))
      (str via "'s observed :bytes counter is positive with one entry stored")))

;; The operator surface (the stream-names / list-streams precedent, lifted to
;; Buckets): bucket-names resolves to a vector of name strings, list-buckets to a
;; vector of normalized status maps, and bucket-status to the same map for one
;; Bucket — pinned field-by-field against a fully-specified config plus one put,
;; so both legs surface an identical shape (ADR 0023, KV vocabulary throughout).
(deftest operator-surface-reports-bucket-topology
  (let [bucket "TRACER_KV_STATUS"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx (a-config bucket)) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (deref (kv/put handle "k" "hello") 5000 ::timeout)
                 (let [names (deref (kv/bucket-names ctx) 5000 ::timeout)]
                   (is (vector? names) "bucket-names resolves to a vector")
                   (is (every? string? names) "bucket-names holds name strings")
                   (is (some #{bucket} names) "bucket-names names every Bucket, including this one"))
                 (assert-status bucket (deref (kv/bucket-status ctx bucket) 5000 ::timeout)
                                "bucket-status")
                 (let [statuses (deref (kv/list-buckets ctx) 5000 ::timeout)]
                   (is (vector? statuses) "list-buckets resolves to a vector")
                   (assert-status bucket
                                  (first (filter #(= bucket (:bucket %)) statuses))
                                  "list-buckets' element")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx (a-config bucket))
                                    (p/then (fn [handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/put handle "k" "hello")
                                                      (p/then (fn [_] (kv/bucket-names ctx)))
                                                      (p/then (fn [names]
                                                                (is (vector? names) "bucket-names resolves to a vector")
                                                                (is (every? string? names) "bucket-names holds name strings")
                                                                (is (some #{bucket} names) "bucket-names names every Bucket, including this one")
                                                                (kv/bucket-status ctx bucket)))
                                                      (p/then (fn [status]
                                                                (assert-status bucket status "bucket-status")
                                                                (kv/list-buckets ctx)))
                                                      (p/then (fn [statuses]
                                                                (is (vector? statuses) "list-buckets resolves to a vector")
                                                                (assert-status bucket
                                                                               (first (filter #(= bucket (:bucket %)) statuses))
                                                                               "list-buckets' element"))))))))))))))))))

;; bucket-status on a Bucket that was never created rejects with
;; :bucket-not-found — the same KV face as open/delete (ADR 0023).
(deftest bucket-status-missing-rejects-bucket-not-found
  (let [bucket "TRACER_KV_STATUS_MISSING"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx (deref (kv/kv conn) 5000 ::timeout)
                 e   (reject-reason (kv/bucket-status ctx bucket))]
             (is (= :bucket-not-found (:type (ex-data e)))
                 "bucket-status on a missing Bucket rejects with :bucket-not-found"))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/bucket-status ctx bucket)
                                    (p/then (fn [_] (is false "expected bucket-status to reject with :bucket-not-found")))
                                    (p/catch (fn [e]
                                               (is (= :bucket-not-found (:type (ex-data e)))
                                                   "bucket-status on a missing Bucket rejects with :bucket-not-found")))))))))))))

;; Capture how a native promise SETTLES, either way — `[:resolved v]` or
;; `[:rejected e]` with the BARE ex-info (ADR 0006) — so a race's two outcomes can
;; be collected without knowing in advance which writer loses. The JVM helper
;; blocks (the test thread is allowed to); the CLJS helper returns a promise of
;; the outcome pair.
#?(:clj
   (defn- settle [^java.util.concurrent.CompletableFuture cf]
     (let [a (promise)]
       (.whenComplete cf (reify java.util.function.BiConsumer
                           (accept [_ v e] (deliver a (if e [:rejected e] [:resolved v])))))
       (deref a 5000 [:rejected ::timeout])))
   :cljs
   (defn- settle [p]
     (p/handle p (fn [v e] (if e [:rejected e] [:resolved v])))))

;; The shared race assertions both legs run over the two settled outcomes:
;; exactly one winner resolving the new Revision, exactly one loser rejecting
;; :wrong-revision with the contested :key (ADR 0023).
(defn- assert-race [key o1 o2]
  (let [{resolved :resolved rejected :rejected} (group-by first [o1 o2])]
    (is (= 1 (count resolved)) "exactly one racing writer wins")
    (is (= 1 (count rejected)) "exactly one racing writer loses")
    (let [[_ rev] (first resolved)]
      (is (number? rev) "the winner resolves to the new Revision as a bare number"))
    (let [[_ e] (first rejected)]
      (is (= :wrong-revision (:type (ex-data e)))
          "the loser rejects with :wrong-revision")
      (is (= key (:key (ex-data e)))
          "the loser's rejection carries the contested :key"))))

;; First-writer-wins: create resolves to the new Revision on an absent key —
;; enabling initialization and locks — and rejects with :wrong-revision carrying
;; the :key once the key exists (ADR 0023): KV vocabulary, never the substrate's
;; :wrong-last-sequence, though the wire condition is the very same.
(deftest create-wins-once-then-rejects-wrong-revision
  (let [bucket "TRACER_KV_CAS_CREATE"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (let [rev (deref (kv/create handle "lock.owner" "writer-1") 5000 ::timeout)]
                   (is (number? rev) "create on an absent key resolves to the new Revision as a bare number")
                   (is (pos? rev) "the new Revision is positive"))
                 (let [e (reject-reason (kv/create handle "lock.owner" "writer-2"))]
                   (is (= :wrong-revision (:type (ex-data e)))
                       "create on an existing key rejects with :wrong-revision")
                   (is (= "lock.owner" (:key (ex-data e)))
                       "the rejection carries the contested :key"))
                 (is (= "writer-1" (:value (deref (kv/get handle "lock.owner") 5000 ::timeout)))
                     "the first writer's value survives the lost create"))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory})
                                    (p/then (fn [handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/create handle "lock.owner" "writer-1")
                                                      (p/then (fn [rev]
                                                                (is (number? rev) "create on an absent key resolves to the new Revision as a bare number")
                                                                (is (pos? rev) "the new Revision is positive")
                                                                (-> (kv/create handle "lock.owner" "writer-2")
                                                                    (p/then (fn [_] (is false "expected create on an existing key to reject with :wrong-revision")))
                                                                    (p/catch (fn [e]
                                                                               (is (= :wrong-revision (:type (ex-data e)))
                                                                                   "create on an existing key rejects with :wrong-revision")
                                                                               (is (= "lock.owner" (:key (ex-data e)))
                                                                                   "the rejection carries the contested :key"))))))
                                                      (p/then (fn [_] (kv/get handle "lock.owner")))
                                                      (p/then (fn [entry]
                                                                (is (= "writer-1" (:value entry))
                                                                    "the first writer's value survives the lost create"))))))))))))))))))

;; Revision-guarded update: update with the latest Revision resolves to the new
;; Revision; update with a stale Revision rejects with :wrong-revision carrying
;; the :key, leaving the guarded value untouched — concurrent writers cannot
;; silently clobber each other (ADR 0023).
(deftest update-guards-on-the-expected-revision
  (let [bucket "TRACER_KV_CAS_UPDATE"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (let [rev1 (deref (kv/put handle "config.app" 1) 5000 ::timeout)
                       rev2 (deref (kv/update handle "config.app" 2 rev1) 5000 ::timeout)]
                   (is (number? rev2) "update with the latest Revision resolves to the new Revision as a bare number")
                   (is (> rev2 rev1) "the new Revision succeeds the expected one")
                   (let [e (reject-reason (kv/update handle "config.app" 3 rev1))]
                     (is (= :wrong-revision (:type (ex-data e)))
                         "update with a stale Revision rejects with :wrong-revision")
                     (is (= "config.app" (:key (ex-data e)))
                         "the rejection carries the contested :key"))
                   (is (= 2 (:value (deref (kv/get handle "config.app") 5000 ::timeout)))
                       "the guarded value is untouched by the lost update")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory})
                                    (p/then (fn [handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/put handle "config.app" 1)
                                                      (p/then (fn [rev1]
                                                                (-> (kv/update handle "config.app" 2 rev1)
                                                                    (p/then (fn [rev2]
                                                                              (is (number? rev2) "update with the latest Revision resolves to the new Revision as a bare number")
                                                                              (is (> rev2 rev1) "the new Revision succeeds the expected one")
                                                                              (-> (kv/update handle "config.app" 3 rev1)
                                                                                  (p/then (fn [_] (is false "expected update with a stale Revision to reject with :wrong-revision")))
                                                                                  (p/catch (fn [e]
                                                                                             (is (= :wrong-revision (:type (ex-data e)))
                                                                                                 "update with a stale Revision rejects with :wrong-revision")
                                                                                             (is (= "config.app" (:key (ex-data e)))
                                                                                                 "the rejection carries the contested :key")))))))))
                                                      (p/then (fn [_] (kv/get handle "config.app")))
                                                      (p/then (fn [entry]
                                                                (is (= 2 (:value entry))
                                                                    "the guarded value is untouched by the lost update"))))))))))))))))))

;; A genuine two-writer race: both updates are in flight before either settles,
;; each expecting the same base Revision — the server serializes them, so exactly
;; one wins and exactly one rejects :wrong-revision (ADR 0023). Which one is
;; nondeterministic; the assertions hold either way.
(deftest racing-writers-yield-one-winner-and-one-wrong-revision
  (let [bucket "TRACER_KV_CAS_RACE"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (let [rev (deref (kv/put handle "race.key" "base") 5000 ::timeout)
                       p1  (kv/update handle "race.key" "writer-1" rev)
                       p2  (kv/update handle "race.key" "writer-2" rev)]
                   (assert-race "race.key" (settle p1) (settle p2))))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory})
                                    (p/then (fn [handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/put handle "race.key" "base")
                                                      (p/then (fn [rev]
                                                                (p/all [(settle (kv/update handle "race.key" "writer-1" rev))
                                                                        (settle (kv/update handle "race.key" "writer-2" rev))])))
                                                      (p/then (fn [[o1 o2]]
                                                                (assert-race "race.key" o1 o2))))))))))))))))))

;; The shared history assertions for two puts then a delete: history resolves to
;; a fully-realized vector of Entries oldest-to-newest — the live puts with their
;; decoded :values AND the Tombstone with its :operation visible (ADR 0023:
;; deletion stays observable to history readers) — each Entry carrying :delta,
;; the distance from the newest revision (verified meaningful on both natives).
(defn- assert-delete-history [bucket rev1 rev2 entries]
  (is (vector? entries) "history resolves to a fully-realized vector")
  (is (= 3 (count entries)) "history retains both puts and the Tombstone")
  (is (= [rev1 rev2] [(:revision (nth entries 0)) (:revision (nth entries 1))])
      "history is ordered oldest-to-newest by Revision")
  (is (= [:put :put :delete] (mapv :operation entries))
      "the Tombstone's :operation is visible alongside the retained puts")
  (is (= [{:v 1} {:v 2} nil] (mapv :value entries))
      "live Entries decode through the Bucket's Codec; the Tombstone carries :value nil")
  (is (= [2 1 0] (mapv :delta entries))
      "each history Entry carries :delta, the distance from the newest revision")
  (is (every? #(= bucket (:bucket %)) entries) "every history Entry names its Bucket")
  (is (every? #(= "doc.k" (:key %)) entries) "every history Entry names its key")
  (is (every? #(string? (:created %)) entries) ":created is the canonical timestamp string"))

;; delete writes a Tombstone (ADR 0023): it resolves to nil, the key then reads
;; as absent via get — while history retains every revision INCLUDING the
;; Tombstone, so deletion stays observable to history readers.
(deftest delete-tombstones-the-key-and-history-retains-it
  (let [bucket "TRACER_KV_DELETE"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (let [rev1 (deref (kv/put handle "doc.k" {:v 1}) 5000 ::timeout)
                       rev2 (deref (kv/put handle "doc.k" {:v 2}) 5000 ::timeout)]
                   (is (nil? (deref (kv/delete handle "doc.k") 5000 ::timeout))
                       "delete resolves to nil")
                   (is (nil? (deref (kv/get handle "doc.k") 5000 ::timeout))
                       "a deleted key reads as absent via get")
                   (assert-delete-history bucket rev1 rev2
                                          (deref (kv/history handle "doc.k") 5000 ::timeout))))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5})
                                    (p/then (fn [handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/put handle "doc.k" {:v 1})
                                                      (p/then (fn [rev1]
                                                                (-> (kv/put handle "doc.k" {:v 2})
                                                                    (p/then (fn [rev2]
                                                                              (-> (kv/delete handle "doc.k")
                                                                                  (p/then (fn [r]
                                                                                            (is (nil? r) "delete resolves to nil")
                                                                                            (kv/get handle "doc.k")))
                                                                                  (p/then (fn [entry]
                                                                                            (is (nil? entry) "a deleted key reads as absent via get")
                                                                                            (kv/history handle "doc.k")))
                                                                                  (p/then (fn [entries]
                                                                                            (assert-delete-history bucket rev1 rev2 entries))))))))))))))))))))))))

;; purge reclaims space for good (ADR 0023): it resolves to nil and erases the
;; key's history down to a single purge marker — wearing its :operation :purge —
;; so the key reads as absent and no prior revision survives.
(deftest purge-erases-history-to-a-single-marker
  (let [bucket "TRACER_KV_PURGE"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (deref (kv/put handle "doc.k" {:v 1}) 5000 ::timeout)
                 (deref (kv/put handle "doc.k" {:v 2}) 5000 ::timeout)
                 (deref (kv/delete handle "doc.k") 5000 ::timeout)
                 (is (nil? (deref (kv/purge handle "doc.k") 5000 ::timeout))
                     "purge resolves to nil")
                 (is (nil? (deref (kv/get handle "doc.k") 5000 ::timeout))
                     "a purged key reads as absent via get")
                 (let [entries (deref (kv/history handle "doc.k") 5000 ::timeout)]
                   (is (= 1 (count entries))
                       "purge erases the key's history down to a single marker")
                   (is (= :purge (:operation (first entries)))
                       "the surviving marker wears :operation :purge")
                   (is (nil? (:value (first entries)))
                       "the purge marker carries :value nil")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5})
                                    (p/then (fn [handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/put handle "doc.k" {:v 1})
                                                      (p/then (fn [_] (kv/put handle "doc.k" {:v 2})))
                                                      (p/then (fn [_] (kv/delete handle "doc.k")))
                                                      (p/then (fn [_] (kv/purge handle "doc.k")))
                                                      (p/then (fn [r]
                                                                (is (nil? r) "purge resolves to nil")
                                                                (kv/get handle "doc.k")))
                                                      (p/then (fn [entry]
                                                                (is (nil? entry) "a purged key reads as absent via get")
                                                                (kv/history handle "doc.k")))
                                                      (p/then (fn [entries]
                                                                (is (= 1 (count entries))
                                                                    "purge erases the key's history down to a single marker")
                                                                (is (= :purge (:operation (first entries)))
                                                                    "the surviving marker wears :operation :purge")
                                                                (is (nil? (:value (first entries)))
                                                                    "the purge marker carries :value nil"))))))))))))))))))

;; The destructive verbs take the optional :revision guard (ADR 0023): a stale
;; guard rejects with :wrong-revision carrying the :key — operators only remove
;; what they believe they are removing — while a current guard goes through and
;; resolves to nil, exactly like the unguarded form.
(deftest delete-and-purge-guard-on-the-expected-revision
  (let [bucket "TRACER_KV_DESTRUCTIVE_CAS"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (let [rev1 (deref (kv/put handle "doc.k" 1) 5000 ::timeout)
                       rev2 (deref (kv/put handle "doc.k" 2) 5000 ::timeout)]
                   (let [e (reject-reason (kv/delete handle "doc.k" {:revision rev1}))]
                     (is (= :wrong-revision (:type (ex-data e)))
                         "delete with a stale :revision guard rejects with :wrong-revision")
                     (is (= "doc.k" (:key (ex-data e)))
                         "the rejection carries the contested :key"))
                   (is (= 2 (:value (deref (kv/get handle "doc.k") 5000 ::timeout)))
                       "the guarded value survives the lost delete")
                   (is (nil? (deref (kv/delete handle "doc.k" {:revision rev2}) 5000 ::timeout))
                       "delete with the latest Revision resolves to nil")
                   (let [e (reject-reason (kv/purge handle "doc.k" {:revision rev2}))]
                     (is (= :wrong-revision (:type (ex-data e)))
                         "purge with a stale :revision guard rejects with :wrong-revision")
                     (is (= "doc.k" (:key (ex-data e)))
                         "the rejection carries the contested :key"))
                   (is (nil? (deref (kv/purge handle "doc.k" {:revision (inc rev2)}) 5000 ::timeout))
                       "purge with the latest Revision (the Tombstone's) resolves to nil")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5})
                                    (p/then (fn [handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/put handle "doc.k" 1)
                                                      (p/then (fn [rev1]
                                                                (-> (kv/put handle "doc.k" 2)
                                                                    (p/then (fn [rev2]
                                                                              (-> (kv/delete handle "doc.k" {:revision rev1})
                                                                                  (p/then (fn [_] (is false "expected delete with a stale :revision to reject with :wrong-revision")))
                                                                                  (p/catch (fn [e]
                                                                                             (is (= :wrong-revision (:type (ex-data e)))
                                                                                                 "delete with a stale :revision guard rejects with :wrong-revision")
                                                                                             (is (= "doc.k" (:key (ex-data e)))
                                                                                                 "the rejection carries the contested :key")))
                                                                                  (p/then (fn [_] (kv/get handle "doc.k")))
                                                                                  (p/then (fn [entry]
                                                                                            (is (= 2 (:value entry))
                                                                                                "the guarded value survives the lost delete")
                                                                                            (kv/delete handle "doc.k" {:revision rev2})))
                                                                                  (p/then (fn [r]
                                                                                            (is (nil? r) "delete with the latest Revision resolves to nil")
                                                                                            (-> (kv/purge handle "doc.k" {:revision rev2})
                                                                                                (p/then (fn [_] (is false "expected purge with a stale :revision to reject with :wrong-revision")))
                                                                                                (p/catch (fn [e]
                                                                                                           (is (= :wrong-revision (:type (ex-data e)))
                                                                                                               "purge with a stale :revision guard rejects with :wrong-revision")
                                                                                                           (is (= "doc.k" (:key (ex-data e)))
                                                                                                               "the rejection carries the contested :key"))))))
                                                                                  (p/then (fn [_] (kv/purge handle "doc.k" {:revision (inc rev2)})))
                                                                                  (p/then (fn [r]
                                                                                            (is (nil? r) "purge with the latest Revision (the Tombstone's) resolves to nil"))))))))))))))))))))))))

;; ─────────────────────────── History archaeology ────────────────────────────

;; get pinned to a Revision reads the past: {:revision n} resolves to the Entry
;; at that exact Revision — the archaeology read — while a Revision the Bucket
;; never assigned to the key (or that belongs to another key) reads as absent,
;; resolving nil on both legs.
(deftest get-pinned-to-a-revision-reads-the-past
  (let [bucket "TRACER_KV_GET_REV"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (let [rev1  (deref (kv/put handle "doc.k" {:v 1}) 5000 ::timeout)
                       rev2  (deref (kv/put handle "doc.k" {:v 2}) 5000 ::timeout)
                       other (deref (kv/put handle "doc.other" {:v 3}) 5000 ::timeout)
                       past  (deref (kv/get handle "doc.k" {:revision rev1}) 5000 ::timeout)]
                   (is (= {:v 1} (:value past))
                       "get pinned to a past Revision decodes that Revision's value")
                   (is (= rev1 (:revision past)) "the pinned Entry carries the pinned Revision")
                   (is (= :put (:operation past)) "a live pinned Entry wears :operation :put")
                   (is (= bucket (:bucket past)) "the pinned Entry names its Bucket")
                   (is (= "doc.k" (:key past)) "the pinned Entry names its key")
                   (is (string? (:created past)) ":created is the canonical timestamp string")
                   (is (= {:v 2} (:value (deref (kv/get handle "doc.k" {:revision rev2}) 5000 ::timeout)))
                       "pinning the latest Revision reads the latest value")
                   (is (nil? (deref (kv/get handle "doc.k" {:revision other}) 5000 ::timeout))
                       "a Revision belonging to another key reads as absent")
                   (is (nil? (deref (kv/get handle "doc.k" {:revision 9999}) 5000 ::timeout))
                       "a Revision the Bucket never assigned reads as absent")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5})
                                    (p/then (fn [handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/put handle "doc.k" {:v 1})
                                                      (p/then (fn [rev1]
                                                                (-> (kv/put handle "doc.k" {:v 2})
                                                                    (p/then (fn [rev2]
                                                                              (-> (kv/put handle "doc.other" {:v 3})
                                                                                  (p/then (fn [other]
                                                                                            (-> (kv/get handle "doc.k" {:revision rev1})
                                                                                                (p/then (fn [past]
                                                                                                          (is (= {:v 1} (:value past))
                                                                                                              "get pinned to a past Revision decodes that Revision's value")
                                                                                                          (is (= rev1 (:revision past)) "the pinned Entry carries the pinned Revision")
                                                                                                          (is (= :put (:operation past)) "a live pinned Entry wears :operation :put")
                                                                                                          (is (= bucket (:bucket past)) "the pinned Entry names its Bucket")
                                                                                                          (is (= "doc.k" (:key past)) "the pinned Entry names its key")
                                                                                                          (is (string? (:created past)) ":created is the canonical timestamp string")
                                                                                                          (kv/get handle "doc.k" {:revision rev2})))
                                                                                                (p/then (fn [latest]
                                                                                                          (is (= {:v 2} (:value latest))
                                                                                                              "pinning the latest Revision reads the latest value")
                                                                                                          (kv/get handle "doc.k" {:revision other})))
                                                                                                (p/then (fn [wrong]
                                                                                                          (is (nil? wrong) "a Revision belonging to another key reads as absent")
                                                                                                          (kv/get handle "doc.k" {:revision 9999})))
                                                                                                (p/then (fn [absent]
                                                                                                          (is (nil? absent) "a Revision the Bucket never assigned reads as absent")))))))))))))))))))))))))))

;; The shared marker-pinned assertions: a get pinned to a delete/purge marker
;; Revision delivers the MARKER Entry — :operation visible, :value nil — rather
;; than hiding it behind nil, identically on both legs (pinned here because the
;; natives diverge: nats.js' revision get surfaces the marker, jnats' hides it).
(defn- assert-pinned-marker [bucket key op marker]
  (is (some? marker) "get pinned to a marker Revision delivers the marker Entry")
  (is (= op (:operation marker)) "the marker Entry's :operation is visible")
  (is (nil? (:value marker)) "the marker Entry carries :value nil")
  (is (= bucket (:bucket marker)) "the marker Entry names its Bucket")
  (is (= key (:key marker)) "the marker Entry names its key")
  (is (string? (:created marker)) ":created is the canonical timestamp string"))

;; get pinned to a marker Revision is archaeology too: the Tombstone a delete
;; wrote and the marker a purge left both deliver as marker Entries (their
;; Revision read from history, where the marker is the newest Entry).
(deftest get-pinned-to-a-marker-revision-delivers-the-marker
  (let [bucket "TRACER_KV_GET_MARKER"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (deref (kv/put handle "doc.k" {:v 1}) 5000 ::timeout)
                 (deref (kv/delete handle "doc.k") 5000 ::timeout)
                 (deref (kv/put handle "doc.gone" {:v 2}) 5000 ::timeout)
                 (deref (kv/purge handle "doc.gone") 5000 ::timeout)
                 (let [del-rev   (:revision (last (deref (kv/history handle "doc.k") 5000 ::timeout)))
                       purge-rev (:revision (last (deref (kv/history handle "doc.gone") 5000 ::timeout)))]
                   (assert-pinned-marker bucket "doc.k" :delete
                                         (deref (kv/get handle "doc.k" {:revision del-rev}) 5000 ::timeout))
                   (assert-pinned-marker bucket "doc.gone" :purge
                                         (deref (kv/get handle "doc.gone" {:revision purge-rev}) 5000 ::timeout))))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5})
                                    (p/then (fn [handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/put handle "doc.k" {:v 1})
                                                      (p/then (fn [_] (kv/delete handle "doc.k")))
                                                      (p/then (fn [_] (kv/put handle "doc.gone" {:v 2})))
                                                      (p/then (fn [_] (kv/purge handle "doc.gone")))
                                                      (p/then (fn [_] (kv/history handle "doc.k")))
                                                      (p/then (fn [entries]
                                                                (kv/get handle "doc.k" {:revision (:revision (last entries))})))
                                                      (p/then (fn [marker]
                                                                (assert-pinned-marker bucket "doc.k" :delete marker)
                                                                (kv/history handle "doc.gone")))
                                                      (p/then (fn [entries]
                                                                (kv/get handle "doc.gone" {:revision (:revision (last entries))})))
                                                      (p/then (fn [marker]
                                                                (assert-pinned-marker bucket "doc.gone" :purge marker))))))))))))))))))

;; keys enumerates a Bucket's LIVE contents: a fully-realized vector of key
;; strings — deleted and purged keys excluded — optionally restricted by a
;; subject-style filter, with no match resolving to [] (the stream-names /
;; consumer-names precedent).
(deftest keys-enumerates-live-keys
  (let [bucket "TRACER_KV_KEYS"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (deref (kv/put handle "user.alpha" {:v 1}) 5000 ::timeout)
                 (deref (kv/put handle "user.beta" {:v 2}) 5000 ::timeout)
                 (deref (kv/put handle "config.delta" {:v 3}) 5000 ::timeout)
                 (deref (kv/delete handle "user.beta") 5000 ::timeout)
                 (deref (kv/put handle "config.gone" {:v 4}) 5000 ::timeout)
                 (deref (kv/purge handle "config.gone") 5000 ::timeout)
                 (let [ks (deref (kv/keys handle) 5000 ::timeout)]
                   (is (vector? ks) "keys resolves to a fully-realized vector")
                   (is (= #{"user.alpha" "config.delta"} (set ks))
                       "keys enumerates the live keys; deleted and purged keys are excluded"))
                 (is (= ["user.alpha"] (deref (kv/keys handle "user.>") 5000 ::timeout))
                     "a subject-style filter restricts keys to the matching live keys")
                 (is (= [] (deref (kv/keys handle "missing.>") 5000 ::timeout))
                     "a filter matching nothing resolves to []"))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (-> (kv/kv conn)
                      (p/then (fn [ctx]
                                (-> (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5})
                                    (p/then (fn [handle]
                                              (with-bucket ctx bucket
                                                (fn []
                                                  (-> (kv/put handle "user.alpha" {:v 1})
                                                      (p/then (fn [_] (kv/put handle "user.beta" {:v 2})))
                                                      (p/then (fn [_] (kv/put handle "config.delta" {:v 3})))
                                                      (p/then (fn [_] (kv/delete handle "user.beta")))
                                                      (p/then (fn [_] (kv/put handle "config.gone" {:v 4})))
                                                      (p/then (fn [_] (kv/purge handle "config.gone")))
                                                      (p/then (fn [_] (kv/keys handle)))
                                                      (p/then (fn [ks]
                                                                (is (vector? ks) "keys resolves to a fully-realized vector")
                                                                (is (= #{"user.alpha" "config.delta"} (set ks))
                                                                    "keys enumerates the live keys; deleted and purged keys are excluded")
                                                                (kv/keys handle "user.>")))
                                                      (p/then (fn [ks]
                                                                (is (= ["user.alpha"] ks)
                                                                    "a subject-style filter restricts keys to the matching live keys")
                                                                (kv/keys handle "missing.>")))
                                                      (p/then (fn [ks]
                                                                (is (= [] ks) "a filter matching nothing resolves to []"))))))))))))))))))

;; ─────────────────────────────── Watch ────────────────────────────────

;; Deep-module unit (ADR 0015), no server: the watch :deliver seam — ONE closed
;; option replacing the natives' flag set, so invalid flag combinations are
;; unrepresentable, and an unrecognized value raises the validation
;; `:type :invalid-deliver` carrying the offending `:deliver`, pre-flight before
;; any native call.
(deftest deep-module-watch-deliver-validation
  (is (= :latest (bucket/validate-deliver :latest))
      ":latest (the default) passes validation unchanged")
  (is (= :history (bucket/validate-deliver :history))
      ":history passes validation unchanged")
  (is (= :updates (bucket/validate-deliver :updates))
      ":updates passes validation unchanged")
  (is (= :invalid-deliver (thrown-type #(bucket/validate-deliver :everything)))
      "a value outside the closed mode set is :invalid-deliver")
  (is (= :invalid-deliver (thrown-type #(bucket/validate-deliver "latest")))
      "a string spelling of a mode is :invalid-deliver — the option is keyword-only")
  (is (= :everything (:deliver (ex-data (try (bucket/validate-deliver :everything)
                                             (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e e)))))
      ":invalid-deliver carries the offending :deliver"))

;; The default watch (:deliver :latest): each key's CURRENT value replays first —
;; older revisions never appear — each matching Entry pushed to the Handler with
;; :value decoded through the Bucket's Codec; the handle's :initialized Promise
;; resolves once that replay completes; then new writes stream as they land. The
;; invalid-:deliver pre-flight rejection rides the same handle (deep-module seam
;; wired through the facade promise).
(deftest watch-latest-replays-then-streams
  (let [bucket "TRACER_KV_WATCH_LATEST"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (deref (kv/put handle "cfg.a" {:v 1}) 5000 ::timeout)
                 (deref (kv/put handle "cfg.a" {:v 2}) 5000 ::timeout)
                 (deref (kv/put handle "cfg.b" {:v 3}) 5000 ::timeout)
                 (let [seen (atom [])
                       w    (deref (kv/watch handle #(swap! seen conj %)) 5000 ::timeout)]
                   (try
                     (is (not= ::timeout (deref (:initialized w) 5000 ::timeout))
                         "the watch handle's :initialized Promise resolves when the replay completes")
                     (is (wait-for #(= 2 (count @seen)) 5000)
                         ":latest replays one Entry per key")
                     (is (= ["cfg.a" "cfg.b"] (mapv :key @seen))
                         "the replay covers each key, in stream order")
                     (is (= [{:v 2} {:v 3}] (mapv :value @seen))
                         "only each key's CURRENT value replays, decoded through the Bucket's Codec")
                     (is (every? #(= :put (:operation %)) @seen)
                         "replayed Entries carry the :put :operation")
                     (is (= bucket (:bucket (first @seen)))
                         "each Entry names its Bucket")
                     (is (string? (:created (first @seen)))
                         ":created is the canonical timestamp string")
                     (is (zero? (:delta (last @seen)))
                         "the Entry completing the replay carries :delta 0")
                     (deref (kv/put handle "cfg.a" {:v 4}) 5000 ::timeout)
                     (is (wait-for #(= 3 (count @seen)) 5000)
                         "after the replay, new writes stream to the Handler")
                     (is (= {:v 4} (:value (last @seen)))
                         "the streamed update carries the new decoded :value")
                     (is (= :invalid-deliver (:type (ex-data (reject-reason (kv/watch handle identity {:deliver :everything})))))
                         "an invalid :deliver value rejects the watch promise as a validation error")
                     (finally (kv/stop w)))))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (p/let [ctx    (kv/kv conn)
                          handle (kv/create-bucket ctx {:bucket bucket :storage :memory})]
                    (with-bucket ctx bucket
                      (fn []
                        (let [seen (atom [])]
                          (p/let [_    (kv/put handle "cfg.a" {:v 1})
                                  _    (kv/put handle "cfg.a" {:v 2})
                                  _    (kv/put handle "cfg.b" {:v 3})
                                  w    (kv/watch handle #(swap! seen conj %))
                                  init (p/race [(:initialized w) (p/delay 5000 ::timeout)])
                                  hit? (wait-for #(= 2 (count @seen)) 5000)]
                            (-> (p/do
                                  (is (not= ::timeout init)
                                      "the watch handle's :initialized Promise resolves when the replay completes")
                                  (is hit? ":latest replays one Entry per key")
                                  (is (= ["cfg.a" "cfg.b"] (mapv :key @seen))
                                      "the replay covers each key, in stream order")
                                  (is (= [{:v 2} {:v 3}] (mapv :value @seen))
                                      "only each key's CURRENT value replays, decoded through the Bucket's Codec")
                                  (is (every? #(= :put (:operation %)) @seen)
                                      "replayed Entries carry the :put :operation")
                                  (is (= bucket (:bucket (first @seen)))
                                      "each Entry names its Bucket")
                                  (is (string? (:created (first @seen)))
                                      ":created is the canonical timestamp string")
                                  (is (zero? (:delta (last @seen)))
                                      "the Entry completing the replay carries :delta 0")
                                  (p/let [_    (kv/put handle "cfg.a" {:v 4})
                                          hit? (wait-for #(= 3 (count @seen)) 5000)]
                                    (is hit? "after the replay, new writes stream to the Handler")
                                    (is (= {:v 4} (:value (last @seen)))
                                        "the streamed update carries the new decoded :value")
                                    (-> (kv/watch handle identity {:deliver :everything})
                                        (p/then (fn [_] (is false "expected :invalid-deliver from watch")))
                                        (p/catch (fn [e] (is (= :invalid-deliver (:type (ex-data e)))
                                                             "an invalid :deliver value rejects the watch promise as a validation error"))))))
                                (p/finally (fn [_ _] (kv/stop w)))))))))))))))

;; :deliver :history replays the key's FULL retained history oldest-to-newest —
;; including the Tombstone a delete wrote, its :operation visible (ADR 0023) —
;; before streaming new writes.
(deftest watch-history-replays-full-history
  (let [bucket "TRACER_KV_WATCH_HISTORY"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (deref (kv/put handle "doc.k" {:v 1}) 5000 ::timeout)
                 (deref (kv/put handle "doc.k" {:v 2}) 5000 ::timeout)
                 (deref (kv/delete handle "doc.k") 5000 ::timeout)
                 (let [seen (atom [])
                       w    (deref (kv/watch handle #(swap! seen conj %) {:deliver :history}) 5000 ::timeout)]
                   (try
                     (is (not= ::timeout (deref (:initialized w) 5000 ::timeout))
                         ":initialized resolves once the history replay completes")
                     (is (wait-for #(= 3 (count @seen)) 5000)
                         ":history replays every retained revision")
                     (is (= [:put :put :delete] (mapv :operation @seen))
                         "the replay arrives oldest-to-newest, the Tombstone's :operation visible")
                     (is (= [{:v 1} {:v 2} nil] (mapv :value @seen))
                         "live values decode through the Bucket's Codec; the Tombstone carries :value nil")
                     (deref (kv/put handle "doc.k" {:v 3}) 5000 ::timeout)
                     (is (wait-for #(= 4 (count @seen)) 5000)
                         "after the replay, new writes stream to the Handler")
                     (is (= {:v 3} (:value (last @seen)))
                         "the streamed update carries the new decoded :value")
                     (finally (kv/stop w)))))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (p/let [ctx    (kv/kv conn)
                          handle (kv/create-bucket ctx {:bucket bucket :storage :memory :history 5})]
                    (with-bucket ctx bucket
                      (fn []
                        (let [seen (atom [])]
                          (p/let [_    (kv/put handle "doc.k" {:v 1})
                                  _    (kv/put handle "doc.k" {:v 2})
                                  _    (kv/delete handle "doc.k")
                                  w    (kv/watch handle #(swap! seen conj %) {:deliver :history})
                                  init (p/race [(:initialized w) (p/delay 5000 ::timeout)])
                                  hit? (wait-for #(= 3 (count @seen)) 5000)]
                            (-> (p/do
                                  (is (not= ::timeout init)
                                      ":initialized resolves once the history replay completes")
                                  (is hit? ":history replays every retained revision")
                                  (is (= [:put :put :delete] (mapv :operation @seen))
                                      "the replay arrives oldest-to-newest, the Tombstone's :operation visible")
                                  (is (= [{:v 1} {:v 2} nil] (mapv :value @seen))
                                      "live values decode through the Bucket's Codec; the Tombstone carries :value nil")
                                  (p/let [_    (kv/put handle "doc.k" {:v 3})
                                          hit? (wait-for #(= 4 (count @seen)) 5000)]
                                    (is hit? "after the replay, new writes stream to the Handler")
                                    (is (= {:v 3} (:value (last @seen)))
                                        "the streamed update carries the new decoded :value")))
                                (p/finally (fn [_ _] (kv/stop w)))))))))))))))

;; :deliver :updates streams ONLY new changes: nothing replays — :initialized
;; resolves immediately, before any delivery — and a pre-existing Entry never
;; reaches the Handler (serial delivery means it would have to precede the new
;; write if it were going to arrive at all).
(deftest watch-updates-streams-only-new
  (let [bucket "TRACER_KV_WATCH_UPDATES"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (deref (kv/put handle "cfg.old" {:v 1}) 5000 ::timeout)
                 (let [seen (atom [])
                       w    (deref (kv/watch handle #(swap! seen conj %) {:deliver :updates}) 5000 ::timeout)]
                   (try
                     (is (not= ::timeout (deref (:initialized w) 5000 ::timeout))
                         ":updates has no replay, so :initialized resolves immediately")
                     (deref (kv/put handle "cfg.new" {:v 2}) 5000 ::timeout)
                     (is (wait-for #(pos? (count @seen)) 5000)
                         "a write after the watch starts is delivered")
                     (is (= ["cfg.new"] (mapv :key @seen))
                         "only the NEW change arrives — the pre-existing Entry never replays")
                     (is (= {:v 2} (:value (first @seen)))
                         "the streamed Entry decodes through the Bucket's Codec")
                     (finally (kv/stop w)))))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (p/let [ctx    (kv/kv conn)
                          handle (kv/create-bucket ctx {:bucket bucket :storage :memory})]
                    (with-bucket ctx bucket
                      (fn []
                        (let [seen (atom [])]
                          (p/let [_    (kv/put handle "cfg.old" {:v 1})
                                  w    (kv/watch handle #(swap! seen conj %) {:deliver :updates})
                                  init (p/race [(:initialized w) (p/delay 5000 ::timeout)])]
                            (-> (p/do
                                  (is (not= ::timeout init)
                                      ":updates has no replay, so :initialized resolves immediately")
                                  (p/let [_    (kv/put handle "cfg.new" {:v 2})
                                          hit? (wait-for #(pos? (count @seen)) 5000)]
                                    (is hit? "a write after the watch starts is delivered")
                                    (is (= ["cfg.new"] (mapv :key @seen))
                                        "only the NEW change arrives — the pre-existing Entry never replays")
                                    (is (= {:v 2} (:value (first @seen)))
                                        "the streamed Entry decodes through the Bucket's Codec")))
                                (p/finally (fn [_ _] (kv/stop w)))))))))))))))

;; stop ends delivery — fire-and-forget, returning nil — and is idempotent: a
;; second stop on an already-ended Watch is a safe no-op (ADR 0012 spirit). The
;; Watch opens over an EMPTY Bucket, pinning the empty-replay edge: :initialized
;; still resolves when there is nothing to replay.
(deftest watch-stop-ends-delivery-and-is-idempotent
  (let [bucket "TRACER_KV_WATCH_STOP"]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx    (deref (kv/kv conn) 5000 ::timeout)
                 handle (deref (kv/create-bucket ctx {:bucket bucket :storage :memory}) 5000 ::timeout)]
             (with-bucket ctx bucket
               (fn []
                 (let [seen (atom [])
                       w    (deref (kv/watch handle #(swap! seen conj %)) 5000 ::timeout)]
                   (is (not= ::timeout (deref (:initialized w) 5000 ::timeout))
                       "an empty Bucket's replay completes immediately — :initialized still resolves")
                   (is (nil? (kv/stop w)) "stop is fire-and-forget, returning nil")
                   (deref (kv/put handle "after.stop" {:v 1}) 5000 ::timeout)
                   (is (not (wait-for #(pos? (count @seen)) 800))
                       "stop ends delivery — a write after stop never reaches the Handler")
                   (is (nil? (kv/stop w)) "a second stop is a safe no-op")))))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (p/let [ctx    (kv/kv conn)
                          handle (kv/create-bucket ctx {:bucket bucket :storage :memory})]
                    (with-bucket ctx bucket
                      (fn []
                        (let [seen (atom [])]
                          (p/let [w    (kv/watch handle #(swap! seen conj %))
                                  init (p/race [(:initialized w) (p/delay 5000 ::timeout)])]
                            (is (not= ::timeout init)
                                "an empty Bucket's replay completes immediately — :initialized still resolves")
                            (is (nil? (kv/stop w)) "stop is fire-and-forget, returning nil")
                            (p/let [_    (kv/put handle "after.stop" {:v 1})
                                    hit? (wait-for #(pos? (count @seen)) 800)]
                              (is (not hit?)
                                  "stop ends delivery — a write after stop never reaches the Handler")
                              (is (nil? (kv/stop w)) "a second stop is a safe no-op")))))))))))))
