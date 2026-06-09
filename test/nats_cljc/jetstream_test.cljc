(ns nats-cljc.jetstream-test
  "Portable JetStream suite (ADR 0017): one `.cljc` source run on the JVM and Node
   (browser is CI-only, ADR 0010). Mirrors `core-test`'s connect/teardown envelope.
   The facade is aliased `jet` rather than `js`, which CLJS reserves for host
   interop."
  (:require #?(:clj  [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is async]])
            [nats-cljc.core :as nats]
            [nats-cljc.codec :as codec]
            [nats-cljc.codec.json]
            [nats-cljc.jetstream :as jet]
            [nats-cljc.jetstream.error :as jet-err]
            [nats-cljc.jetstream.stream :as stream]
            [nats-cljc.jetstream.consumer :as consumer]
            [nats-cljc.jetstream.pub :as pub]
            [nats-cljc.jetstream.pull :as pull]
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
  (is (= :wrong-last-sequence (jet-err/api-error-type 10071))
      "err_code 10071 (wrong last sequence) normalizes to :wrong-last-sequence")
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

;; Stream teardown guard: run `f`, then ALWAYS delete `stream` — even when an
;; assertion derefs to ::timeout or a promise rejects mid-body — so a memory
;; stream can't leak into a re-run. JVM deletes in a `finally`; CLJS awaits the
;; delete through `p/handle` (NOT `p/finally`, which ignores the teardown promise)
;; and re-raises the body's error after cleanup. Teardown is best-effort: a delete
;; failure is swallowed so it can't mask the body's outcome. Create the stream
;; BEFORE the guard so a failed create never deletes a stream that isn't there.
#?(:clj
   (defn- with-stream [ctx stream f]
     (try (f)
          (finally (try (deref (jet/delete-stream ctx stream) 5000 ::timeout)
                        (catch Throwable _ nil)))))
   :cljs
   (defn- with-stream [ctx stream f]
     (p/handle (p/do (f))
               (fn [v e]
                 (p/handle (jet/delete-stream ctx stream)
                           (fn [_ _] (if e (throw e) v)))))))

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

;; A canonical, fully-specified portable config — every supported key set — so the
;; kebab->native->kebab round-trip is exact and the create/info round-trip below has
;; something to compare against. :memory so a skipped teardown vanishes on restart.
(def ^:private a-config
  {:name "TRACER" :subjects ["tracer.>"] :storage :memory :retention :work-queue :max-age-ms 60000})

;; A loose ISO-8601 matcher: a date, a `T`, and a time. The JVM formats its
;; ZonedDateTime to this; nats.js hands back the server's RFC3339 string already in
;; it — the cross-leg point is that `:created` is a normalized timestamp string,
;; never a native ZonedDateTime/Date (ADR 0020).
(def ^:private iso-re #"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*")

;; The strict canonical `:timestamp` shape both legs emit (ADR 0019 pure-data parity):
;; UTC, exactly three fractional digits, ending in `Z` — e.g. 2026-06-09T01:08:17.279Z.
;; Tighter than `iso-re` on purpose: it rejects the JVM leg's old variable-digit,
;; source-offset formatting, so a divergence there can't ship green over the live path.
(def ^:private canonical-ts-re #"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")

;; Capture the validation `:type` a synchronous deep-module guard throws, portably.
(defn- thrown-type [thunk]
  (try (thunk) nil
       (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e (:type (ex-data e)))))

;; AC5, deep-module unit (ADR 0015/0020), no server: building the native config is a
;; pure local construction, so the kebab<->native round-trip needs no JetStream — a
;; fully-specified config round-trips to itself, proving the enum keywords and the
;; ms-duration survive each leg's translation. The closed-key + name guards are the
;; portable pre-flight half, raising their validation `:type`s before any native call.
(deftest deep-module-config-round-trip-and-validation
  (is (= a-config #?(:clj  (jet-jvm/stream-config->map (jet-jvm/->stream-config a-config))
                     :cljs (jet-js/stream-config->map (jet-js/->stream-config a-config))))
      "a fully-specified config round-trips kebab->native->kebab on this leg")
  (is (= a-config (stream/validate-config a-config))
      "a recognized config passes validation unchanged")
  (is (= :unknown-config-key (thrown-type #(stream/validate-config {:name "OK" :bogus 1})))
      "an unrecognized key (the map is closed) is :unknown-config-key")
  (is (= :invalid-name (thrown-type #(stream/validate-name "bad.name")))
      "a name carrying a subject delimiter is :invalid-name")
  (is (= "OK" (stream/validate-name "OK"))
      "a well-formed name passes validation unchanged"))

;; A canonical, fully-specified portable consumer config — every supported key set —
;; so the kebab->native->kebab round-trip is exact. `:durable? true` is the explicit
;; durable selector (ADR 0021).
(def ^:private a-consumer-config
  {:name "TRACER_C" :durable? true :ack-policy :explicit :deliver-policy :all
   :ack-wait-ms 30000 :max-deliver 5 :filter-subjects ["tracer.a" "tracer.b"]})

;; AC4, deep-module unit (ADR 0015/0020), no server: building the native consumer
;; config is a pure local construction, so the kebab<->native round-trip needs no
;; JetStream — a fully-specified config round-trips to itself, proving the ack/deliver
;; policy keywords and the ms-duration survive each leg's translation (a Duration on
;; the JVM, Nanos on CLJS). The closed-key + name guards are the portable pre-flight
;; half, raising their validation `:type`s before any native call.
(deftest deep-module-consumer-config-round-trip-and-validation
  (is (= a-consumer-config #?(:clj  (jet-jvm/consumer-config->map (jet-jvm/->consumer-config a-consumer-config))
                              :cljs (jet-js/consumer-config->map (jet-js/->consumer-config a-consumer-config))))
      "a fully-specified durable consumer config round-trips kebab->native->kebab on this leg")
  (let [ephemeral (assoc a-consumer-config :durable? false)]
    (is (= ephemeral #?(:clj  (jet-jvm/consumer-config->map (jet-jvm/->consumer-config ephemeral))
                        :cljs (jet-js/consumer-config->map (jet-js/->consumer-config ephemeral))))
        "a named ephemeral (:durable? false) round-trips, setting name but no durable field"))
  (is (= a-consumer-config (consumer/validate-config a-consumer-config))
      "a recognized consumer config passes validation unchanged")
  (is (= :unknown-config-key (thrown-type #(consumer/validate-config {:name "OK" :bogus 1})))
      "an unrecognized key (the map is closed) is :unknown-config-key")
  (is (= :missing-required-key (thrown-type #(consumer/validate-config {:ack-policy :explicit})))
      "a durable config omitting :name is :missing-required-key, not :invalid-name {:name nil}")
  (is (= {:durable? false :ack-policy :explicit} (consumer/validate-config {:durable? false :ack-policy :explicit}))
      "an ephemeral (:durable? false) may omit :name — the server assigns one")
  (is (= :invalid-name (thrown-type #(consumer/validate-name "bad.name")))
      "a name carrying a subject delimiter is :invalid-name")
  (is (= "OK" (consumer/validate-name "OK"))
      "a well-formed consumer name passes validation unchanged"))

;; Deep-module unit (ADR 0019), no server: lifting a delivered message's
;; metadata timestamp is a pure normalization, so feeding each leg's
;; `->canonical-timestamp` the SAME instant — carried at sub-millisecond precision, which
;; must truncate (`.279999999` → `.279`, never round to `.280`) — must yield one
;; byte-identical canonical string. This is the cross-leg parity check the live
;; integration path can't make (the server picks the instant); it locks the format
;; against the JVM leg's old ISO_OFFSET_DATE_TIME (variable digits, source offset).
(deftest deep-module-canonical-timestamp
  (is (= "2026-06-09T01:08:17.279Z"
         #?(:clj  (#'jet-jvm/->canonical-timestamp
                   (java.time.ZonedDateTime/parse "2026-06-09T01:08:17.279999999Z"))
            :cljs (#'jet-js/->canonical-timestamp "2026-06-09T01:08:17.279999999Z")))
      "a delivered :timestamp normalizes to UTC, exactly three fractional digits, truncated to millis"))

;; AC4, deep-module unit (ADR 0015/0020), no server: the reserved-header guard is the
;; portable pre-flight that keeps the Nats-* namespace sanctioned-only — :msg-id and
;; :expect are the way to set those, so a reserved key set directly in user :headers
;; is caller misuse, raised as :reserved-header before any native call. The check is
;; case-insensitive (the wire names are Title-Case but the namespace is what's
;; reserved), and a map free of reserved keys passes through unchanged.
(deftest deep-module-reserved-header-guard
  (is (= :reserved-header (thrown-type #(pub/validate-headers {"Nats-Msg-Id" "x"})))
      "a reserved Nats-* header is :reserved-header")
  (is (= :reserved-header (thrown-type #(pub/validate-headers {"nats-expected-last-sequence" "1"})))
      "the reserved-prefix check is case-insensitive")
  (is (= ["Nats-Msg-Id"]
         (:keys (ex-data (try (pub/validate-headers {"Nats-Msg-Id" "x" "ok" "y"})
                              (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e e)))))
      ":reserved-header carries the offending :keys")
  (let [clean {"My-Header" ["a" "b"] "X-Trace" "1"}]
    (is (= clean (pub/validate-headers clean))
        "a header map free of reserved keys passes validation unchanged"))
  (is (nil? (pub/validate-headers nil))
      "nil headers pass through — there are none to guard")
  (is (= :invalid-header (thrown-type #(pub/validate-headers ["Nats-Msg-Id" "x"])))
      "a non-map :headers (e.g. a vector) is :invalid-header, not a raw ClassCastException"))

;; Deep-module unit (ADR 0015), no server: the pull `:expires-ms` guard is the portable
;; pre-flight that keeps a sub-floor or non-integer poll window from reaching either leg
;; as an un-normalized native throw (jnats' IllegalArgumentException, nats.js'
;; InvalidArgumentError — both enforce a 1000ms floor). A below-floor value is caller
;; misuse, raised as :invalid-expires before any native call; an omitted or well-formed
;; window passes through unchanged.
(deftest deep-module-expires-guard
  (is (= :invalid-expires (thrown-type #(pull/validate-expires {:expires-ms 500})))
      "a sub-1000ms :expires-ms is :invalid-expires")
  (is (= :invalid-expires (thrown-type #(pull/validate-expires {:expires-ms 0})))
      "a zero :expires-ms is below the floor — :invalid-expires, not a native default")
  (is (= :invalid-expires (thrown-type #(pull/validate-expires {:expires-ms "2000"})))
      "a non-integer :expires-ms is :invalid-expires, not a raw native type error")
  (is (= 500 (:expires-ms (ex-data (try (pull/validate-expires {:expires-ms 500})
                                        (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e e)))))
      ":invalid-expires carries the offending :expires-ms")
  (let [ok {:expires-ms 1000 :batch 3}]
    (is (= ok (pull/validate-expires ok))
        "an at-floor :expires-ms passes validation unchanged"))
  (is (= {:batch 3} (pull/validate-expires {:batch 3}))
      "an omitted :expires-ms passes through to the native default"))

;; AC1/AC3/AC4, deep-module unit (ADR 0020), no server: building the native publish
;; options is a pure local construction, so the portable opts -> native round-trip
;; needs no JetStream. :msg-id, :timeout-ms, and all four :expect fields survive each
;; leg's translation (a Duration on the JVM, a plain ms number on CLJS), proving the
;; per-leg option builder the impl hands the native publish.
(deftest deep-module-publish-options-round-trip
  (let [opts {:msg-id "id-1" :timeout-ms 3500
              :expect {:last-seq 7 :last-msg-id "m-9" :stream "S" :last-subject-seq 3}}]
    #?(:clj
       (let [o (jet-jvm/->publish-options opts)]
         (is (= "id-1" (.getMessageId o)) ":msg-id -> messageId")
         (is (= 7 (.getExpectedLastSequence o)) ":last-seq -> expectedLastSequence")
         (is (= "m-9" (.getExpectedLastMsgId o)) ":last-msg-id -> expectedLastMsgId")
         (is (= "S" (.getExpectedStream o)) ":stream -> expectedStream")
         (is (= 3 (.getExpectedLastSubjectSequence o)) ":last-subject-seq -> expectedLastSubjectSequence"))
       :cljs
       (let [o ^js (jet-js/->publish-options opts)
             e ^js (.-expect o)]
         (is (= "id-1" (.-msgID o)) ":msg-id -> msgID")
         (is (= 3500 (.-timeout o)) ":timeout-ms -> timeout ms")
         (is (= 7 (.-lastSequence e)) ":last-seq -> lastSequence")
         (is (= "m-9" (.-lastMsgID e)) ":last-msg-id -> lastMsgID")
         (is (= "S" (.-streamName e)) ":stream -> streamName")
         (is (= 3 (.-lastSubjectSequence e)) ":last-subject-seq -> lastSubjectSequence")))))

;; Deep-module unit (ADR 0020), no server: empty opts must build no native options
;; object at all — the builder/`clj->js {}` allocation is skipped on the hot path and the
;; native client takes its own default. Both legs return nil so the publish call passes a
;; null/undefined options argument. Drop the `(seq opts)` short-circuit and this goes red.
(deftest deep-module-publish-options-empty-skips-allocation
  (let [->opts #?(:clj jet-jvm/->publish-options :cljs jet-js/->publish-options)]
    (is (nil? (->opts {})) "empty opts -> nil (no native options object)")
    (is (nil? (->opts nil)) "nil opts -> nil (no native options object)")))

;; JVM only: the ack deadline is enforced by `bound-ack` bounding the returned
;; future, NOT by jnats reading PublishOptions.streamTimeout (the publish path ignores it,
;; falling back to the ~5s request-cleanup interval). A never-completing future stands in
;; for "no ack arrives"; `bound-ack` must reject NEAR the requested deadline — well under
;; that 5s fallback that would mask a no-op — with a bare `:timeout` ex-info. The lower
;; bound proves it actually waited for the deadline rather than rejecting instantly; drop
;; the `.orTimeout` from `bound-ack` and this goes red (never rejects / rejects at 5s).
#?(:clj
   (deftest jvm-publish-ack-deadline-honored
     (let [fut   (java.util.concurrent.CompletableFuture.)
           start (System/nanoTime)
           e     (reject-reason (#'jet-jvm/bound-ack fut 100))
           ms    (/ (- (System/nanoTime) start) 1e6)]
       (is (= :timeout (:type (ex-data e))) "bounded ack deadline rejects with a bare :timeout ex-info")
       (is (< 50 ms 2000) (str "rejected near the 100ms deadline, not instantly nor at the 5s fallback; was " ms "ms")))))

;; JVM only: a non-API, non-timeout publish failure — a 503/no-stream or a
;; connection drop, jnats' IOException — must reach the caller as a BARE ex-info
;; carrying a `:type`, like the JetStreamApiException path (ADR 0006). A future
;; completed exceptionally with that IOException stands in for the failed publishAsync;
;; pushed through the real `bound-ack` + deliver-bare seam, the leaf must surface typed,
;; not as the raw IOException whose `(ex-data e)` is nil. Leave `publish-ex->ex-info`'s
;; `:else` returning the raw exception and this goes red (`:type` nil).
#?(:clj
   (deftest jvm-publish-non-api-failure-is-typed
     (let [fut (doto (java.util.concurrent.CompletableFuture.)
                 (.completeExceptionally (java.io.IOException. "503 no responders available for request")))
           e   (reject-reason (#'jet-jvm/bound-ack fut nil))]
       (is (= :publish-failed (:type (ex-data e)))
           "a non-API publish failure rejects with a bare :publish-failed ex-info")
       (is (instance? java.io.IOException (.getCause ^Throwable e))
           "the typed ex-info carries the unwrapped IOException leaf as its cause"))))

;; CLJS only, no server: a non-JetStreamApiError publish rejection — a nats.js
;; `TimeoutError` / no-responders, a raw object whose `(ex-data e)` is nil — must
;; normalize to a bare ex-info carrying a `:type`, like the JetStreamApiError path
;; (ADR 0006). `publish-error` is the publish leg's normalizer (`-js-publish`'s
;; `.catch`); a non-API error routes to the operational catch-all `:publish-failed`,
;; the same keyword the JVM leg's `publish-ex->ex-info` uses. Leave it passing the raw
;; object through and this goes red (`:type` nil).
#?(:cljs
   (deftest cljs-publish-non-api-failure-is-typed
     (let [e (jet-js/publish-error #js {:name "TimeoutError" :message "timeout"})]
       (is (= :publish-failed (:type (ex-data e)))
           "a non-API publish failure normalizes to a bare :publish-failed ex-info"))))

;; AC3 (ADR 0015), no server: a validation failure surfaces through create-stream's
;; OWN channel — the returned promise REJECTS, it does not throw synchronously — and
;; pre-flight, before any native call. A nil ctx proves the pre-flight: validation
;; rejects first, so the native -create-stream never dereferences it.
(deftest create-stream-rejects-validation-on-its-promise
  #?(:clj
     (do
       (is (= :unknown-config-key
              (:type (ex-data (reject-reason (jet/create-stream nil {:name "OK" :bogus 1})))))
           "an unknown key rejects the create-stream promise pre-flight")
       (is (= :invalid-name
              (:type (ex-data (reject-reason (jet/create-stream nil {:name "bad.name" :subjects ["x.>"]})))))
           "a malformed name rejects the create-stream promise pre-flight"))
     :cljs
     (async done
            (-> (jet/create-stream nil {:name "OK" :bogus 1})
                (p/then (fn [_] (is false "expected an :unknown-config-key rejection")))
                (p/catch (fn [e] (is (= :unknown-config-key (:type (ex-data e)))
                                     "an unknown key rejects the create-stream promise pre-flight")))
                (p/then (fn [_] (jet/create-stream nil {:name "bad.name" :subjects ["x.>"]})))
                (p/then (fn [_] (is false "expected an :invalid-name rejection")))
                (p/catch (fn [e] (is (= :invalid-name (:type (ex-data e)))
                                     "a malformed name rejects the create-stream promise pre-flight")))
                (p/finally (fn [_ _] (done)))))))

;; AC4 (ADR 0015), no server: a validation failure surfaces through create-consumer's
;; OWN channel — the returned promise REJECTS, it does not throw synchronously — and
;; pre-flight, before any native call. A nil ctx proves the pre-flight: validation
;; rejects first, so the native -create-consumer never dereferences it. Both the closed
;; config map and BOTH name positions (the stream arg and the config's durable :name)
;; are guarded.
(deftest create-consumer-rejects-validation-on-its-promise
  #?(:clj
     (do
       (is (= :unknown-config-key
              (:type (ex-data (reject-reason (jet/create-consumer nil "OK" {:name "C" :bogus 1})))))
           "an unknown key rejects the create-consumer promise pre-flight")
       (is (= :missing-required-key
              (:type (ex-data (reject-reason (jet/create-consumer nil "OK" {:ack-policy :explicit})))))
           "a durable config omitting :name rejects the create-consumer promise pre-flight")
       (is (= :invalid-name
              (:type (ex-data (reject-reason (jet/create-consumer nil "OK" {:name "bad.name"})))))
           "a malformed durable name rejects the create-consumer promise pre-flight")
       (is (= :invalid-name
              (:type (ex-data (reject-reason (jet/create-consumer nil "bad.stream" {:name "C"})))))
           "a malformed stream name rejects the create-consumer promise pre-flight"))
     :cljs
     (async done
            (-> (jet/create-consumer nil "OK" {:name "C" :bogus 1})
                (p/then (fn [_] (is false "expected an :unknown-config-key rejection")))
                (p/catch (fn [e] (is (= :unknown-config-key (:type (ex-data e)))
                                     "an unknown key rejects the create-consumer promise pre-flight")))
                (p/then (fn [_] (jet/create-consumer nil "OK" {:ack-policy :explicit})))
                (p/then (fn [_] (is false "expected a :missing-required-key rejection")))
                (p/catch (fn [e] (is (= :missing-required-key (:type (ex-data e)))
                                     "a durable config omitting :name rejects the create-consumer promise pre-flight")))
                (p/then (fn [_] (jet/create-consumer nil "OK" {:name "bad.name"})))
                (p/then (fn [_] (is false "expected an :invalid-name rejection")))
                (p/catch (fn [e] (is (= :invalid-name (:type (ex-data e)))
                                     "a malformed durable name rejects the create-consumer promise pre-flight")))
                (p/then (fn [_] (jet/create-consumer nil "bad.stream" {:name "C"})))
                (p/then (fn [_] (is false "expected an :invalid-name rejection")))
                (p/catch (fn [e] (is (= :invalid-name (:type (ex-data e)))
                                     "a malformed stream name rejects the create-consumer promise pre-flight")))
                (p/finally (fn [_ _] (done)))))))

;; (ADR 0015), no server: a sub-floor `:expires-ms` surfaces through fetch's
;; and next's OWN channel — the returned promise REJECTS, it does not throw synchronously
;; — pre-flight, before any native call, and as the SAME typed :invalid-expires on both
;; legs (where the raw clients diverge: a jnats IllegalArgumentException vs a nats.js
;; InvalidArgumentError). A nil ctx proves the pre-flight: validation rejects first, so the
;; native -js-fetch/-js-next never dereferences it.
(deftest pull-rejects-invalid-expires-on-its-promise
  #?(:clj
     (do
       (is (= :invalid-expires
              (:type (ex-data (reject-reason (jet/fetch nil "PULL" "PULL_C" {:expires-ms 500})))))
           "a sub-floor :expires-ms rejects the fetch promise pre-flight")
       (is (= :invalid-expires
              (:type (ex-data (reject-reason (jet/next nil "PULL" "PULL_C" {:expires-ms 500})))))
           "a sub-floor :expires-ms rejects the next promise pre-flight"))
     :cljs
     (async done
            (-> (jet/fetch nil "PULL" "PULL_C" {:expires-ms 500})
                (p/then (fn [_] (is false "expected an :invalid-expires rejection")))
                (p/catch (fn [e] (is (= :invalid-expires (:type (ex-data e)))
                                     "a sub-floor :expires-ms rejects the fetch promise pre-flight")))
                (p/then (fn [_] (jet/next nil "PULL" "PULL_C" {:expires-ms 500})))
                (p/then (fn [_] (is false "expected an :invalid-expires rejection")))
                (p/catch (fn [e] (is (= :invalid-expires (:type (ex-data e)))
                                     "a sub-floor :expires-ms rejects the next promise pre-flight")))
                (p/finally (fn [_ _] (done)))))))

;; AC4 (ADR 0015), no server: the reserved-header guard surfaces through publish's OWN
;; channel — the returned promise REJECTS, it does not throw synchronously — and
;; pre-flight, before any native call. A nil ctx proves the pre-flight: the guard
;; rejects first, so the native -js-publish never dereferences it.
(deftest publish-rejects-reserved-header-on-its-promise
  #?(:clj
     (is (= :reserved-header
            (:type (ex-data (reject-reason (jet/publish nil "tracer.1" {:a 1} {:headers {"Nats-Msg-Id" "x"}})))))
         "a reserved Nats-* user header rejects the publish promise pre-flight")
     :cljs
     (async done
            (-> (jet/publish nil "tracer.1" {:a 1} {:headers {"Nats-Msg-Id" "x"}})
                (p/then (fn [_] (is false "expected a :reserved-header rejection")))
                (p/catch (fn [e] (is (= :reserved-header (:type (ex-data e)))
                                     "a reserved Nats-* user header rejects the publish promise pre-flight")))
                (p/finally (fn [_ _] (done)))))))

;; ADR 0015, no server: publish's :invalid-header surface, through its OWN
;; channel — the returned promise REJECTS, it does not throw synchronously — and pre-flight,
;; before any native call (nil ctx). Two sources, both typed :invalid-header: a non-map
;; :headers (e.g. a vector) caught by validate-headers' type guard instead of leaking a raw
;; ClassCastException from `keys`, and a malformed header (non-ASCII value here) caught by
;; core/normalize-headers in the then stage.
(deftest publish-rejects-invalid-header-on-its-promise
  #?(:clj
     (do
       (is (= :invalid-header
              (:type (ex-data (reject-reason (jet/publish nil "tracer.1" {:a 1} {:headers ["x" "y"]})))))
           "a non-map :headers rejects the publish promise pre-flight")
       (is (= :invalid-header
              (:type (ex-data (reject-reason (jet/publish nil "tracer.1" {:a 1} {:headers {"X-Trace" "café"}})))))
           "a non-ASCII header value rejects the publish promise pre-flight"))
     :cljs
     (async done
            (-> (jet/publish nil "tracer.1" {:a 1} {:headers ["x" "y"]})
                (p/then (fn [_] (is false "expected an :invalid-header rejection")))
                (p/catch (fn [e] (is (= :invalid-header (:type (ex-data e)))
                                     "a non-map :headers rejects the publish promise pre-flight")))
                (p/then (fn [_] (jet/publish nil "tracer.1" {:a 1} {:headers {"X-Trace" "café"}})))
                (p/then (fn [_] (is false "expected an :invalid-header rejection")))
                (p/catch (fn [e] (is (= :invalid-header (:type (ex-data e)))
                                     "a non-ASCII header value rejects the publish promise pre-flight")))
                (p/finally (fn [_ _] (done)))))))

;; AC1 + AC2 (ADR 0017/0020), integration on the JetStream-enabled :4222 server:
;; create a Stream from the portable config and read its normalized info back (config
;; round-trips, :created is an ISO-8601 string, a fresh stream has no messages), then
;; delete it and confirm a subsequent info surfaces the operational :stream-not-found.
(deftest stream-create-info-delete
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx     (deref (jet/jetstream conn) 5000 ::timeout)
               created (deref (jet/create-stream ctx a-config) 5000 ::timeout)
               info    (deref (jet/stream-info ctx "TRACER") 5000 ::timeout)]
           (is (= a-config (:config created)) "create-stream returns the normalized config")
           (is (= a-config (:config info)) "stream-info round-trips the normalized config")
           (is (re-matches iso-re (:created info)) "stream-info :created is an ISO-8601 string")
           (is (= 0 (get-in info [:state :messages])) "a freshly created stream has no messages")
           (is (nil? (deref (jet/delete-stream ctx "TRACER") 5000 ::timeout)) "delete-stream resolves nil")
           (is (= :stream-not-found (:type (ex-data (reject-reason (jet/stream-info ctx "TRACER")))))
               "stream-info after delete surfaces :stream-not-found"))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx     (jet/jetstream conn)
                        created (jet/create-stream ctx a-config)
                        info    (jet/stream-info ctx "TRACER")]
                  (is (= a-config (:config created)) "create-stream returns the normalized config")
                  (is (= a-config (:config info)) "stream-info round-trips the normalized config")
                  (is (re-matches iso-re (:created info)) "stream-info :created is an ISO-8601 string")
                  (is (= 0 (get-in info [:state :messages])) "a freshly created stream has no messages")
                  (p/let [del (jet/delete-stream ctx "TRACER")]
                    (is (nil? del) "delete-stream resolves nil")
                    (-> (jet/stream-info ctx "TRACER")
                        (p/then (fn [_] (is false "expected :stream-not-found after delete")))
                        (p/catch (fn [e] (is (= :stream-not-found (:type (ex-data e)))
                                             "stream-info after delete surfaces :stream-not-found")))))))))))

;; A stream the consumer integration tests hang their durable consumers off. Its
;; subjects cover the consumer config's two filter subjects. :memory so a skipped
;; teardown vanishes on restart.
(def ^:private a-stream-for-consumers
  {:name "CTRACER" :subjects ["tracer.>"] :storage :memory})

;; AC1 + AC2 (ADR 0020), integration on the JetStream-enabled :4222 server: create a
;; durable Consumer on a Stream from the portable config and read its normalized info
;; back — the config round-trips, :created is an ISO-8601 string, and a fresh consumer
;; has delivered/ack-floor cursors at zero with nothing pending. Same on both legs;
;; creates its own stream fixture and cleans up.
(deftest consumer-create-info
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx     (deref (jet/jetstream conn) 5000 ::timeout)
               _       (deref (jet/create-stream ctx a-stream-for-consumers) 5000 ::timeout)
               created (deref (jet/create-consumer ctx "CTRACER" a-consumer-config) 5000 ::timeout)
               info    (deref (jet/consumer-info ctx "CTRACER" "TRACER_C") 5000 ::timeout)]
           (is (= "CTRACER" (:stream created)) "create-consumer returns the owning stream")
           (is (= "TRACER_C" (:name created)) "create-consumer returns the durable name")
           (is (= a-consumer-config (:config created)) "create-consumer returns the normalized config")
           (is (= a-consumer-config (:config info)) "consumer-info round-trips the normalized config")
           (is (re-matches iso-re (:created info)) "consumer-info :created is an ISO-8601 string")
           (is (= 0 (get-in info [:delivered :stream-seq])) "a fresh consumer has delivered nothing")
           (is (= 0 (get-in info [:ack-floor :stream-seq])) "a fresh consumer's ack floor is zero")
           (is (= 0 (:pending info)) "a fresh consumer has nothing pending")
           (deref (jet/delete-stream ctx "CTRACER") 5000 ::timeout))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx     (jet/jetstream conn)
                        _       (jet/create-stream ctx a-stream-for-consumers)
                        created (jet/create-consumer ctx "CTRACER" a-consumer-config)
                        info    (jet/consumer-info ctx "CTRACER" "TRACER_C")]
                  (is (= "CTRACER" (:stream created)) "create-consumer returns the owning stream")
                  (is (= "TRACER_C" (:name created)) "create-consumer returns the durable name")
                  (is (= a-consumer-config (:config created)) "create-consumer returns the normalized config")
                  (is (= a-consumer-config (:config info)) "consumer-info round-trips the normalized config")
                  (is (re-matches iso-re (:created info)) "consumer-info :created is an ISO-8601 string")
                  (is (= 0 (get-in info [:delivered :stream-seq])) "a fresh consumer has delivered nothing")
                  (is (= 0 (get-in info [:ack-floor :stream-seq])) "a fresh consumer's ack floor is zero")
                  (is (= 0 (:pending info)) "a fresh consumer has nothing pending")
                  (jet/delete-stream ctx "CTRACER")))))))

;; AC3 (ADR 0020), integration: delete-consumer resolves nil once the durable is gone,
;; and a subsequent consumer-info surfaces the operational :consumer-not-found (err_code
;; 10014, normalized via the shared table) — the consumer analog of :stream-not-found.
;; Same on both legs; creates its own stream + consumer fixture and cleans up the stream.
(deftest consumer-delete-then-not-found
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (jet/jetstream conn) 5000 ::timeout)]
           (deref (jet/create-stream ctx a-stream-for-consumers) 5000 ::timeout)
           (deref (jet/create-consumer ctx "CTRACER" a-consumer-config) 5000 ::timeout)
           (is (nil? (deref (jet/delete-consumer ctx "CTRACER" "TRACER_C") 5000 ::timeout)) "delete-consumer resolves nil")
           (is (= :consumer-not-found (:type (ex-data (reject-reason (jet/consumer-info ctx "CTRACER" "TRACER_C")))))
               "consumer-info after delete surfaces :consumer-not-found")
           (deref (jet/delete-stream ctx "CTRACER") 5000 ::timeout))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx (jet/jetstream conn)
                        _   (jet/create-stream ctx a-stream-for-consumers)
                        _   (jet/create-consumer ctx "CTRACER" a-consumer-config)
                        del (jet/delete-consumer ctx "CTRACER" "TRACER_C")]
                  (is (nil? del) "delete-consumer resolves nil")
                  (-> (jet/consumer-info ctx "CTRACER" "TRACER_C")
                      (p/then (fn [_] (is false "expected :consumer-not-found after delete")))
                      (p/catch (fn [e] (is (= :consumer-not-found (:type (ex-data e)))
                                           "consumer-info after delete surfaces :consumer-not-found")))
                      (p/then (fn [_] (jet/delete-stream ctx "CTRACER"))))))))))

;; AC2 (ADR 0020), integration: list-consumers enumerates a Stream's durables as
;; normalized info maps, and consumer-names is its name projection (the facade derives
;; it, since nats.js has no names endpoint). Two durables off one stream prove the
;; enumeration; both legs return the same set of names. Creates its own fixture, cleans up.
(deftest consumer-list-and-names
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (jet/jetstream conn) 5000 ::timeout)]
           (deref (jet/create-stream ctx a-stream-for-consumers) 5000 ::timeout)
           (deref (jet/create-consumer ctx "CTRACER" (assoc a-consumer-config :name "C_ONE")) 5000 ::timeout)
           (deref (jet/create-consumer ctx "CTRACER" (assoc a-consumer-config :name "C_TWO")) 5000 ::timeout)
           (let [infos (deref (jet/list-consumers ctx "CTRACER") 5000 ::timeout)
                 names (deref (jet/consumer-names ctx "CTRACER") 5000 ::timeout)]
             (is (= 2 (count infos)) "list-consumers enumerates both durables")
             (is (= #{"C_ONE" "C_TWO"} (set (map :name infos))) "each enumerated info carries its durable name")
             (is (= #{"C_ONE" "C_TWO"} (set names)) "consumer-names projects the durable names"))
           (deref (jet/delete-stream ctx "CTRACER") 5000 ::timeout))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx   (jet/jetstream conn)
                        _     (jet/create-stream ctx a-stream-for-consumers)
                        _     (jet/create-consumer ctx "CTRACER" (assoc a-consumer-config :name "C_ONE"))
                        _     (jet/create-consumer ctx "CTRACER" (assoc a-consumer-config :name "C_TWO"))
                        infos (jet/list-consumers ctx "CTRACER")
                        names (jet/consumer-names ctx "CTRACER")]
                  (is (= 2 (count infos)) "list-consumers enumerates both durables")
                  (is (= #{"C_ONE" "C_TWO"} (set (map :name infos))) "each enumerated info carries its durable name")
                  (is (= #{"C_ONE" "C_TWO"} (set names)) "consumer-names projects the durable names")
                  (jet/delete-stream ctx "CTRACER")))))))

;; AC (ADR 0021), integration: a single-element :filter-subjects round-trips — the case the
;; two-subject configs elsewhere dodge. On every server version this project supports
;; (nats-server >= 2.12, where the plural `filter_subjects` field exists), both jnats and
;; nats.js return a single filter under the plural field, so the plural read round-trips it
;; natively. (Verified: probing the raw native config shows the plural populated on both
;; legs; pre-2.12 singular-only storage is out of scope, so no coalesce is carried.)
(deftest consumer-single-filter-round-trip
  (let [solo (assoc a-consumer-config :name "C_SOLO" :filter-subjects ["tracer.a"])]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx     (deref (jet/jetstream conn) 5000 ::timeout)
                 _       (deref (jet/create-stream ctx a-stream-for-consumers) 5000 ::timeout)
                 created (deref (jet/create-consumer ctx "CTRACER" solo) 5000 ::timeout)]
             (is (= ["tracer.a"] (:filter-subjects (:config created)))
                 "a single :filter-subjects round-trips (singular/plural coalesced)")
             (deref (jet/delete-stream ctx "CTRACER") 5000 ::timeout))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (p/let [ctx     (jet/jetstream conn)
                          _       (jet/create-stream ctx a-stream-for-consumers)
                          created (jet/create-consumer ctx "CTRACER" solo)]
                    (is (= ["tracer.a"] (:filter-subjects (:config created)))
                        "a single :filter-subjects round-trips (singular/plural coalesced)")
                    (jet/delete-stream ctx "CTRACER"))))))))

;; AC (ADR 0021), integration: create-consumer builds an EPHEMERAL with :durable? false.
;; The create response is the server-applied config; a named ephemeral reads its name back
;; and reports :durable? false (no durable_name field), distinguishing it from a durable.
;; Asserting on the create return is race-free against the ephemeral's inactivity reaping.
(deftest consumer-ephemeral-create
  (let [eph (assoc a-consumer-config :name "C_EPH" :durable? false)]
    #?(:clj
       (with-conn {:servers [server-url]}
         (fn [conn]
           (let [ctx     (deref (jet/jetstream conn) 5000 ::timeout)
                 _       (deref (jet/create-stream ctx a-stream-for-consumers) 5000 ::timeout)
                 created (deref (jet/create-consumer ctx "CTRACER" eph) 5000 ::timeout)]
             (is (= "C_EPH" (:name created)) "a named ephemeral reads its name back")
             (is (false? (:durable? (:config created))) "an ephemeral reports :durable? false")
             (deref (jet/delete-stream ctx "CTRACER") 5000 ::timeout))))
       :cljs
       (async done
              (with-conn {:servers [server-url]} done
                (fn [conn]
                  (p/let [ctx     (jet/jetstream conn)
                          _       (jet/create-stream ctx a-stream-for-consumers)
                          created (jet/create-consumer ctx "CTRACER" eph)]
                    (is (= "C_EPH" (:name created)) "a named ephemeral reads its name back")
                    (is (false? (:durable? (:config created))) "an ephemeral reports :durable? false")
                    (jet/delete-stream ctx "CTRACER"))))))))

;; AC (ADR 0021), integration: create-consumer is CREATE-ONLY on both legs — re-creating a
;; durable with a CHANGED config is rejected by the server (action=create), identically on
;; both legs, rather than silently upserting (the old JVM .addOrUpdateConsumer behavior).
;; The unmapped "already exists" code defaults to the catch-all :jetstream-api-error. An
;; identical re-create would be idempotent, so the config must differ (:max-deliver here).
(deftest consumer-create-is-create-only
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (jet/jetstream conn) 5000 ::timeout)]
           (deref (jet/create-stream ctx a-stream-for-consumers) 5000 ::timeout)
           (deref (jet/create-consumer ctx "CTRACER" a-consumer-config) 5000 ::timeout)
           (is (= :jetstream-api-error
                  (:type (ex-data (reject-reason (jet/create-consumer ctx "CTRACER" (assoc a-consumer-config :max-deliver 9))))))
               "a config-changing re-create rejects (create-only, not upsert)")
           (deref (jet/delete-stream ctx "CTRACER") 5000 ::timeout))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx (jet/jetstream conn)
                        _   (jet/create-stream ctx a-stream-for-consumers)
                        _   (jet/create-consumer ctx "CTRACER" a-consumer-config)]
                  (-> (jet/create-consumer ctx "CTRACER" (assoc a-consumer-config :max-deliver 9))
                      (p/then (fn [_] (is false "expected a config-changing re-create to reject")))
                      (p/catch (fn [e] (is (= :jetstream-api-error (:type (ex-data e)))
                                           "a config-changing re-create rejects (create-only, not upsert)")))
                      (p/then (fn [_] (jet/delete-stream ctx "CTRACER"))))))))))

;; AC4 (ADR 0020), integration: a config the SERVER rejects (a subject overlap with an
;; existing stream — valid name, valid keys, so not pre-flight validation) is
;; detected AFTER the native call and so is operational :jetstream-api-error, carrying
;; the {:code :description} the server returned. Cleans up the first stream after.
(deftest server-rejected-config-is-jetstream-api-error
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (jet/jetstream conn) 5000 ::timeout)]
           (deref (jet/create-stream ctx {:name "OVL_A" :subjects ["shared.>"] :storage :memory}) 5000 ::timeout)
           (let [e (reject-reason (jet/create-stream ctx {:name "OVL_B" :subjects ["shared.>"] :storage :memory}))]
             (is (= :jetstream-api-error (:type (ex-data e))) "a server-rejected config is operational :jetstream-api-error")
             (is (number? (:code (ex-data e))) ":jetstream-api-error carries the server's :code")
             (is (string? (:description (ex-data e))) ":jetstream-api-error carries the server's :description"))
           (deref (jet/delete-stream ctx "OVL_A") 5000 ::timeout))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx (jet/jetstream conn)
                        _   (jet/create-stream ctx {:name "OVL_A" :subjects ["shared.>"] :storage :memory})]
                  (-> (jet/create-stream ctx {:name "OVL_B" :subjects ["shared.>"] :storage :memory})
                      (p/then (fn [_] (is false "expected a :jetstream-api-error rejection")))
                      (p/catch (fn [e]
                                 (is (= :jetstream-api-error (:type (ex-data e))) "a server-rejected config is operational :jetstream-api-error")
                                 (is (number? (:code (ex-data e))) ":jetstream-api-error carries the server's :code")
                                 (is (string? (:description (ex-data e))) ":jetstream-api-error carries the server's :description")))
                      (p/then (fn [_] (jet/delete-stream ctx "OVL_A"))))))))))

;; AC1 (ADR 0017/0020), integration on the JetStream-enabled :4222 server: an acked
;; publish into a Stream resolves to the normalized PubAck {:stream :seq :duplicate
;; :domain}, identically on both legs. A fresh memory stream so the first publish is
;; deterministically sequence 1, not a duplicate, with no domain configured. Cleans up.
(deftest publish-resolves-to-puback
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (jet/jetstream conn) 5000 ::timeout)]
           (deref (jet/create-stream ctx {:name "PUBACK" :subjects ["puback.>"] :storage :memory}) 5000 ::timeout)
           (let [ack (deref (jet/publish ctx "puback.a" {:n 1}) 5000 ::timeout)]
             (is (= "PUBACK" (:stream ack)) "PubAck :stream is the stream that stored it")
             (is (= 1 (:seq ack)) "the first publish lands at stream sequence 1")
             (is (false? (:duplicate ack)) "a first publish is not a duplicate")
             (is (nil? (:domain ack)) "no JetStream domain configured ⇒ :domain nil"))
           (deref (jet/delete-stream ctx "PUBACK") 5000 ::timeout))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx (jet/jetstream conn)
                        _   (jet/create-stream ctx {:name "PUBACK" :subjects ["puback.>"] :storage :memory})
                        ack (jet/publish ctx "puback.a" {:n 1})]
                  (is (= "PUBACK" (:stream ack)) "PubAck :stream is the stream that stored it")
                  (is (= 1 (:seq ack)) "the first publish lands at stream sequence 1")
                  (is (false? (:duplicate ack)) "a first publish is not a duplicate")
                  (is (nil? (:domain ack)) "no JetStream domain configured ⇒ :domain nil")
                  (jet/delete-stream ctx "PUBACK")))))))

;; AC2 (ADR 0020), integration: re-publishing with the same :msg-id within the
;; stream's dedup window is recognized server-side as a duplicate — the second PubAck
;; carries :duplicate true and the SAME sequence as the first, so a publish retry is
;; idempotent rather than double-storing. Same on both legs; cleans up.
(deftest publish-msg-id-dedup
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (jet/jetstream conn) 5000 ::timeout)]
           (deref (jet/create-stream ctx {:name "DEDUP" :subjects ["dedup.>"] :storage :memory}) 5000 ::timeout)
           (let [a1 (deref (jet/publish ctx "dedup.a" {:n 1} {:msg-id "id-1"}) 5000 ::timeout)
                 a2 (deref (jet/publish ctx "dedup.a" {:n 1} {:msg-id "id-1"}) 5000 ::timeout)]
             (is (false? (:duplicate a1)) "the first publish with a :msg-id is not a duplicate")
             (is (true? (:duplicate a2)) "re-publishing the same :msg-id within the window is a duplicate")
             (is (= (:seq a1) (:seq a2)) "the duplicate ack carries the original sequence"))
           (deref (jet/delete-stream ctx "DEDUP") 5000 ::timeout))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx (jet/jetstream conn)
                        _   (jet/create-stream ctx {:name "DEDUP" :subjects ["dedup.>"] :storage :memory})
                        a1  (jet/publish ctx "dedup.a" {:n 1} {:msg-id "id-1"})
                        a2  (jet/publish ctx "dedup.a" {:n 1} {:msg-id "id-1"})]
                  (is (false? (:duplicate a1)) "the first publish with a :msg-id is not a duplicate")
                  (is (true? (:duplicate a2)) "re-publishing the same :msg-id within the window is a duplicate")
                  (is (= (:seq a1) (:seq a2)) "the duplicate ack carries the original sequence")
                  (jet/delete-stream ctx "DEDUP")))))))

;; AC3 (ADR 0020), integration: an :expect optimistic-concurrency assertion the server
;; rejects (a :last-seq that does not match the stream's actual last sequence) surfaces
;; through publish's promise as the operational :type :wrong-last-sequence (err_code
;; 10071, normalized via the shared table) — not a raw native exception. On the JVM the
;; failure arrives wrapped (CompletionException > RuntimeException > JetStreamApiException)
;; and must be unwrapped; on CLJS nats.js rejects with a JetStreamApiError directly.
(deftest publish-expect-wrong-last-seq
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (jet/jetstream conn) 5000 ::timeout)]
           (deref (jet/create-stream ctx {:name "EXPECT" :subjects ["expect.>"] :storage :memory}) 5000 ::timeout)
           (deref (jet/publish ctx "expect.a" {:n 1}) 5000 ::timeout)
           (let [e (reject-reason (jet/publish ctx "expect.a" {:n 2} {:expect {:last-seq 999}}))]
             (is (= :wrong-last-sequence (:type (ex-data e)))
                 "a wrong :last-seq expectation rejects with operational :wrong-last-sequence"))
           (deref (jet/delete-stream ctx "EXPECT") 5000 ::timeout))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx (jet/jetstream conn)
                        _   (jet/create-stream ctx {:name "EXPECT" :subjects ["expect.>"] :storage :memory})
                        _   (jet/publish ctx "expect.a" {:n 1})]
                  (-> (jet/publish ctx "expect.a" {:n 2} {:expect {:last-seq 999}})
                      (p/then (fn [_] (is false "expected a :wrong-last-sequence rejection")))
                      (p/catch (fn [e] (is (= :wrong-last-sequence (:type (ex-data e)))
                                           "a wrong :last-seq expectation rejects with operational :wrong-last-sequence")))
                      (p/then (fn [_] (jet/delete-stream ctx "EXPECT"))))))))))

;; A `:bytes` subscriber observes the exact wire bytes a publisher produced; UTF-8
;; decode them (codec's public bridge) so the codec-override assertion reads as text.
(def ^:private raw->str codec/bytes->str)

;; AC4 (ADR 0011/0020), integration: a per-call :codec override and non-reserved user
;; :headers both take effect on the published message, observed by a plain core
;; subscriber on the stream subject (a JS-published message is an ordinary publish the
;; stream also captures). The :json override is proven by the wire being JSON text —
;; never the connection's default EDN — and :headers by the user header riding through
;; alongside the sanctioned Nats-Msg-Id that :msg-id set. Same on both legs; cleans up.
(deftest publish-codec-override-and-header-passthrough
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx      (deref (jet/jetstream conn) 5000 ::timeout)
               received (promise)]
           (deref (jet/create-stream ctx {:name "CODEC" :subjects ["codec.>"] :storage :memory}) 5000 ::timeout)
           (nats/subscribe conn "codec.a" #(deliver received %) {:codec :bytes})
           (deref (jet/publish ctx "codec.a" {:n 1}
                               {:codec :json :headers {"My-Header" "trace-1"} :msg-id "mid-1"})
                  5000 ::timeout)
           (let [msg (deref received 5000 ::timeout)]
             (is (not= ::timeout msg) "a core subscriber observes the JS-published message")
             (is (= "{\"n\":1}" (raw->str (:data msg)))
                 "the :codec :json override encodes the wire as JSON, not the default EDN")
             (is (= ["trace-1"] (get (:headers msg) "My-Header"))
                 "a non-reserved user header rides through onto the published message")
             (is (= ["mid-1"] (get (:headers msg) "Nats-Msg-Id"))
                 ":msg-id is delivered as the sanctioned reserved Nats-Msg-Id header"))
           (deref (jet/delete-stream ctx "CODEC") 5000 ::timeout))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx (jet/jetstream conn)
                        _   (jet/create-stream ctx {:name "CODEC" :subjects ["codec.>"] :storage :memory})]
                  (let [received (p/deferred)]
                    (nats/subscribe conn "codec.a" #(p/resolve! received %) {:codec :bytes})
                    (p/let [_ (jet/publish ctx "codec.a" {:n 1}
                                           {:codec :json :headers {"My-Header" "trace-1"} :msg-id "mid-1"})
                            msg (p/timeout received 5000 ::timeout)]
                      (is (not= ::timeout msg) "a core subscriber observes the JS-published message")
                      (is (= "{\"n\":1}" (raw->str (:data msg)))
                          "the :codec :json override encodes the wire as JSON, not the default EDN")
                      (is (= ["trace-1"] (get (:headers msg) "My-Header"))
                          "a non-reserved user header rides through onto the published message")
                      (is (= ["mid-1"] (get (:headers msg) "Nats-Msg-Id"))
                          ":msg-id is delivered as the sanctioned reserved Nats-Msg-Id header")
                      (jet/delete-stream ctx "CODEC")))))))))

;; A durable pull consumer over a single subject, explicit-ack so messages stay pending
;; until acked — the deterministic delivery set the pull tracer reads. :deliver-policy
;; defaults to :all (every stored message), filtered to the one publish subject.
(def ^:private a-pull-consumer
  {:name "PULL_C" :durable? true :ack-policy :explicit :filter-subjects ["pull.a"]})

;; AC1 + AC3 (ADR 0018/0019), integration on the :4222 JetStream server: populate a
;; stream with three acked publishes, then `fetch` up to :batch from a durable consumer.
;; The promise resolves to a vector of up-to-N PURE-DATA messages {:subject :data :js}:
;; :data is decoded with the context codec in stream order, and there is NO native
;; object on the map. The :js metadata is lifted from each leg's native accessor —
;; stream/consumer, the stream + delivery sequences, the delivery count (⇒ :redelivered
;; false on a first delivery), pending, an ISO-8601 :timestamp, nil :domain, and the
;; captured $JS.ACK ack-subject. Same on both legs; memory stream, cleans up.
(deftest pull-fetch-delivers-pure-data
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (jet/jetstream conn) 5000 ::timeout)]
           (deref (jet/create-stream ctx {:name "PULL" :subjects ["pull.>"] :storage :memory}) 5000 ::timeout)
           (with-stream ctx "PULL"
             (fn []
               (deref (jet/create-consumer ctx "PULL" a-pull-consumer) 5000 ::timeout)
               (doseq [n [1 2 3]] (deref (jet/publish ctx "pull.a" {:n n}) 5000 ::timeout))
               (let [msgs (deref (jet/fetch ctx "PULL" "PULL_C" {:batch 3 :expires-ms 2000}) 5000 ::timeout)
                     m1   (first msgs)
                     js   (:js m1)]
                 (is (= 3 (count msgs)) "fetch resolves a vector of up to :batch messages")
                 (is (= [{:n 1} {:n 2} {:n 3}] (map :data msgs)) "each :data is decoded with the context codec, in stream order")
                 (is (= ["pull.a" "pull.a" "pull.a"] (map :subject msgs)) "each message carries its publish subject")
                 (is (= #{:subject :data :js} (set (keys m1))) "a delivered message is pure data (no native object, no :headers when none set)")
                 (is (= "PULL" (:stream js)) ":js carries the source stream")
                 (is (= "PULL_C" (:consumer js)) ":js carries the consumer")
                 (is (= 1 (:stream-seq js)) ":js stream-seq is the message's stream sequence")
                 (is (= 1 (:delivery-seq js)) ":js delivery-seq is the per-consumer delivery sequence")
                 (is (= 1 (:delivered js)) "a first delivery has delivered count 1")
                 (is (false? (:redelivered js)) "delivered once ⇒ not redelivered")
                 (is (= 2 (:pending js)) "two messages remain pending behind the first")
                 (is (re-matches canonical-ts-re (:timestamp js)) ":js timestamp is canonical UTC millis (e.g. 2026-06-09T01:08:17.279Z)")
                 (is (nil? (:domain js)) "no JetStream domain configured ⇒ :js :domain nil")
                 (is (boolean (re-find #"^\$JS\.ACK\." (:ack-subject js))) ":js ack-subject is the captured $JS.ACK reply subject")))))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx (jet/jetstream conn)
                        _   (jet/create-stream ctx {:name "PULL" :subjects ["pull.>"] :storage :memory})]
                  (with-stream ctx "PULL"
                    (fn []
                      (p/let [_   (jet/create-consumer ctx "PULL" a-pull-consumer)
                              _   (jet/publish ctx "pull.a" {:n 1})
                              _   (jet/publish ctx "pull.a" {:n 2})
                              _   (jet/publish ctx "pull.a" {:n 3})
                              msgs (jet/fetch ctx "PULL" "PULL_C" {:batch 3 :expires-ms 2000})]
                        (let [m1 (first msgs)
                              js (:js m1)]
                          (is (= 3 (count msgs)) "fetch resolves a vector of up to :batch messages")
                          (is (= [{:n 1} {:n 2} {:n 3}] (map :data msgs)) "each :data is decoded with the context codec, in stream order")
                          (is (= ["pull.a" "pull.a" "pull.a"] (map :subject msgs)) "each message carries its publish subject")
                          (is (= #{:subject :data :js} (set (keys m1))) "a delivered message is pure data (no native object, no :headers when none set)")
                          (is (= "PULL" (:stream js)) ":js carries the source stream")
                          (is (= "PULL_C" (:consumer js)) ":js carries the consumer")
                          (is (= 1 (:stream-seq js)) ":js stream-seq is the message's stream sequence")
                          (is (= 1 (:delivery-seq js)) ":js delivery-seq is the per-consumer delivery sequence")
                          (is (= 1 (:delivered js)) "a first delivery has delivered count 1")
                          (is (false? (:redelivered js)) "delivered once ⇒ not redelivered")
                          (is (= 2 (:pending js)) "two messages remain pending behind the first")
                          (is (re-matches canonical-ts-re (:timestamp js)) ":js timestamp is canonical UTC millis (e.g. 2026-06-09T01:08:17.279Z)")
                          (is (nil? (:domain js)) "no JetStream domain configured ⇒ :js :domain nil")
                          (is (boolean (re-find #"^\$JS\.ACK\." (:ack-subject js))) ":js ack-subject is the captured $JS.ACK reply subject")))))))))))

;; AC2 (ADR 0018), integration on the :4222 JetStream server: `next` polls one message.
;; With a single message stored, the first `next` resolves that PURE-DATA message (its
;; :data decoded, :js metadata lifted); a second `next` against the now-drained consumer
;; resolves NIL once its `:expires-ms` window elapses with nothing in flight (the first
;; message is unacked but within ack-wait, so it is not yet redelivered). Same on both
;; legs; memory stream, cleans up.
(deftest pull-next-delivers-one-or-nil
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (jet/jetstream conn) 5000 ::timeout)]
           (deref (jet/create-stream ctx {:name "PULLN" :subjects ["pulln.>"] :storage :memory}) 5000 ::timeout)
           (with-stream ctx "PULLN"
             (fn []
               (deref (jet/create-consumer ctx "PULLN" {:name "PULLN_C" :durable? true :ack-policy :explicit :filter-subjects ["pulln.a"]}) 5000 ::timeout)
               (deref (jet/publish ctx "pulln.a" {:n 1}) 5000 ::timeout)
               (let [m (deref (jet/next ctx "PULLN" "PULLN_C" {:expires-ms 2000}) 5000 ::timeout)]
                 (is (= {:n 1} (:data m)) "next resolves the stored message, :data decoded")
                 (is (= "pulln.a" (:subject m)) "next's message carries its publish subject")
                 (is (= 1 (get-in m [:js :stream-seq])) "next's message carries its :js stream-seq")
                 (let [empty? (deref (jet/next ctx "PULLN" "PULLN_C" {:expires-ms 1000}) 5000 ::timeout)]
                   (is (nil? empty?) "next against a drained consumer resolves nil"))))))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx (jet/jetstream conn)
                        _   (jet/create-stream ctx {:name "PULLN" :subjects ["pulln.>"] :storage :memory})]
                  (with-stream ctx "PULLN"
                    (fn []
                      (p/let [_   (jet/create-consumer ctx "PULLN" {:name "PULLN_C" :durable? true :ack-policy :explicit :filter-subjects ["pulln.a"]})
                              _   (jet/publish ctx "pulln.a" {:n 1})
                              m   (jet/next ctx "PULLN" "PULLN_C" {:expires-ms 2000})
                              empty? (jet/next ctx "PULLN" "PULLN_C" {:expires-ms 1000})]
                        (is (= {:n 1} (:data m)) "next resolves the stored message, :data decoded")
                        (is (= "pulln.a" (:subject m)) "next's message carries its publish subject")
                        (is (= 1 (get-in m [:js :stream-seq])) "next's message carries its :js stream-seq")
                        (is (nil? empty?) "next against a drained consumer resolves nil"))))))))))

;; ADR 0019, integration on the :4222 JetStream server: the :redelivered (delivered > 1)
;; true branch. A durable consumer with a short `:ack-wait-ms` polls one message and leaves
;; it UNACKED — delivered 1, not redelivered. Once ack-wait elapses the server requeues it,
;; so a second `next` (its window spanning that ack-wait) resolves the SAME message with
;; delivered 2 and :redelivered true, exercising the branch both legs derive from the
;; delivery count. Memory stream, cleans up.
(deftest pull-redelivers-unacked-message
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (jet/jetstream conn) 5000 ::timeout)]
           (deref (jet/create-stream ctx {:name "REDELIV" :subjects ["rd.>"] :storage :memory}) 5000 ::timeout)
           (with-stream ctx "REDELIV"
             (fn []
               (deref (jet/create-consumer ctx "REDELIV" {:name "RD_C" :durable? true :ack-policy :explicit :ack-wait-ms 1000 :filter-subjects ["rd.a"]}) 5000 ::timeout)
               (deref (jet/publish ctx "rd.a" {:n 1}) 5000 ::timeout)
               (let [m1 (deref (jet/next ctx "REDELIV" "RD_C" {:expires-ms 2000}) 5000 ::timeout)]
                 (is (= 1 (get-in m1 [:js :delivered])) "the first delivery has delivered count 1")
                 (is (false? (get-in m1 [:js :redelivered])) "delivered once ⇒ not redelivered"))
               ;; m1 is left unacked: after :ack-wait-ms the server redelivers, so this next —
               ;; waiting past that window — catches the same message on its second delivery.
               (let [m2 (deref (jet/next ctx "REDELIV" "RD_C" {:expires-ms 5000}) 8000 ::timeout)]
                 (is (= {:n 1} (:data m2)) "the redelivered message is the same one, :data decoded")
                 (is (= 2 (get-in m2 [:js :delivered])) "a redelivery has delivered count 2")
                 (is (true? (get-in m2 [:js :redelivered])) "delivered more than once ⇒ redelivered true")))))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx (jet/jetstream conn)
                        _   (jet/create-stream ctx {:name "REDELIV" :subjects ["rd.>"] :storage :memory})]
                  (with-stream ctx "REDELIV"
                    (fn []
                      (p/let [_   (jet/create-consumer ctx "REDELIV" {:name "RD_C" :durable? true :ack-policy :explicit :ack-wait-ms 1000 :filter-subjects ["rd.a"]})
                              _   (jet/publish ctx "rd.a" {:n 1})
                              m1  (jet/next ctx "REDELIV" "RD_C" {:expires-ms 2000})
                              m2  (jet/next ctx "REDELIV" "RD_C" {:expires-ms 5000})]
                        (is (= 1 (get-in m1 [:js :delivered])) "the first delivery has delivered count 1")
                        (is (false? (get-in m1 [:js :redelivered])) "delivered once ⇒ not redelivered")
                        (is (= {:n 1} (:data m2)) "the redelivered message is the same one, :data decoded")
                        (is (= 2 (get-in m2 [:js :delivered])) "a redelivery has delivered count 2")
                        (is (true? (get-in m2 [:js :redelivered])) "delivered more than once ⇒ redelivered true"))))))))))

;; ADR 0018, integration on the :4222 JetStream server: with `:expires-ms` omitted, both
;; pull verbs fall through to each leg's 30000ms native default (jnats FetchConsumeOptions
;; / nats.js fetch — verified equal across the legs). With the batch already satisfiable
;; neither blocks that window: `fetch` settles as soon as :batch is filled and `next` as
;; soon as a message is in flight, so each resolves inside the 5s deref — far under the 30s
;; default, proving the omitted path delivers without waiting the full window. A separate
;; durable consumer feeds each verb from the stream start. Memory stream, cleans up.
(deftest pull-omitted-expires-delivers-without-blocking
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (jet/jetstream conn) 5000 ::timeout)]
           (deref (jet/create-stream ctx {:name "PULLO" :subjects ["pullo.>"] :storage :memory}) 5000 ::timeout)
           (with-stream ctx "PULLO"
             (fn []
               (deref (jet/create-consumer ctx "PULLO" {:name "PULLO_F" :durable? true :ack-policy :explicit :filter-subjects ["pullo.a"]}) 5000 ::timeout)
               (deref (jet/create-consumer ctx "PULLO" {:name "PULLO_N" :durable? true :ack-policy :explicit :filter-subjects ["pullo.a"]}) 5000 ::timeout)
               (doseq [n [1 2 3]] (deref (jet/publish ctx "pullo.a" {:n n}) 5000 ::timeout))
               (let [msgs (deref (jet/fetch ctx "PULLO" "PULLO_F" {:batch 3}) 5000 ::timeout)]
                 (is (= [{:n 1} {:n 2} {:n 3}] (map :data msgs))
                     "fetch with :expires-ms omitted resolves the full batch inside 5s, not the 30s default"))
               (let [m (deref (jet/next ctx "PULLO" "PULLO_N") 5000 ::timeout)]
                 (is (= {:n 1} (:data m))
                     "next with :expires-ms omitted resolves the stored message inside 5s, not the 30s default")))))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx (jet/jetstream conn)
                        _   (jet/create-stream ctx {:name "PULLO" :subjects ["pullo.>"] :storage :memory})]
                  (with-stream ctx "PULLO"
                    (fn []
                      (p/let [_   (jet/create-consumer ctx "PULLO" {:name "PULLO_F" :durable? true :ack-policy :explicit :filter-subjects ["pullo.a"]})
                              _   (jet/create-consumer ctx "PULLO" {:name "PULLO_N" :durable? true :ack-policy :explicit :filter-subjects ["pullo.a"]})
                              _   (jet/publish ctx "pullo.a" {:n 1})
                              _   (jet/publish ctx "pullo.a" {:n 2})
                              _   (jet/publish ctx "pullo.a" {:n 3})
                              msgs (jet/fetch ctx "PULLO" "PULLO_F" {:batch 3})
                              m    (jet/next ctx "PULLO" "PULLO_N")]
                        (is (= [{:n 1} {:n 2} {:n 3}] (map :data msgs))
                            "fetch with :expires-ms omitted resolves the full batch promptly, not the 30s default")
                        (is (= {:n 1} (:data m))
                            "next with :expires-ms omitted resolves the stored message promptly, not the 30s default"))))))))))

;; ADR 0020, integration on the :4222 JetStream server: next and fetch against a
;; consumer that does not exist reject with the operational :consumer-not-found
;; (err_code 10014, normalized via the shared table) — the pull-path analog of
;; consumer-info's not-found. The two legs reach it by different routes: jnats'
;; getConsumerContext raises a JetStreamApiException, while nats.js' consumers.get
;; rejects with a ConsumerNotFoundError (a JetStreamApiError subclass) — both carry
;; 10014, so both surface the same :type. A stream exists but the named consumer was
;; never created; cleans up the stream.
(deftest pull-next-fetch-missing-consumer
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [ctx (deref (jet/jetstream conn) 5000 ::timeout)]
           (deref (jet/create-stream ctx {:name "PULLNF" :subjects ["pullnf.>"] :storage :memory}) 5000 ::timeout)
           (with-stream ctx "PULLNF"
             (fn []
               (is (= :consumer-not-found (:type (ex-data (reject-reason (jet/next ctx "PULLNF" "MISSING_C" {:expires-ms 1000})))))
                   "next against a missing consumer rejects with :consumer-not-found")
               (is (= :consumer-not-found (:type (ex-data (reject-reason (jet/fetch ctx "PULLNF" "MISSING_C" {:batch 1 :expires-ms 1000})))))
                   "fetch against a missing consumer rejects with :consumer-not-found"))))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (p/let [ctx (jet/jetstream conn)
                        _   (jet/create-stream ctx {:name "PULLNF" :subjects ["pullnf.>"] :storage :memory})]
                  (with-stream ctx "PULLNF"
                    (fn []
                      (-> (jet/next ctx "PULLNF" "MISSING_C" {:expires-ms 1000})
                          (p/then (fn [_] (is false "expected :consumer-not-found from next")))
                          (p/catch (fn [e] (is (= :consumer-not-found (:type (ex-data e)))
                                               "next against a missing consumer rejects with :consumer-not-found")))
                          (p/then (fn [_] (jet/fetch ctx "PULLNF" "MISSING_C" {:batch 1 :expires-ms 1000})))
                          (p/then (fn [_] (is false "expected :consumer-not-found from fetch")))
                          (p/catch (fn [e] (is (= :consumer-not-found (:type (ex-data e)))
                                               "fetch against a missing consumer rejects with :consumer-not-found"))))))))))))

;; CLJS only, no server: a per-message lift throwing mid-batch must release the
;; ConsumerMessages, not leave the pull subscription/inbox dangling until the
;; expires/heartbeat window — the CLJS analog of the JVM fetch's `(finally (.close fc))`.
;; A fake ConsumerMessages yields a delivery whose lift throws (an `info` getter that
;; raises, the way `js-msg->raw` reads DeliveryInfo); `drain-fetch` must reject with that
;; error AND have called `.close` on the iterable. Drop the throw-path close and `@closed?`
;; stays false — the leak this guards against.
#?(:cljs
   (deftest cljs-drain-fetch-closes-on-lift-throw
     (async done
            (let [closed?  (atom false)
                  boom     (js/Error. "lift boom")
                  msg      (js/Object.defineProperty
                            #js {} "info" #js {:get (fn [] (throw boom))})
                  iterator #js {:next (fn [] (js/Promise.resolve #js {:done false :value msg}))}
                  cm       #js {:close (fn [] (reset! closed? true) (js/Promise.resolve))}]
              (unchecked-set cm (.-asyncIterator js/Symbol) (fn [] iterator))
              (-> (jet-js/drain-fetch cm)
                  (p/then (fn [_] (is false "a lift throwing mid-batch must reject, not resolve")))
                  (p/catch (fn [e]
                             (is (identical? boom e) "drain-fetch rejects with the lift's error")
                             (is @closed? "drain-fetch closes the ConsumerMessages on the throw path")))
                  (p/finally (fn [_ _] (done))))))))
