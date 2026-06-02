(ns nats-cljc.codec-test
  "Codec-level behaviors (ADR 0004/0011): built-in round-trips, custom codecs,
   the `:codec-error` shape, and the opt-in `:transit`/`:json` codecs. These
   exercise `nats-cljc.codec` directly — no server — so they are plain `is`
   assertions on every platform. Per-call override *through* publish/subscribe/
   request/reply is integration and lives in `core-test`."
  (:require #?(:clj  [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is]])
            [nats-cljc.codec :as codec]
            ;; cljs loads the opt-in codecs at compile time (registering their
            ;; keywords), so their round-trip tests just assert. On the JVM the
            ;; same namespaces are required at runtime *inside* their deftests, so
            ;; the unloaded-keyword path stays observable (ADR 0011).
            #?(:cljs [nats-cljc.codec.transit])
            #?(:cljs [nats-cljc.codec.json])))

;; Short aliases to codec's public UTF-8 bridge, so a custom codec in a test
;; produces honest wire bytes rather than passing an opaque object straight back
;; to decode — and the bridge stays defined in exactly one place.
(def ^:private ->bytes codec/str->bytes)
(def ^:private ->str codec/bytes->str)

(deftest custom-codec-round-trips-inline
  (let [rot (reify codec/ICodec
              (-encode [_ v] (->bytes (apply str (reverse v))))
              (-decode [_ b] (apply str (reverse (->str b)))))
        v   "tracer"]
    (is (= v (codec/decode rot (codec/encode rot v)))
        "a custom reify-ICodec instance round-trips through encode/decode")))

(deftest string-codec-round-trips
  (is (= "hello world" (codec/decode :string (codec/encode :string "hello world")))
      ":string round-trips a string verbatim")
  (is (= "42" (codec/decode :string (codec/encode :string 42)))
      ":string is lenient — a non-string value is coerced with `str` on encode"))

(deftest bytes-codec-round-trips
  ;; :bytes is a strict passthrough, so the same platform-native bytes come back
  ;; out. Compare via a UTF-8 round-trip rather than byte equality, which differs
  ;; between a JVM byte[] and a cljs Uint8Array.
  (let [b (->bytes "raw bytes")]
    (is (= "raw bytes" (->str (codec/decode :bytes (codec/encode :bytes b))))
        ":bytes passes platform-native bytes through unchanged")))

;; Call `thunk` expecting it to throw; return the thrown ex-data (nil if it did
;; not throw, which still fails the :codec-error assertions below).
(defn- codec-error-data [thunk]
  (try (thunk) nil
       (catch #?(:clj Throwable :cljs :default) e (ex-data e))))

(deftest decode-failure-surfaces-codec-error
  (let [d (codec-error-data #(codec/decode :edn (->bytes "{")))]
    (is (= :codec-error (:type d)) "a decode failure normalizes to :codec-error")
    (is (= :edn (:codec d)) "ex-data names the codec that failed")
    (is (= :decode (:op d)) "ex-data names the failing direction")))

(deftest bytes-codec-rejects-non-bytes
  (let [d (codec-error-data #(codec/encode :bytes "not platform bytes"))]
    (is (= :codec-error (:type d)) ":bytes rejects a non-byte value with :codec-error")
    (is (= :bytes (:codec d)) "ex-data names the :bytes codec")
    (is (= :encode (:op d)) "the :bytes guard fires during encode, so the wrapper stamps :op :encode")))

(deftest unknown-codec-keyword-surfaces-codec-error
  (let [d (codec-error-data #(codec/encode :no-such-codec "x"))]
    (is (= :codec-error (:type d)) "an unresolved codec keyword is a :codec-error")
    (is (= :no-such-codec (:codec d)) "ex-data names the offending keyword")
    (is (nil? (:op d)) "a resolution miss is not tagged as an encode/decode failure")))

(deftest instance-codec-error-names-a-stable-id
  ;; A custom codec whose -encode throws. The failure must name the instance by a
  ;; stable keyword (:custom) — never the live reify object or its class-hash —
  ;; so :codec keeps the keyword shape consumers see for the built-ins.
  (let [boom (reify codec/ICodec
               (-encode [_ _] (throw (ex-info "boom" {})))
               (-decode [_ b] b))
        d    (codec-error-data #(codec/encode boom "x"))]
    (is (= :codec-error (:type d)) "an instance encode failure normalizes to :codec-error")
    (is (= :encode (:op d)) "ex-data names the failing direction")
    (is (= :custom (:codec d)) "an instance codec is named by the stable :custom id, not the live object")))

(deftest nested-codec-error-carries-outer-op
  ;; A custom codec whose -decode internally calls codec/decode with an unknown
  ;; keyword — that inner failure is itself a :codec-error with no :op. The OUTER
  ;; decode must re-stamp it with the outer op/codec, not leak the inner miss, so
  ;; a caller matching on :op/:codec sees the operation it actually invoked.
  (let [nested (reify codec/ICodec
                 (-encode [_ v] (->bytes (pr-str v)))
                 (-decode [_ _] (codec/decode :no-such-codec (->bytes "x"))))
        d      (codec-error-data #(codec/decode nested (->bytes "anything")))]
    (is (= :codec-error (:type d)) "a nested codec failure still normalizes to :codec-error")
    (is (= :decode (:op d)) "the OUTER op is surfaced, not the inner resolution miss's nil")
    (is (= :custom (:codec d)) "the OUTER instance codec is named, not the inner :no-such-codec")
    (is (nil? (:require d)) "the inner miss's :require does not leak past the outer boundary")))

;; A Clojure-faithful value: keywords, a set, nested colls — transit preserves
;; all of it (unlike the lossy :json codec below).
(def ^:private rich-value {:a 1 :b [2 3] :s #{:x :y} :k :kw :str "hi"})

(deftest transit-codec-round-trips
  ;; JVM: require at runtime to register :transit — idempotent, a no-op once the
  ;; ns is loaded, so a second run-tests in the same JVM is fine. cljs loaded it
  ;; at compile time via the ns form. The unloaded-keyword path is asserted
  ;; separately below against a registry snapshot, so it doesn't hinge on whether
  ;; a prior run already required :transit into the process-global registry.
  #?(:clj (require 'nats-cljc.codec.transit))
  (is (= rich-value (codec/decode :transit (codec/encode :transit rich-value)))
      ":transit round-trips Clojure-faithfully once required"))

;; A JSON-faithful value: string keys keywordize back, values are JSON scalars
;; and vectors. :json is lossy (no keyword values, sets, or rich types), so the
;; round-trip value stays within what JSON preserves.
(def ^:private json-value {:a 1 :b "two" :c [3 4]})

(deftest json-codec-round-trips
  #?(:clj (require 'nats-cljc.codec.json))
  (is (= json-value (codec/decode :json (codec/encode :json json-value)))
      ":json round-trips a JSON-faithful value (keys keywordized)"))

;; :json large integers diverge across legs (ADR 0004): the JVM reader keeps an
;; exact Long, js/JSON.parse coerces to f64 and rounds beyond 2^53. Pin each leg's
;; documented behavior against the *same wire bytes* a JVM producer would emit (a
;; cljs literal can't even hold 2^53+1), so a regression on either side is visible.
(deftest json-large-integer-precision-is-platform-specific
  #?(:clj (require 'nats-cljc.codec.json))
  (let [wire    (->bytes "{\"id\":9007199254740993}")
        decoded (:id (codec/decode :json wire))]
    #?(:clj  (is (= 9007199254740993 decoded)
                 "JVM data.json reads JSON integers as exact Long")
       :cljs (is (= 9007199254740992 decoded)
                 "cljs js/JSON.parse coerces to f64, rounding beyond 2^53 (documented lossiness)"))))

;; The actionable "not loaded -> (require '<ns>)" hint is a property of
;; resolve-codec for *known* opt-in keywords, independent of platform. Snapshot
;; the registry without the opt-in codecs so the unloaded path is observable no
;; matter what a prior run-tests already required globally (the round-trip tests
;; above register :transit/:json process-wide and never unregister). This is the
;; idempotent replacement for the old in-test unloaded assertions.
(deftest unloaded-opt-in-keyword-points-at-require
  (with-redefs [codec/registry (atom (apply dissoc @codec/registry [:transit :json]))]
    (let [d (codec-error-data #(codec/encode :transit {:a 1}))]
      (is (= :codec-error (:type d)) "an unloaded opt-in keyword is a :codec-error")
      (is (= 'nats-cljc.codec.transit (:require d))
          "ex-data points at the :transit namespace to require"))
    (let [d (codec-error-data #(codec/encode :json {:a 1}))]
      (is (= 'nats-cljc.codec.json (:require d))
          "ex-data points at the :json namespace to require"))))
