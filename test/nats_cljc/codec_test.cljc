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

;; Test-local UTF-8 byte helpers — the codec's own are private. Mirror its
;; reader-conditional shape so a custom codec in a test produces honest wire
;; bytes rather than passing an opaque object straight back to decode.
(defn- ->bytes [s]
  #?(:clj  (.getBytes ^String s java.nio.charset.StandardCharsets/UTF_8)
     :cljs (.encode (js/TextEncoder.) s)))

(defn- ->str [b]
  #?(:clj  (String. ^bytes b java.nio.charset.StandardCharsets/UTF_8)
     :cljs (.decode (js/TextDecoder.) b)))

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
    (is (= :bytes (:codec d)) "ex-data names the :bytes codec")))

(deftest unknown-codec-keyword-surfaces-codec-error
  (let [d (codec-error-data #(codec/encode :no-such-codec "x"))]
    (is (= :codec-error (:type d)) "an unresolved codec keyword is a :codec-error")
    (is (= :no-such-codec (:codec d)) "ex-data names the offending keyword")
    (is (nil? (:op d)) "a resolution miss is not tagged as an encode/decode failure")))

;; A Clojure-faithful value: keywords, a set, nested colls — transit preserves
;; all of it (unlike the lossy :json codec below).
(def ^:private rich-value {:a 1 :b [2 3] :s #{:x :y} :k :kw :str "hi"})

(deftest transit-codec-round-trips
  #?(:clj
     ;; JVM: :transit is unloaded until required, so one deftest walks the
     ;; actionable miss -> runtime require -> round-trip in guaranteed order.
     (do
       (let [d (codec-error-data #(codec/encode :transit rich-value))]
         (is (= :codec-error (:type d)) "an unloaded :transit keyword is a :codec-error")
         (is (= 'nats-cljc.codec.transit (:require d))
             "ex-data points at the namespace to require"))
       (require 'nats-cljc.codec.transit)
       (is (= rich-value (codec/decode :transit (codec/encode :transit rich-value)))
           ":transit round-trips Clojure-faithfully once required"))
     :cljs
     ;; cljs requires the namespace at compile time (ns form), so the unloaded
     ;; path is JVM-only; here just assert the round-trip.
     (is (= rich-value (codec/decode :transit (codec/encode :transit rich-value)))
         ":transit round-trips Clojure-faithfully")))

;; A JSON-faithful value: string keys keywordize back, values are JSON scalars
;; and vectors. :json is lossy (no keyword values, sets, or rich types), so the
;; round-trip value stays within what JSON preserves.
(def ^:private json-value {:a 1 :b "two" :c [3 4]})

(deftest json-codec-round-trips
  #?(:clj
     (do
       (let [d (codec-error-data #(codec/encode :json json-value))]
         (is (= :codec-error (:type d)) "an unloaded :json keyword is a :codec-error")
         (is (= 'nats-cljc.codec.json (:require d))
             "ex-data points at the namespace to require"))
       (require 'nats-cljc.codec.json)
       (is (= json-value (codec/decode :json (codec/encode :json json-value)))
           ":json round-trips a JSON-faithful value once required (keys keywordized)"))
     :cljs
     (is (= json-value (codec/decode :json (codec/encode :json json-value)))
         ":json round-trips a JSON-faithful value (keys keywordized)")))
