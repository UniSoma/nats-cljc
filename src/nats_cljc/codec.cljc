(ns nats-cljc.codec
  "Codecs convert between Clojure values and the bytes on the wire (ADR 0004).
   This slice ships only the dependency-free built-in `:edn` codec — the
   connection default. Per-call override and the opt-in `:transit`/`:json`
   codecs are a later slice; the `case` here is the seed of that registry.

   `:edn` decodes with clojure.edn / cljs.reader (never `eval`)."
  (:require #?(:clj  [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])))

(defn- str->bytes [s]
  #?(:clj  (.getBytes ^String s java.nio.charset.StandardCharsets/UTF_8)
     :cljs (.encode (js/TextEncoder.) s)))

(defn- bytes->str [b]
  #?(:clj  (String. ^bytes b java.nio.charset.StandardCharsets/UTF_8)
     :cljs (.decode (js/TextDecoder.) b)))

(defn encode
  "Encode a Clojure value to wire bytes using `codec`."
  [codec value]
  (case codec
    :edn (str->bytes (pr-str value))
    (throw (ex-info (str "Unknown codec: " codec)
                    {:type :codec-error :codec codec}))))

(defn decode
  "Decode wire bytes to a Clojure value using `codec`."
  [codec bytes]
  (case codec
    :edn (edn/read-string (bytes->str bytes))
    (throw (ex-info (str "Unknown codec: " codec)
                    {:type :codec-error :codec codec}))))
