(ns nats-cljc.codec.json
  "Opt-in JSON codec (ADR 0004/0011). Requiring this namespace registers `:json` —
   never forced on consumers. org.clojure/data.json on the JVM, ambient js/JSON on
   cljs. Keys are keywordized on decode. Lossy by design: it is a polyglot wire,
   not a Clojure round-trip format — keyword *values* become strings and rich
   types (sets, symbols) are not preserved."
  (:require [nats-cljc.codec :as codec]
            #?(:clj [clojure.data.json :as json])))

;; data.json and js/JSON both deal in strings; the wire is bytes, so UTF-8
;; bridges the two on each platform.
(defn- str->bytes [s]
  #?(:clj  (.getBytes ^String s java.nio.charset.StandardCharsets/UTF_8)
     :cljs (.encode (js/TextEncoder.) s)))

(defn- bytes->str [b]
  #?(:clj  (String. ^bytes b java.nio.charset.StandardCharsets/UTF_8)
     :cljs (.decode (js/TextDecoder.) b)))

(defrecord JsonCodec []
  codec/ICodec
  (-encode [_ value]
    (str->bytes #?(:clj  (json/write-str value)
                   :cljs (js/JSON.stringify (clj->js value)))))
  (-decode [_ bytes]
    (let [s (bytes->str bytes)]
      #?(:clj  (json/read-str s :key-fn keyword)
         :cljs (js->clj (js/JSON.parse s) :keywordize-keys true)))))

(codec/register! :json (->JsonCodec))
