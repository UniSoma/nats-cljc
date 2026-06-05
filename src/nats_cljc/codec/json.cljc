(ns nats-cljc.codec.json
  "Opt-in JSON codec (ADR 0004/0011). Requiring this namespace registers `:json` —
   never forced on consumers. org.clojure/data.json on the JVM, ambient js/JSON on
   cljs. Keys are keywordized on decode. Lossy by design: it is a polyglot wire,
   not a Clojure round-trip format — keyword *values* become strings and rich
   types (sets, symbols) are not preserved. The loss is also platform-asymmetric
   for large integers: the JVM reader keeps a JSON integer as an exact `Long`, but
   cljs `js/JSON.parse` yields an f64, silently rounding beyond 2^53 — so an
   integer ID past 2^53 does not survive a JVM->cljs leg intact. Documented, not
   guarded (see ADR 0004)."
  (:require [nats-cljc.codec :as codec]
            #?(:clj [clojure.data.json :as json])))

;; data.json and js/JSON both deal in strings; the wire is bytes, so codec's
;; shared UTF-8 bridge converts between the two on each platform.
(defrecord ^:no-doc JsonCodec []
  codec/ICodec
  (-encode [_ value]
    (codec/str->bytes #?(:clj  (json/write-str value)
                         :cljs (js/JSON.stringify (clj->js value)))))
  (-decode [_ bytes]
    (let [s (codec/bytes->str bytes)]
      #?(:clj  (json/read-str s :key-fn keyword)
         :cljs (js->clj (js/JSON.parse s) :keywordize-keys true)))))

(codec/register! :json (->JsonCodec))
