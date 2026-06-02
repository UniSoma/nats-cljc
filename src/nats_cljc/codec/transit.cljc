(ns nats-cljc.codec.transit
  "Opt-in transit-json codec (ADR 0004/0011). Requiring this namespace registers
   `:transit` — never forced on consumers. transit-clj on the JVM, transit-cljs
   on cljs; both expose `cognitect.transit`, only the writer/reader construction
   differs. transit-json (not msgpack — transit-cljs has no msgpack) is the
   cross-platform interop format, and it is Clojure-faithful: keywords, sets and
   symbols survive the round-trip."
  (:require [nats-cljc.codec :as codec]
            [cognitect.transit :as transit])
  #?(:clj (:import [java.io ByteArrayInputStream ByteArrayOutputStream])))

;; transit-cljs reads/writes JSON strings; the wire is bytes, so codec's shared
;; UTF-8 bridge converts between the two. The JVM writer/reader stream straight
;; to/from bytes, so no bridge there.
(defrecord TransitCodec []
  codec/ICodec
  (-encode [_ value]
    #?(:clj  (let [out (ByteArrayOutputStream.)]
               (transit/write (transit/writer out :json) value)
               (.toByteArray out))
       :cljs (codec/str->bytes (transit/write (transit/writer :json) value))))
  (-decode [_ bytes]
    #?(:clj  (transit/read (transit/reader (ByteArrayInputStream. bytes) :json))
       :cljs (transit/read (transit/reader :json) (codec/bytes->str bytes)))))

(codec/register! :transit (->TransitCodec))
