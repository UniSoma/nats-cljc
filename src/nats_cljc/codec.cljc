(ns nats-cljc.codec
  "Codecs convert between Clojure values and the bytes on the wire (ADR 0004).

   A codec is an `ICodec` — `-encode`/`-decode`. The dependency-free built-ins
   (`:edn` the default, `:string`, `:bytes`) are records held in a `defonce`
   registry keyed by keyword; opt-in codecs (`:transit`/`:json`) self-register
   from their own namespace via `register!`, so `(require …)` is what makes their
   keyword resolvable (ADR 0011). A custom codec is any `ICodec` instance and is
   accepted wherever a keyword is.

   `encode`/`decode` are the public seam `core` calls: they `resolve-codec` the
   reference (instance as-is, keyword via the registry) and call the protocol
   method. `:edn` decodes with clojure.edn / cljs.reader (never `eval`)."
  (:require #?(:clj  [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])))

(defn- str->bytes [s]
  #?(:clj  (.getBytes ^String s java.nio.charset.StandardCharsets/UTF_8)
     :cljs (.encode (js/TextEncoder.) s)))

(defn- bytes->str [b]
  #?(:clj  (String. ^bytes b java.nio.charset.StandardCharsets/UTF_8)
     :cljs (.decode (js/TextDecoder.) b)))

(defn- bytes-value? [x]
  #?(:clj  (bytes? x)
     :cljs (instance? js/Uint8Array x)))

(defprotocol ICodec
  "The codec extension point (ADR 0011): a pair of pure conversions between a
   Clojure value and wire bytes. Public — the moment a consumer ships a custom
   codec, these method names are API."
  (-encode [codec value] "Encode a Clojure value to wire bytes.")
  (-decode [codec bytes] "Decode wire bytes to a Clojure value."))

(defrecord EdnCodec []
  ICodec
  (-encode [_ value] (str->bytes (pr-str value)))
  (-decode [_ bytes] (edn/read-string (bytes->str bytes))))

(defrecord StringCodec []
  ICodec
  ;; Lenient: any value is coerced with `str` on encode; decode yields the UTF-8
  ;; string. No type guard (unlike `:bytes`).
  (-encode [_ value] (str->bytes (str value)))
  (-decode [_ bytes] (bytes->str bytes)))

(defrecord BytesCodec []
  ICodec
  ;; Strict passthrough: the delivered `:data` is platform-native bytes — the
  ;; ADR-0004-sanctioned exception to Data being a portable Clojure value.
  ;; Encode demands platform bytes; anything else is a `:codec-error` (rethrown
  ;; as-is by the wrapper, never wrapped with an `:op`).
  (-encode [_ value]
    (if (bytes-value? value)
      value
      (throw (ex-info (str ":bytes codec requires platform bytes, got " (type value))
                      {:type :codec-error :codec :bytes}))))
  (-decode [_ bytes] bytes))

(defonce ^{:doc "Keyword -> ICodec, seeded with the built-ins. Opt-in codecs add
   themselves via `register!`; `defonce` keeps registrations across REPL reloads."}
  registry
  (atom {:edn    (->EdnCodec)
         :string (->StringCodec)
         :bytes  (->BytesCodec)}))

(defn register!
  "Register `codec` (an `ICodec`) under keyword `k`. Opt-in codec namespaces call
   this at load time, so requiring the namespace is what makes `k` resolvable."
  [k codec]
  (swap! registry assoc k codec)
  nil)

(def ^:private opt-in-ns
  "Built-in keywords whose codec lives in an opt-in namespace, so a registry miss
   can point at the namespace to `require` instead of just reporting `:unknown`."
  {:transit 'nats-cljc.codec.transit
   :json    'nats-cljc.codec.json})

(defn- resolve-codec
  "An `ICodec` instance passes through; a keyword is looked up in the registry. A
   miss throws `:codec-error` naming the keyword — a resolution failure, so no
   `:op` (distinct from an encode/decode failure). A known opt-in keyword carries
   `:require '<ns>` and an actionable message; a genuinely unknown one does not."
  [codec]
  (if (satisfies? ICodec codec)
    codec
    (or (get @registry codec)
        (let [ns (opt-in-ns codec)]
          (throw (ex-info (if ns
                            (str "Codec " codec " is not loaded — (require '" ns ")")
                            (str "Unknown codec: " codec))
                          (cond-> {:type :codec-error :codec codec}
                            ns (assoc :require ns))))))))

(defn- ->codec-error
  "Normalize a failure to an `ex-info` `:type :codec-error` (ADR 0006/0011). An
   already-`:codec-error` ex-info (the `:bytes` guard, a registry miss) is
   rethrown as-is — never nested; ex-data stays minimal (no raw value/bytes)."
  [e codec op]
  (if (= :codec-error (:type (ex-data e)))
    e
    (ex-info (str "Codec " (name op) " failed for " codec)
             {:type :codec-error :codec codec :op op}
             e)))

(defn encode
  "Encode a Clojure value to wire bytes using `codec` (a keyword or `ICodec`).
   Any failure surfaces as `ex-info` `:type :codec-error`."
  [codec value]
  (try
    (-encode (resolve-codec codec) value)
    (catch #?(:clj Throwable :cljs :default) e
      (throw (->codec-error e codec :encode)))))

(defn decode
  "Decode wire bytes to a Clojure value using `codec` (a keyword or `ICodec`).
   Any failure surfaces as `ex-info` `:type :codec-error`."
  [codec bytes]
  (try
    (-decode (resolve-codec codec) bytes)
    (catch #?(:clj Throwable :cljs :default) e
      (throw (->codec-error e codec :decode)))))
