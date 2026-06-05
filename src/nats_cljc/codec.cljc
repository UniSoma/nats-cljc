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

;; cljs `TextEncoder`/`TextDecoder` are stateless, so one module-level instance
;; each serves every call (`defonce`, but a stale instance after a reload is
;; harmless — unlike the codec records, the encoder carries no protocol impl).
;; The JVM bridges through `String`/`getBytes` directly, with no instance to hold.
#?(:cljs (defonce ^:private text-encoder (js/TextEncoder.)))
#?(:cljs (defonce ^:private text-decoder (js/TextDecoder.)))

(defn str->bytes
  "UTF-8-encode string `s` to wire bytes. The single UTF-8 bridge the built-in
   codecs and the opt-in `:json`/`:transit` namespaces share; public so a custom
   `ICodec` can produce honest wire bytes too."
  [s]
  #?(:clj  (.getBytes ^String s java.nio.charset.StandardCharsets/UTF_8)
     :cljs (.encode text-encoder s)))

(defn bytes->str
  "UTF-8-decode wire bytes `b` to a string — the inverse of `str->bytes`."
  [b]
  #?(:clj  (String. ^bytes b java.nio.charset.StandardCharsets/UTF_8)
     :cljs (.decode text-decoder b)))

(defn- bytes-value?
  "Is `x` the platform's wire-byte type? Deliberately platform-specific — `:bytes`
   is ADR 0004's one non-portable codec. JVM: a primitive `byte[]` only (jnats'
   native payload), so a `ByteBuffer` or boxed `Byte[]` is rejected. cljs: any
   `js/Uint8Array`, which by design includes its subclasses (notably a Node
   `Buffer`). The accepted *type* therefore differs per leg, but values never cross
   legs, so no single value is accepted on one and rejected on the other; the
   asymmetry (JVM exact, cljs lenient about subclasses) is documented, not silent."
  [x]
  #?(:clj  (bytes? x)
     :cljs (instance? js/Uint8Array x)))

(defprotocol ICodec
  "The codec extension point (ADR 0011): a pair of pure conversions between a
   Clojure value and wire bytes. Public — the moment a consumer ships a custom
   codec, these method names are API."
  (-encode [codec value] "Encode a Clojure value to wire bytes.")
  (-decode [codec bytes] "Decode wire bytes to a Clojure value."))

(defrecord ^:no-doc EdnCodec []
  ICodec
  (-encode [_ value] (str->bytes (pr-str value)))
  (-decode [_ bytes] (edn/read-string (bytes->str bytes))))

(defrecord ^:no-doc StringCodec []
  ICodec
  ;; Lenient: any value is coerced with `str` on encode; decode yields the UTF-8
  ;; string. No type guard (unlike `:bytes`).
  (-encode [_ value] (str->bytes (str value)))
  (-decode [_ bytes] (bytes->str bytes)))

(defrecord ^:no-doc BytesCodec []
  ICodec
  ;; Strict passthrough: the delivered `:data` is platform-native bytes — the
  ;; ADR-0004-sanctioned exception to Data being a portable Clojure value.
  ;; Encode demands platform bytes; anything else is a `:codec-error`. The guard
  ;; raises it with no `:op`; the encode wrapper re-stamps `:op :encode` (it is an
  ;; encode failure) while keeping this "got <type>" message.
  (-encode [_ value]
    (if (bytes-value? value)
      value
      (throw (ex-info (str ":bytes codec requires platform bytes, got " (type value))
                      {:type :codec-error :codec :bytes}))))
  (-decode [_ bytes] bytes))

(defonce ^{:no-doc true :doc "Keyword -> ICodec. Both the built-ins (registered just below)
   and opt-in/custom codecs enter through `register!`; `defonce` keeps the atom
   across a reload, so opt-in/custom registrations survive it."}
  registry
  (atom {}))

(defn register!
  "Register `codec` (an `ICodec`) under keyword `k`. The built-ins register
   themselves below; opt-in codec namespaces call this at load time, so requiring
   the namespace is what makes `k` resolvable."
  [k codec]
  (swap! registry assoc k codec)
  nil)

;; Register the built-ins on every load — NOT inside the `defonce`. A registry
;; seeded once would, after a `:reload` of this ns, keep the *old* records: built
;; against the previous `ICodec`, so `(encode :edn …)` throws "No implementation
;; of method -encode" until a JVM restart, breaking the reload-aware nREPL
;; workflow. Re-registering on each load overwrites those stale instances; opt-in
;; and custom codecs (registered under other keys) survive because `defonce`
;; keeps the atom.
(register! :edn    (->EdnCodec))
(register! :string (->StringCodec))
(register! :bytes  (->BytesCodec))

;; A connection's default codec, resolved once at connect (ADR 0011): `impl` is the
;; `ICodec` the hot path calls; `id` is the stable keyword (or `:custom`) error
;; stamping needs. Holding both lets steady-state encode/decode skip the registry
;; deref while a failure still names `:edn` (not the resolved record). Per-call
;; `:codec` overrides stay raw keyword/instance refs — the rare path — and resolve
;; through the registry as before. Internal plumbing — `^:no-doc` so it stays out
;; of the codec extension API (ICodec/register!/encode/decode/str->bytes), which is
;; the only codec surface a consumer is meant to touch.
(defrecord ^:no-doc Prepared [impl id])

(defn- opt-in-ns
  "The namespace a keyword codec registers from, by convention
   `nats-cljc.codec.<name>` (ADR 0011), so a registry miss can point at a namespace
   to `require` rather than just report the keyword unknown. By convention, not a
   static map, so a new opt-in codec needs no edit here. Trade-off: a *typo'd*
   keyword yields a hint for a namespace that won't resolve, rather than a plain
   \"unknown codec\" — acceptable since the dominant miss is a real opt-in codec
   left un-required."
  [codec]
  (symbol (str "nats-cljc.codec." (name codec))))

(defn- resolve-codec
  "Resolve a codec reference to an `ICodec`. A `Prepared` (the connection default,
   resolved once at connect) returns its `impl` with no registry deref — the hot
   path. Otherwise the common argument is a keyword, so that path runs next —
   `keyword?` plus a registry lookup — reserving the slower, uncached `satisfies?`
   for the rare instance case. A keyword miss throws `:codec-error` with a
   `:require '<ns>` hint (ADR 0011); a non-keyword that is not an `ICodec` is
   genuinely unknown. Either is a *resolution* failure, so no `:op` (distinct from
   an encode/decode failure)."
  [codec]
  (cond
    (instance? Prepared codec) (:impl codec)
    (keyword? codec)
    (or (get @registry codec)
        (let [ns (opt-in-ns codec)]
          (throw (ex-info (str "Codec " codec " is not loaded — (require '" ns ")")
                          {:type :codec-error :codec codec :require ns}))))
    (satisfies? ICodec codec) codec
    :else (throw (ex-info (str "Unknown codec: " codec)
                          {:type :codec-error :codec codec}))))

(defn- codec-id
  "A stable, keyword-shaped identifier for `codec` in error ex-data: a `Prepared`
   default yields the `id` it captured at connect; a registry keyword passes
   through; an `ICodec` instance becomes the sentinel `:custom`. Never the live
   instance or its class-hash `toString` — both leak the object into
   logs/serialized payloads and shift across reloads, where a keyword matches the
   built-in case so consumers can always expect a keyword under `:codec`."
  [codec]
  (cond
    (instance? Prepared codec) (:id codec)
    (keyword? codec) codec
    :else :custom))

(defn ^:no-doc prepare
  "Internal (called only by `nats-cljc.core/connect`; not codec extension API).
   Resolve a connection's default codec reference `ref` (a keyword or `ICodec`)
   once, into a `Prepared` the facade stores on the connection so steady-state
   encode and decode skip the registry deref (ADR 0011). Captures the stable id
   alongside the resolved record, so a failure on the default path still names
   `:edn` (or `:custom`), never the record. An unresolvable `ref` throws here — at
   connect — rather than lazily on first use."
  [ref]
  (->Prepared (resolve-codec ref) (codec-id ref)))

(defn- ->codec-error
  "Normalize an encode/decode failure to an `ex-info` `:type :codec-error` carrying
   the operation under `:op` and a stable `:codec` id (ADR 0006/0011). `:op` is
   *always* present here: resolution failures — the no-`:op` case — are raised by
   `resolve-codec` outside the try, so anything reaching this is an encode/decode
   failure. An already-`:codec-error` (the `:bytes` guard, or a codec that nests
   `codec/encode`/`decode` internally) is re-stamped with this op and codec rather
   than rethrown verbatim — so callers matching on `:op`/`:codec` see *this*
   operation, not an inner one — chaining its cause, never the codec-error itself,
   so `:codec-error` is never nested in `:codec-error`."
  [e codec op]
  (let [id (codec-id codec)]
    (if (= :codec-error (:type (ex-data e)))
      (ex-info (ex-message e)
               {:type :codec-error :codec id :op op}
               (ex-cause e))
      (ex-info (str "Codec " (name op) " failed for " id)
               {:type :codec-error :codec id :op op}
               e))))

(defn encode
  "Encode a Clojure value to wire bytes using `codec` (a keyword or `ICodec`).
   Any failure surfaces as `ex-info` `:type :codec-error`."
  [codec value]
  (let [c (resolve-codec codec)]
    (try
      (-encode c value)
      (catch #?(:clj Throwable :cljs :default) e
        (throw (->codec-error e codec :encode))))))

(defn decode
  "Decode wire bytes to a Clojure value using `codec` (a keyword or `ICodec`).
   Any failure surfaces as `ex-info` `:type :codec-error`."
  [codec bytes]
  (let [c (resolve-codec codec)]
    (try
      (-decode c bytes)
      (catch #?(:clj Throwable :cljs :default) e
        (throw (->codec-error e codec :decode))))))
