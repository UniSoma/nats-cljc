(ns ^:no-doc nats-cljc.impl.msg
  "Message plumbing shared by the core and JetStream facades: header
   normalization and trimming, and the per-call codec precedence rule. Internal —
   here rather than `nats-cljc.core` so the public namespaces expose only API."
  (:require [clojure.string :as str]))

;; A valid header name is a non-empty printable-ASCII token with no colon (the
;; name/value delimiter): every char in 0x20–0x7E except 0x3A. The class is split
;; around the colon (0x39 | 0x3B) rather than using regex intersection, which Java
;; supports but JS does not — so this one literal matches identically on both legs.
(def ^:private header-name-re #"[\x20-\x39\x3B-\x7E]+")

;; A valid header value is printable ASCII (0x20–0x7E), empty allowed. This rejects
;; CR/LF (header injection) and every non-ASCII char — pinning one rule where the
;; native clients otherwise diverge (jnats rejects non-ASCII, nats.js publishes it).
(def ^:private header-value-re #"[\x20-\x7E]*")

(defn normalize-headers
  "Normalize a user `:headers` map to the canonical portable form the protocol
   carries: `{name -> vector-of-strings}` with case-sensitive string names. A
   scalar value is wrapped in a one-element vector; a vector is kept as-is
   (CONTEXT: Headers). Returns nil for nil/empty input so `:headers` stays absent
   when none were set.

   Names and values must be strings: jnats and nats.js diverge on a non-string
   value (notably nil — jnats drops it and publishes headerless, nats.js throws),
   so we reject it here with a portable `:type :invalid-header` ex-info rather
   than leak that divergence. A set is treated as a scalar (its order is
   undefined); pass a vector for multiple values.

   Names must also be valid header tokens — non-empty printable-ASCII with no
   colon — and values must be printable ASCII (CR/LF and any non-ASCII rejected).
   An invalid name would otherwise reach the native client, whose throw publish
   mislabels as :max-payload-exceeded, and a non-ASCII value is where jnats and
   nats.js silently diverge (jnats rejects, nats.js publishes); validating both
   here keeps the caller-misuse a portable `:type :invalid-header` on every leg
   (CONTEXT: Headers).

   Shared by both facades, but it only enforces the `:invalid-header` shape rules
   above — NOT the reserved `Nats-*` pre-flight, which lives separately in
   `nats-cljc.jetstream.impl.pub`. A JetStream-style caller must run
   `validate-headers` first (as the facade does); reaching this directly would let
   a reserved header through."
  [headers]
  (when (seq headers)
    (reduce-kv (fn [m k v]
                 (when-not (string? k)
                   (throw (ex-info "Header names must be strings"
                                   {:type :invalid-header :name k})))
                 (when-not (re-matches header-name-re k)
                   (throw (ex-info "Header names must be printable-ASCII tokens without a colon"
                                   {:type :invalid-header :name k})))
                 (let [vs (if (sequential? v) (vec v) [v])]
                   (when-not (every? string? vs)
                     (throw (ex-info "Header values must be strings"
                                     {:type :invalid-header :name k :values vs})))
                   (when-not (every? #(re-matches header-value-re %) vs)
                     (throw (ex-info "Header values must be printable-ASCII without CR/LF"
                                     {:type :invalid-header :name k :values vs})))
                   (assoc m k vs)))
               {} headers)))

(defn trim-headers
  "Apply the portable header-value contract to a raw canonical headers map
   `{name -> vector-of-strings}`: strip surrounding whitespace from every value,
   which is insignificant on delivery (nats.js already trims, so the JS leg
   double-trims harmlessly; jnats does not, so this is what makes the JVM leg
   agree). The single home of the trim contract, shared by core's `decode-msg`
   and JetStream's `decode-js-msg` (CONTEXT: Headers)."
  [headers]
  (reduce-kv (fn [m k vs] (assoc m k (mapv str/trim vs))) {} headers))

(defn effective-codec
  "The codec for a single call: a per-call `:codec` in `opts` overrides the
   connection default, else the connection's `:codec` — the `Prepared` resolved
   once at connect, so the default path skips the registry deref (ADR 0011). The
   one place the precedence rule lives, shared by both facades so
   publish/subscribe/request/reply can't drift. A raw override resolves through
   the registry in `codec/encode`/`decode`."
  [conn opts]
  (or (:codec opts) (:codec conn)))
