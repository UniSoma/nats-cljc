(ns ^:no-doc nats-cljc.kv.impl.bucket
  "Portable Bucket config-translation + closed-key-validation deep module (ADR
   0015/0023), the KV sibling of `nats-cljc.jetstream.impl.stream`: the closed set
   of recognized Bucket config keys, the storage keyword ↔ wire-string table, and
   the pre-flight guards that raise `:unknown-config-key` / `:missing-required-key`
   / `:invalid-name` before any native call. The per-leg impl namespaces build the
   actual native config from these tables (Duration on the JVM, ms options on
   CLJS) — the interop half — so this is the shared seam a single no-server unit
   test covers.")

(def config-keys
  "The CLOSED set of recognized portable Bucket config keys. A key outside it is
   caller misuse — `:unknown-config-key`, raised pre-flight — rather than silently
   dropped, so a misspelling fails loudly instead of vanishing. Topology keys
   (placement/republish/mirror/sources) are deliberately absent — additive
   minor-bump material later."
  #{:bucket :description :history :ttl-ms :max-value-size :max-bucket-size
    :storage :replicas :compression?})

(def storage->wire
  "Portable `:storage` keyword → the NATS wire string both legs agree on. KV-local
   rather than borrowed from the stream tables so the KV deep module never names
   its substrate (ADR 0023)."
  {:file "file" :memory "memory"})

;; A valid Bucket name is a non-empty run of alphanumerics, dash, and underscore —
;; the `[-\w]+` constraint both jnats (validateBucketName) and @nats-io/kv
;; (validBucketRe) enforce natively, pinned here so a malformed name is the same
;; portable `:invalid-name` pre-flight on every leg instead of a per-client throw.
;; Stricter than a Stream name: the Bucket names a `$KV.<bucket>.>` subject token.
(def ^:private name-re #"[a-zA-Z0-9_-]+")

(defn valid-name?
  "True when `name` is a string that is a well-formed Bucket name."
  [name]
  (and (string? name) (boolean (re-matches name-re name))))

(defn validate-name
  "Guard a Bucket `name` pre-flight, throwing a `:type :invalid-name` ex-info on
   caller misuse (ADR 0015); returns `name` so it can sit in a promise chain stage."
  [name]
  (when-not (valid-name? name)
    (throw (ex-info "Invalid bucket name" {:type :invalid-name :name name})))
  name)

(defn validate-config
  "Guard a portable Bucket `config` map pre-flight (ADR 0015), before any native
   call: an unrecognized key (the map is closed) throws `:type :unknown-config-key`
   carrying the offending `:keys`; an omitted `:bucket` (the one required key)
   throws the clearer `:type :missing-required-key` (carrying `:key`) rather than
   `:invalid-name {:name nil}`; a supplied `:bucket` runs through `validate-name`.
   Returns `config` so it can sit in a promise chain stage."
  [config]
  (let [unknown (remove config-keys (keys config))]
    (when (seq unknown)
      (throw (ex-info "Unknown bucket config key(s)"
                      {:type :unknown-config-key :keys (vec unknown)}))))
  (when (nil? (:bucket config))
    (throw (ex-info "Bucket config requires :bucket"
                    {:type :missing-required-key :key :bucket})))
  (validate-name (:bucket config))
  config)
