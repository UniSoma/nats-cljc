(ns ^:no-doc nats-cljc.service.impl.config
  "Portable Service config pre-flight deep module (ADR 0015/0024), the services
   sibling of `nats-cljc.kv.impl.bucket`: the strict-from-day-one guards that raise
   `:missing-required-key` / `:invalid-name` / `:invalid-version` /
   `:duplicate-endpoint` before any native or wire call, thrown identically on both
   legs. The per-leg impl namespaces build the actual native Service from a config
   this has already passed — the interop half — so this is the shared seam a single
   no-server unit test covers.

   Endpoint `:subject` syntax is deliberately NOT a pre-flight: a subject is the
   server's concern, native/server-enforced (ADR 0024). Empty or absent
   `:endpoints` is legal.")

;; A valid service or endpoint name is a non-empty run of alphanumerics, dash, and
;; underscore — the `[-\w]+` constraint both natives enforce on a name (jnats'
;; `validateIsRestrictedTerm`, @nats-io/services' `validName` regex `^[-\w]+$`),
;; pinned here so a malformed name is the same portable `:invalid-name` pre-flight
;; on every leg. A name is a subject token in the `$SRV.*` backplane, so it is the
;; same infrastructure-name rule a Bucket carries (ADR 0024).
(def ^:private name-re #"[a-zA-Z0-9_-]+")

(defn valid-name?
  "True when `name` is a string that is a well-formed service/endpoint name."
  [name]
  (and (string? name) (boolean (re-matches name-re name))))

(defn validate-name
  "Guard a service or endpoint `name` pre-flight, throwing a `:type :invalid-name`
   ex-info carrying the offending `:name` on caller misuse (ADR 0015); returns
   `name` so it can sit in a promise chain stage."
  [name]
  (when-not (valid-name? name)
    (throw (ex-info "Invalid name" {:type :invalid-name :name name})))
  name)

;; The strict, anchored semver both natives accept (jnats' `SEMVER_PATTERN`, the
;; full MAJOR.MINOR.PATCH(-prerelease)?(+build)? grammar) — pinned here so a
;; non-semver `:version` is the same portable `:invalid-version` pre-flight on every
;; leg. Verified against BOTH natives at the 3.4.0 floor on the borderline pair:
;; "1.0" is rejected (no PATCH) and "1.2.3-rc1+build" is accepted by jnats'
;; `validateSemVer` AND @nats-io/services' `parseSemVer` (its unanchored
;; `(\d+).(\d+).(\d+)` matches the leading "1.2.3" and rejects the two-segment
;; "1.0"), so this stricter regex is the safe common subset.
(def ^:private semver-re
  #"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?")

(defn valid-version?
  "True when `version` is a string that is well-formed semver."
  [version]
  (and (string? version) (boolean (re-matches semver-re version))))

(defn validate-version
  "Guard a Service `:version` pre-flight, throwing a `:type :invalid-version`
   ex-info carrying the offending `:version` on caller misuse (ADR 0015); returns
   `version` so it can sit in a promise chain stage."
  [version]
  (when-not (valid-version? version)
    (throw (ex-info "Invalid version" {:type :invalid-version :version version})))
  version)

(defn validate-config
  "Guard a portable Service `config` map pre-flight (ADR 0015/0024), before any
   native or wire call: an omitted `:name` or `:version` throws `:type
   :missing-required-key` carrying the offending `:key`; a malformed service or
   endpoint `:name` throws `:type :invalid-name`; a non-semver `:version` throws
   `:type :invalid-version`; two endpoints sharing a `:name` throw `:type
   :duplicate-endpoint` carrying the offending `:name` (the per-endpoint stats key
   must be unique). Empty or absent `:endpoints` is legal. Endpoint `:subject`
   syntax stays native/server-enforced. Returns `config` so it can sit in a promise
   chain stage."
  [{:keys [name version endpoints] :as config}]
  (when (nil? name)
    (throw (ex-info "Service config requires :name"
                    {:type :missing-required-key :key :name})))
  (when (nil? version)
    (throw (ex-info "Service config requires :version"
                    {:type :missing-required-key :key :version})))
  (validate-name name)
  (validate-version version)
  (doseq [{ep-name :name} endpoints]
    (validate-name ep-name))
  (let [names (map :name endpoints)
        dup   (->> names (frequencies) (some (fn [[n c]] (when (> c 1) n))))]
    (when dup
      (throw (ex-info "Duplicate endpoint name"
                      {:type :duplicate-endpoint :name dup}))))
  config)
