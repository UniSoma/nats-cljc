(ns ^:no-doc nats-cljc.jetstream.pull
  "Portable pull-poll pre-flight deep module (ADR 0015/0018). The pure,
   platform-neutral half of the `fetch`/`next` poll verbs: the `:expires-ms` guard
   that keeps a sub-floor or non-integer poll window from reaching either leg as an
   un-normalized native throw. Both clients enforce a 1000ms floor — jnats raises a
   bare `IllegalArgumentException`, nats.js an `InvalidArgumentError` — so the same
   portable input would otherwise yield two different, un-typed error shapes. A
   window below the floor (or not a whole number of milliseconds) is caller misuse,
   raised pre-flight as `:invalid-expires` before any native call; an omitted window
   passes through to the native default. The per-leg impl namespaces build the native
   pull options — the interop half — so this is the shared seam a single no-server
   unit covers, the pull sibling of `nats-cljc.jetstream.pub`.")

;; Both jnats (BaseConsumeOptions/MIN_EXPIRES_MILLS) and nats.js (PullConsumer fetch/next)
;; reject an expires window below 1000ms; pinned here so a sub-floor window is the same
;; portable :invalid-expires pre-flight on every leg instead of a per-client throw.
(def ^:private min-expires-ms 1000)

;; The portable `:batch` (max-messages) default when a caller omits it. Both legs reference
;; this so they can't silently diverge: jnats' own FetchConsumeOptions default is 500, so
;; the JVM leg must pass an explicit value to match nats.js' 100.
(def default-batch 100)

(defn valid-expires?
  "True when `ms` is a whole number of milliseconds at or above the 1000ms floor both
   clients enforce."
  [ms]
  (and (integer? ms) (>= ms min-expires-ms)))

(defn validate-expires
  "Guard the pull `opts` map's `:expires-ms` pre-flight (ADR 0015), before any native
   call: a window present but below the 1000ms floor (or not a whole number) throws
   `:type :invalid-expires` carrying the offending `:expires-ms`, identical on both
   legs. An omitted `:expires-ms` passes through to the native default. Returns `opts`
   so it can sit in a promise chain stage."
  [opts]
  (when-some [ms (:expires-ms opts)]
    (when-not (valid-expires? ms)
      (throw (ex-info "Pull :expires-ms must be a whole number of milliseconds >= 1000"
                      {:type :invalid-expires :expires-ms ms}))))
  opts)
