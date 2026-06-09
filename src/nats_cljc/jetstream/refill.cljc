(ns ^:no-doc nats-cljc.jetstream.refill
  "Portable consume refill-decision deep module (ADR 0015/0018). The pure,
   platform-neutral half of the continuous `consume` verb: the refill knobs'
   pre-flight guard and the threshold-unit conversion the legs disagree on.

   The portable `:threshold` is a COUNT — refill once the buffered count drops to
   it — which is nats.js-native (`threshold_messages`: repull when the messages
   still expected from the open pull fall to the count). jnats expresses the same
   decision as a PERCENT: it repulls `max(1, batch * percent / 100)` messages once
   the expected count falls to `batch` minus that amount. `threshold->percent` is
   the conversion the JVM leg feeds `thresholdPercent`, chosen so jnats' repull
   point lands exactly on the portable count. The per-leg impl namespaces build
   the native consume options — the interop half — so this is the shared seam a
   single no-server unit covers, the consume sibling of `nats-cljc.jetstream.pull`."
  (:require [nats-cljc.jetstream.pull :as pull]))

(defn threshold->percent
  "Convert the portable `:threshold` count into the jnats `thresholdPercent` whose
   repull point equals it: jnats repulls when the expected count falls to
   `batch - max(1, batch*P/100)` (integer division), so P is the ceiling of
   `100*(batch-threshold)/batch` — rounding up keeps the truncated repull amount at
   least `batch - threshold`, so the JVM never refills LATER than the count asks.
   Clamped to jnats' [1, 100] percent range: a count equal to `batch` (refill on
   any consumption) lands on the 1% floor, whose repull point is within
   `max(1, batch/100)` of full."
  [threshold batch]
  (-> (* 100 (- batch threshold))
      (+ (dec batch))
      (quot batch)
      (max 1)
      (min 100)))

(defn validate-opts
  "Guard the consume refill knobs pre-flight (ADR 0015), before any native call:

   A pull window is bounded by message COUNT (`:batch`/`:threshold`) OR by BYTES
   (`:max-bytes`), never both — nats.js forbids a user setting `max_messages` with
   `max_bytes`, so the portable contract is their intersection: `:max-bytes`
   combined with `:batch` or `:threshold` throws `:type :exclusive-window`. The
   JVM, where jnats would accept both as dual caps, rejects identically so the
   contract holds on both legs.

   `:threshold` must be a positive integer no greater than `:batch` (defaulting to
   `pull/default-batch` when omitted) — anything else throws `:type
   :invalid-threshold` carrying the offending `:threshold`, identical on both legs,
   rather than feeding either native client a refill point it would misread.
   `:expires-ms` routes through the shared pull guard (`:invalid-expires`). Every
   knob is optional; returns `opts` so it can sit in a promise chain stage."
  [opts]
  (when (and (:max-bytes opts) (or (:batch opts) (:threshold opts)))
    (throw (ex-info "Consume :max-bytes (a byte window) is mutually exclusive with the message-count :batch/:threshold"
                    {:type :exclusive-window :max-bytes (:max-bytes opts)
                     :batch (:batch opts) :threshold (:threshold opts)})))
  (when-some [threshold (:threshold opts)]
    (let [batch (:batch opts pull/default-batch)]
      (when-not (and (pos-int? threshold) (<= threshold batch))
        (throw (ex-info "Consume :threshold must be a positive integer no greater than :batch"
                        {:type :invalid-threshold :threshold threshold :batch batch})))))
  (pull/validate-expires opts))
