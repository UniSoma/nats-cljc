(ns ^:no-doc nats-cljc.jetstream.impl.error
  "Shared JetStream error normalization (ADR 0020): the JetStream API error codes
   are identical numbers on jnats and nats.js, so mapping an `err_code` to its
   canonical operational `:type` is a single portable lookup — the JetStream
   sibling of `nats-cljc.impl.error`'s server-error classifier. Seeded with the Phase-2
   entry point's code, the Stream/Consumer tracers' not-found codes, and acked
   publish's wrong-last-sequence; later slices add rows.

   Also home to the consume side-band classifier: the 409 statuses the server
   issues against an open pull are the same wire values on both legs (jnats hands
   the raw Status through its ErrorListener, nats.js through its consume status
   events), so classifying them into the per-consume operational `:type`s is one
   shared, case-insensitive table too (ADR 0020)."
  (:require [clojure.string :as str]))

(def ^:private err-code->type
  "JetStream API `err_code` → canonical operational `:type` (ADR 0020)."
  {10039 :jetstream-not-enabled
   10014 :consumer-not-found
   10037 :no-message-found
   10059 :stream-not-found
   10071 :wrong-last-sequence
   10164 :wrong-last-sequence})

(defn api-error-type
  "Normalize a JetStream API `code` to its canonical operational `:type`, defaulting
   to the catch-all `:jetstream-api-error` for any code without a dedicated `:type`
   (ADR 0020). One shared lookup, since the codes are identical on both legs."
  [code]
  (get err-code->type code :jetstream-api-error))

(defn api-error-data
  "Build the portable ex-data for a server-issued JetStream API error from its
   `code` and `description` (ADR 0020): the normalized `:type` plus the `:code` and
   `:description` the catch-all `:jetstream-api-error` carries. One shared shape, so
   both legs surface a server rejection identically regardless of which native
   exception carried it."
  [code description]
  {:type (api-error-type code) :code code :description description})

(defn side-band-type
  "Classify a consume-time side-band status — a 409 the server issues against an
   open pull — into its canonical operational `:type` (ADR 0020): `consumer
   deleted` is `:consumer-deleted`, `stream deleted` reuses `:stream-not-found`
   (a backing-stream loss), the `exceeded max*` family and `message size exceeds
   maxbytes` collapse into `:exceeded-limits`, and any other status falls to the
   `:jetstream-api-error` catch-all. Case-insensitive on purpose: the wire values
   are identical on both legs, but jnats surfaces them title-cased (`Consumer
   Deleted`) where nats.js lowercases — one shared table must absorb both."
  [code description]
  (let [d (str/lower-case (or description ""))]
    (if (= 409 code)
      (cond
        (= d "consumer deleted")                  :consumer-deleted
        (= d "stream deleted")                    :stream-not-found
        (str/starts-with? d "exceeded max")       :exceeded-limits
        (= d "message size exceeds maxbytes")     :exceeded-limits
        :else                                     :jetstream-api-error)
      :jetstream-api-error)))

(defn side-band-error
  "Build the portable ex-info for a consume side-band status from its `code` and
   `description` (ADR 0020): the classified `:type` plus the raw `:code` and
   `:description`, one shared shape so both legs deliver the condition to a
   consume's `:on-error` identically."
  [code description]
  (ex-info (str "Consume side-band status: " description)
           {:type (side-band-type code description) :code code :description description}))

(defn side-band-terminal?
  "Is a classified side-band `:type` terminal — the consumer or its backing stream
   is gone, so the consume cannot continue and its handle completes (ADR 0020)?
   `:heartbeats-missed` and `:exceeded-limits` are conditions a consume can ride
   out; these are not."
  [type]
  (contains? #{:consumer-deleted :stream-not-found :consumer-not-found} type))

(defn heartbeats-missed-error
  "Build the portable ex-info for the client-side missed-idle-heartbeats condition
   (ADR 0020). Synthesized by each native client's own monitor rather than read
   off the wire, so it carries no status code — a bare `{:type ...}` keeps the two
   legs byte-identical."
  []
  (ex-info "Idle heartbeats missed" {:type :heartbeats-missed}))
