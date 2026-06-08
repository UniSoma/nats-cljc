(ns ^:no-doc nats-cljc.jetstream.error
  "Shared JetStream error normalization (ADR 0020): the JetStream API error codes
   are identical numbers on jnats and nats.js, so mapping an `err_code` to its
   canonical operational `:type` is a single portable lookup — the JetStream
   sibling of `nats-cljc.error`'s server-error classifier. Seeded with the Phase-2
   entry point's code, the Stream/Consumer tracers' not-found codes, and acked
   publish's wrong-last-sequence; later slices add rows.")

(def ^:private err-code->type
  "JetStream API `err_code` → canonical operational `:type` (ADR 0020)."
  {10039 :jetstream-not-enabled
   10014 :consumer-not-found
   10059 :stream-not-found
   10071 :wrong-last-sequence})

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
