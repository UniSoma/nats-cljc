(ns ^:no-doc nats-cljc.jetstream.error
  "Shared JetStream error normalization (ADR 0020): the JetStream API error codes
   are identical numbers on jnats and nats.js, so mapping an `err_code` to its
   canonical operational `:type` is a single portable lookup — the JetStream
   sibling of `nats-cljc.error`'s server-error classifier. Seeded with the Phase-2
   entry point's code; later slices add rows (`:stream-not-found`,
   `:consumer-not-found`, `:wrong-last-sequence`).")

(def ^:private err-code->type
  "JetStream API `err_code` → canonical operational `:type` (ADR 0020)."
  {10039 :jetstream-not-enabled})

(defn api-error-type
  "Normalize a JetStream API `code` to its canonical operational `:type`, defaulting
   to the catch-all `:jetstream-api-error` for any code without a dedicated `:type`
   (ADR 0020). One shared lookup, since the codes are identical on both legs."
  [code]
  (get err-code->type code :jetstream-api-error))
