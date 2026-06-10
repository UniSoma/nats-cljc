(ns ^:no-doc nats-cljc.kv.impl.error
  "Shared KV error normalization (ADR 0023): the KV impl layer owns the mapping
   from each native client's stream-flavored failures to the KV-flavored canonical
   `:type`s, and this is its portable half — the KV sibling of
   `nats-cljc.jetstream.impl.error`. A Bucket is a stream under the hood, so a
   missing Bucket surfaces from both natives as the very same not-found API error a
   missing Stream would; re-facing that code here is what keeps the portable
   surface speaking KV vocabulary. Codes without a KV face fall through to the
   shared JetStream table (the entry point's 10039, the operational catch-all)."
  (:require [nats-cljc.jetstream.impl.error :as jet-err]))

(def ^:private err-code->type
  "JetStream API `err_code` → canonical KV `:type` (ADR 0023) — the codes whose KV
   face differs from their stream-layer one. Later slices add the CAS row."
  {10059 :bucket-not-found})

(defn api-error-data
  "Build the portable ex-data for a server-issued API error reaching the KV layer
   from its `code` and `description`: the KV-faced `:type` when the code has one
   (ADR 0023), else the shared JetStream normalization — so `:jetstream-not-enabled`
   and the `:jetstream-api-error` catch-all surface identically to Phase 2. One
   shared shape, since the codes are identical on both legs."
  [code description]
  (if-let [type (err-code->type code)]
    {:type type :code code :description description}
    (jet-err/api-error-data code description)))
