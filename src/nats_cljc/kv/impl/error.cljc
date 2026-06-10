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
   face differs from their stream-layer one."
  {10059 :bucket-not-found})

(def ^:private cas-code?
  "The JetStream API `err_code`s of the server's wrong-last-sequence condition —
   the wire form of a lost compare-and-set race, the same pair the stream layer
   re-faces `:wrong-last-sequence`."
  #{10071 10164})

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

(defn cas-error-data
  "Build the portable ex-data for a server-issued API error reaching a
   compare-and-set verb (`create`/`update`) over `key`: a wrong-last-sequence
   code is the lost race, re-faced as the KV-vocabulary `:type :wrong-revision`
   carrying the contested `:key` (ADR 0023) — one face shared by both verbs,
   since a create IS an update expecting revision 0 (which is how both native
   clients model it). Any other code falls through to the Bucket-verb
   normalization, so a CAS verb's non-race failures keep their usual faces."
  [code description key]
  (if (cas-code? code)
    {:type :wrong-revision :key key :code code :description description}
    (api-error-data code description)))
