(ns ^:no-doc nats-cljc.jetstream.consumer
  "Portable Consumer config-translation + closed-key-validation deep module (ADR
   0015/0020), the consumer sibling of `nats-cljc.jetstream.stream`. The pure,
   platform-neutral half of consumer management: the closed set of recognized config
   keys, the keyword-enum ↔ wire-string tables for the ack/deliver policies, and the
   pre-flight guards that raise `:unknown-config-key` / `:invalid-name` before any
   native call. The per-leg impl namespaces build the actual native config from these
   tables (Duration on the JVM, Nanos on CLJS) and read it back — the interop half —
   so this is the shared seam the no-server deep-module unit test covers. The Stream
   name constraint is identical, so the name guard reuses `stream/valid-name?`."
  (:require [clojure.set :as set]
            [nats-cljc.jetstream.stream :as stream]))

(def config-keys
  "The CLOSED set of recognized portable Consumer config keys (ADR 0020). A key
   outside it is caller misuse — `:unknown-config-key`, raised pre-flight — rather
   than silently dropped, so a misspelling fails loudly instead of vanishing.
   `:name` is the consumer name; `:durable?` (default true) selects a durable vs an
   ephemeral consumer (ADR 0021). Later Consumer slices grow it."
  #{:name :durable? :ack-policy :deliver-policy :ack-wait-ms :max-deliver :filter-subjects})

(def ack-policy->wire
  "Portable `:ack-policy` keyword → the NATS wire string both legs agree on (ADR 0020).
   The single source of truth each leg's native translation routes through."
  {:none "none" :all "all" :explicit "explicit"})

(def deliver-policy->wire
  "Portable `:deliver-policy` keyword → the NATS wire string. `:last-per-subject` is
   the lone non-identity mapping (the wire spells it `last_per_subject`), which is
   exactly why this table exists rather than `name`-ing the keyword."
  {:all "all" :last "last" :new "new" :last-per-subject "last_per_subject"})

(def wire->ack-policy
  "The reverse of `ack-policy->wire`: NATS wire string → portable `:ack-policy` keyword,
   used to read a consumer's active ack policy back into the normalized config map."
  (set/map-invert ack-policy->wire))

(def wire->deliver-policy
  "The reverse of `deliver-policy->wire`: NATS wire string → portable `:deliver-policy`
   keyword, used to read a consumer's active deliver policy back into the normalized config map."
  (set/map-invert deliver-policy->wire))

(def ordered-config-keys
  "The CLOSED set of recognized portable Ordered-consumer opts keys (ADR 0020). An
   Ordered consumer is server-managed — name, ack policy (none), and recreation are
   the client library's business — so the caller tunes only what it replays:
   `:filter-subjects` (a vector of subject strings) and `:deliver-policy` (as in
   `config-keys`). A key outside the set is caller misuse — `:unknown-config-key`,
   raised pre-flight — rather than silently dropped."
  #{:filter-subjects :deliver-policy})

(defn validate-ordered-config
  "Guard a portable Ordered-consumer `opts` map pre-flight (ADR 0015), before any
   native call: an unrecognized key (the map is closed) throws `:type
   :unknown-config-key` carrying the offending `:keys`. Every key is optional —
   there is no `:name` (the server/client manage the ephemeral's identity).
   Returns `opts` so it can sit in a promise chain stage."
  [opts]
  (let [unknown (remove ordered-config-keys (keys opts))]
    (when (seq unknown)
      (throw (ex-info "Unknown ordered consumer config key(s)"
                      {:type :unknown-config-key :keys (vec unknown)}))))
  opts)

(defn validate-name
  "Guard a Consumer `name` pre-flight, throwing a `:type :invalid-name` ex-info on
   caller misuse (ADR 0015); returns `name` so it can sit in a promise chain stage.
   A consumer name carries the same constraint as a stream name, so the check reuses
   `stream/valid-name?` — only the error message differs."
  [name]
  (when-not (stream/valid-name? name)
    (throw (ex-info "Invalid consumer name" {:type :invalid-name :name name})))
  name)

(defn validate-config
  "Guard a portable Consumer `config` map pre-flight (ADR 0015/0021), before any native
   call: an unrecognized key (the map is closed) throws `:type :unknown-config-key`
   carrying the offending `:keys`. A durable consumer (`:durable?` absent or true) requires
   a `:name`, so an omitted one throws the clearer `:type :missing-required-key` (carrying
   `:key`) rather than `:invalid-name {:name nil}`; an ephemeral (`:durable? false`) may omit
   it (the server assigns one). Any `:name` that IS supplied — durable or named-ephemeral —
   is run through `validate-name`. Returns `config` so it can sit in a promise chain stage."
  [config]
  (let [unknown (remove config-keys (keys config))]
    (when (seq unknown)
      (throw (ex-info "Unknown consumer config key(s)"
                      {:type :unknown-config-key :keys (vec unknown)}))))
  (let [durable? (not (false? (:durable? config)))]
    (when (and durable? (nil? (:name config)))
      (throw (ex-info "Durable consumer requires :name (set :durable? false for an ephemeral)"
                      {:type :missing-required-key :key :name}))))
  (when (some? (:name config))
    (validate-name (:name config)))
  config)
