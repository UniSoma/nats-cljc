(ns ^:no-doc nats-cljc.jetstream.impl.stream
  "Portable Stream config-translation + closed-key-validation deep module (ADR
   0015/0020). The pure, platform-neutral half of stream management: the closed set
   of recognized config keys, the keyword-enum ↔ wire-string tables, and the
   pre-flight guards that raise `:unknown-config-key` / `:invalid-name` before any
   native call. The per-leg impl namespaces build the actual native config from
   these tables (Duration on the JVM, Nanos on CLJS) and read it back — the
   interop half — so this is the shared seam a single no-server unit test covers,
   the stream sibling of `nats-cljc.jetstream.impl.error`."
  (:require [clojure.set :as set]))

(def config-keys
  "The CLOSED set of recognized portable Stream config keys (ADR 0020). A key
   outside it is caller misuse — `:unknown-config-key`, raised pre-flight — rather
   than silently dropped, so a misspelling fails loudly instead of vanishing.
   Later Stream slices (update/purge) grow it."
  #{:name :subjects :storage :retention :max-age-ms})

(def storage->wire
  "Portable `:storage` keyword → the NATS wire string both legs agree on (ADR 0020).
   The single source of truth each leg's native translation routes through."
  {:file "file" :memory "memory"})

(def retention->wire
  "Portable `:retention` keyword → the NATS wire string. `:work-queue` is the lone
   non-identity mapping (the wire spells it `workqueue`), which is exactly why this
   table exists rather than `name`-ing the keyword."
  {:limits "limits" :interest "interest" :work-queue "workqueue"})

(def wire->storage (set/map-invert storage->wire))
(def wire->retention (set/map-invert retention->wire))

;; A valid Stream name is a non-empty token free of the Subject delimiters/wildcards
;; (`.` `*` `>`), path separators (`/` `\`), and whitespace — the constraint both
;; jnats and nats.js enforce natively, pinned here so a malformed name is the same
;; portable `:invalid-name` pre-flight on every leg instead of a per-client throw.
(def ^:private name-re #"[^.*>/\\\s]+")

(defn valid-name?
  "True when `name` is a string that is a well-formed Stream name."
  [name]
  (and (string? name) (boolean (re-matches name-re name))))

(defn validate-name
  "Guard a Stream `name` pre-flight, throwing a `:type :invalid-name` ex-info on
   caller misuse (ADR 0015); returns `name` so it can sit in a promise chain stage."
  [name]
  (when-not (valid-name? name)
    (throw (ex-info "Invalid stream name" {:type :invalid-name :name name})))
  name)

(defn validate-config
  "Guard a portable Stream `config` map pre-flight (ADR 0015), before any native
   call: an unrecognized key (the map is closed) throws `:type :unknown-config-key`
   carrying the offending `:keys`, and the `:name` is run through `validate-name`.
   Returns `config` so it can sit in a promise chain stage."
  [config]
  (let [unknown (remove config-keys (keys config))]
    (when (seq unknown)
      (throw (ex-info "Unknown stream config key(s)"
                      {:type :unknown-config-key :keys (vec unknown)}))))
  (validate-name (:name config))
  config)
