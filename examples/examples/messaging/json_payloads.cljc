(ns examples.messaging.json-payloads
  "JSON for Message Payloads
   Upstream: https://natsbyexample.com/examples/messaging/json/go
   Exercises: nats-cljc.codec.json vs the EDN default codec, and two ways to
   handle a malformed payload — strict (:on-error drops it) vs salvage (inline
   decode keeps it)."
  (:require
    [examples.util :as util]
    [nats-cljc.codec :as codec]
    [nats-cljc.codec.json]
    [nats-cljc.core :as nats.core]
    [promesa.core :as p]))

(def payload {:foo "bar" :bar 27})

;; Strict path: let the connection's :json codec decode every message and route
;; any failure to :on-error as a :codec-error. The handler only ever sees valid
;; data; the bad message is dropped (ADR 0006). Good when you only want clean input.
(defn strict-path [conn]
  (nats.core/subscribe conn "strict.foo"
    (fn [{:keys [data]}]
      (println "  valid payload:" data))
    {:on-error (fn [e]
                 (when (= :codec-error (:type (ex-data e)))
                   (println "  dropped invalid JSON payload")))})
  (nats.core/publish conn "strict.foo" payload)
  (nats.core/publish conn "strict.foo" "not json" {:codec :string}))

;; Salvage path: take the raw bytes (:bytes codec), try to decode as JSON, and on
;; a :codec-error fall back to the raw string instead of discarding. "The payload
;; is a sequence of bytes, so it is up to the application to define how to
;; serialize and deserialize it" — the try-json/show-raw move every upstream
;; sibling makes (Deno m.json()/m.string(), Rust, Python).
(defn salvage-path [conn]
  (nats.core/subscribe conn "salvage.foo"
    (fn [{:keys [data]}]
      (try
        (println "  valid payload:" (codec/decode :json data))
        (catch #?(:clj Throwable :cljs :default) e
          (if (= :codec-error (:type (ex-data e)))
            (println "  invalid JSON payload, raw:" (codec/bytes->str data))
            (throw e)))))
    {:codec :bytes})
  (nats.core/publish conn "salvage.foo" payload)
  (nats.core/publish conn "salvage.foo" "not json" {:codec :string}))

(defn example [conn]
  ;; Surface codec.json vs the EDN default: the SAME value, two wire formats.
  (println "JSON wire:" (codec/bytes->str (codec/encode :json payload)))
  (println " EDN wire:" (codec/bytes->str (codec/encode :edn payload)))
  ;; Each subscription delivers on its own thread, so run the two paths one at a
  ;; time: the short delay lets the strict block's messages round-trip and print
  ;; before the salvage block starts, otherwise the two interleave. The salvage
  ;; block needs no trailing wait — run-example flushes and drains the connection
  ;; on the way out, delivering its messages before close.
  (p/do
    (println "strict path (:json + :on-error, drops bad input):")
    (strict-path conn)
    (p/delay 200)
    (println "salvage path (:bytes + inline decode, keeps bad input):")
    (salvage-path conn)))

(defn -main [& _args]
  (util/run-example {:codec :json} example))
