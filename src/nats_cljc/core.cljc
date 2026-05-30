(ns nats-cljc.core
  "Portable public facade for nats-cljc (always aliased `nats`).

   A thin `.cljc` surface over the internal protocol (ADR 0005): it owns codec
   encode/decode and ergonomics, delegating primitive operations to the platform
   Connection record. The same consumer code compiles and runs on the JVM, the
   browser, and Node."
  (:require [nats-cljc.codec :as codec]
            [nats-cljc.protocol :as proto]
            #?(:clj  [nats-cljc.impl.jvm :as impl]
               :cljs [nats-cljc.impl.js :as impl])))

(def version
  "Current library version."
  "0.1.0-SNAPSHOT")

(defn connect
  "Open a connection to the NATS server(s) in `:servers`, returning a
   platform-native promise (CompletableFuture on the JVM, js/Promise on
   ClojureScript) that resolves to a Connection. The connection's default codec
   is `:codec` (default `:edn`). Transport is fixed per platform: TCP on the JVM,
   WebSocket on ClojureScript (ADR 0001)."
  [opts]
  (impl/connect opts))

(defn publish
  "Publish `data` to `subject` on `conn`, encoding it with the connection's codec.
   Fire-and-forget: returns nil (ADR 0002)."
  [conn subject data]
  (proto/-publish conn subject (codec/encode (:codec conn) data))
  nil)

(defn subscribe
  "Subscribe to `subject`, returning a Subscription synchronously. `handler` is
   invoked once per message with `{:subject :data}`, where `:data` is decoded
   with the connection's codec (ADR 0007)."
  [conn subject handler]
  (let [codec-kw (:codec conn)]
    (proto/-subscribe conn subject
                      (fn [{:keys [subject bytes]}]
                        (handler {:subject subject
                                  :data    (codec/decode codec-kw bytes)})))))
