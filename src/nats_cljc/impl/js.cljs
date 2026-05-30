(ns nats-cljc.impl.js
  "ClojureScript platform implementation: a Connection record wrapping
   @nats-io/nats-core over WebSocket (ADR 0001/0003), serving browser and Node
   from one package. All JS interop is quarantined here (ADR 0005)."
  (:require [nats-cljc.protocol :as proto]
            ["@nats-io/nats-core" :as nats-core]))

(defrecord JsConnection [client codec]
  proto/Conn
  (-publish [_ subject bytes]
    (.publish ^js client subject bytes))
  (-subscribe [_ subject handler]
    ;; A subscribe with a :callback delivers per-message and returns the
    ;; Subscription synchronously (instead of becoming an async iterable).
    (.subscribe ^js client subject
                #js {:callback (fn [_err ^js msg]
                                 (handler {:subject (.-subject msg)
                                           :bytes   (.-data msg)}))})))

(defn connect
  "Open a WebSocket connection to `:servers`, returning a js/Promise that resolves
   to a JsConnection (ADR 0002). `:codec` defaults to :edn."
  [{:keys [servers codec] :or {codec :edn}}]
  (-> (nats-core/wsconnect (clj->js {:servers servers}))
      (.then (fn [nc] (->JsConnection nc codec)))
      (.catch (fn [e]
                (throw (ex-info "Failed to connect to NATS"
                                {:type :connect-failed :servers servers}
                                e))))))
