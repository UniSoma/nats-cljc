(ns examples.util
  "Shared envelope for the natsbyexample ports. `run-example` connects, runs the
   example body, then flushes and drains so every outbound message is delivered
   before the connection closes — and forks the JVM/cljs await so a single body
   runs on both legs (ADR 0001/0002).

   Scope: it owns the *connection* lifecycle only. Resource cleanup — deleting
   streams, KV buckets, consumers for idempotent re-runs — is the example body's
   job, done before it returns; flush + drain only closes the connection."
  (:require
    [nats-cljc.core :as nats.core]
    [promesa.core :as p]))

;; The local anonymous nats-server (ci/nats.conf): TCP on the JVM, WebSocket on
;; ClojureScript (ADR 0001). Auth examples pass their own conn-opts instead.
(def default-url #?(:clj "nats://127.0.0.1:4222" :cljs "ws://127.0.0.1:8080"))

(defn run-example
  "Connect with `conn-opts` (default: the anonymous server), run `(body conn)`,
   then flush and drain. `body` returns a promise of its async work, or any value
   if it is synchronous after connect.

   Forks the await per platform: on the JVM it blocks until the whole chain
   (drain included) settles, since `clojure -m` exits the moment -main returns; on
   cljs it returns the promise and lets Node's event loop drain it (the
   examples.main dispatcher awaits it and reports a rejection). flush runs only on
   the success path — a body that rejects goes straight to drain via p/finally."
  ([body] (run-example {:servers [default-url]} body))
  ([conn-opts body]
    (-> (p/let [conn (nats.core/connect conn-opts)]
          (-> (p/do (body conn))
            (p/then (fn [_] (nats.core/flush conn)))
            (p/finally (fn [_ _] (nats.core/drain conn)))))
      #?(:clj deref :cljs identity))))
