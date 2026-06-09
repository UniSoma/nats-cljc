(ns nats-cljc.test-support
  "Shared test-only helpers. Not a test namespace — the `-test$` selection
   (cognitect runner, shadow's :ns-regexp) never picks it up."
  #?(:cljs (:require [promesa.core :as p])))

;; Native clients deliver on their own schedule (a jnats listener thread; the
;; CLJS event-loop turn), so assertions wait for a state rather than race it.
#?(:clj
   (defn wait-for
     "Poll `pred` until truthy or `timeout-ms` elapses; return the last value."
     [pred timeout-ms]
     (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
       (loop []
         (or (pred)
             (when (< (System/currentTimeMillis) deadline)
               (Thread/sleep 20)
               (recur))))))
   :cljs
   (defn wait-for
     "Promise resolving to true once `pred` is truthy (polling every 25ms), or
      false at `timeout-ms` — the async-friendly twin of the JVM poll."
     [pred timeout-ms]
     (p/create
      (fn [resolve _reject]
        (let [deadline (+ (js/Date.now) timeout-ms)]
          (letfn [(check []
                    (cond
                      (pred)                     (resolve true)
                      (< (js/Date.now) deadline) (js/setTimeout check 25)
                      :else                      (resolve false)))]
            (check)))))))
