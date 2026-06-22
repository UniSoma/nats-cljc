(ns examples.messaging.request-reply
  "Request-Reply
   Upstream: https://natsbyexample.com/examples/messaging/request-reply/go
   Exercises: nats-cljc.core request / reply / :queue groups."
  (:require
    [examples.util :as util]
    [nats-cljc.core :as nats.core]
    [promesa.core :as p]))

;; The request handler is just a subscription that *responds* to the message
;; sent to it. This kind of subscription is called a *service*.
(defn default-handler [conn]
  (fn [{:keys [subject] :as msg}]
    ;; Parse out the second token in the subject (everything after "greet.")
    ;; and use it as part of the response message.
    (nats.core/reply conn msg (str "hello, " (subs subject 6)))))

;; Like default-handler, but tallies each invocation under `worker` so we can
;; observe how the queue group spreads requests across responders.
(defn counting-handler [conn worker counts]
  (fn [{:keys [subject] :as msg}]
    (swap! counts update worker (fnil inc 0))
    (nats.core/reply conn msg (str "hello, " (subs subject 6)))))

;; `request` does the service request and returns a promise of the reply. Since
;; we are *waiting* for a reply we don't want to wait forever, so it rejects with
;; `:timeout` after `:timeout-ms` (default 5000) — and rejects right away with
;; `:no-responders` when nobody is subscribed to the subject at all.
(defn greet [conn person]
  (->
    (nats.core/request conn (str "greet." person) "hello")
    (p/then #(println (-> % :data)))))

(defn example [conn]
  (p/do
    ;; In addition to vanilla publish-subscribe, NATS supports request-reply
    ;; interactions. Under the covers this is just an optimized pair of
    ;; publish-subscribe operations: subscribe one service to "greet.*".
    (p/let [sub (nats.core/subscribe conn "greet.*" (default-handler conn))]
      (p/do
        ;; Each request is awaited before the next, so the replies print in order.
        (greet conn "joe")
        (greet conn "sue")
        (greet conn "bob")
        ;; What happens if the service is *unavailable*? Simulate it by draining
        ;; the handler. Unlike `unsubscribe`, `drain` is awaitable — it flushes
        ;; pending messages, then ends the subscription — so the next request is
        ;; guaranteed to find no responder.
        (nats.core/drain sub)
        ;; With the service gone, the request rejects with `:no-responders`.
        (-> (greet conn "pam")
          (p/catch #(println "No responders for subject" (-> % ex-data :subject))))))

    ;; Beyond the upstream example: a request can be load-balanced across many
    ;; service instances by putting them in the same queue group. The server
    ;; delivers each matching request to exactly one member, so the two
    ;; responders share the work — the basis for horizontally scaling a service.
    (p/let [counts (atom {})
            a (nats.core/subscribe conn "greet.*" (counting-handler conn "A" counts) {:queue "greeters"})
            b (nats.core/subscribe conn "greet.*" (counting-handler conn "B" counts) {:queue "greeters"})]
      (p/do
        (p/run! #(greet conn %) ["joe" "sue" "bob" "pam" "amy" "rob"])
        (nats.core/drain a)
        (nats.core/drain b)
        ;; The split across A/B is not guaranteed even — the only guarantee is
        ;; that each request is handled exactly once, so the counts sum to 6.
        (println "handled by:" @counts)))))

(defn -main [& _args]
  (util/run-example example))

