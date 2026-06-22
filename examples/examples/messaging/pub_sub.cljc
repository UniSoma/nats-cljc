(ns examples.messaging.pub-sub
  "Core Publish-Subscribe
   Upstream: https://natsbyexample.com/examples/messaging/pub-sub/cli
   Exercises: nats-cljc.core publish / subscribe / unsubscribe."
  (:require
    [examples.util :as util]
    [nats-cljc.core :as nats.core]))

(defn example [conn]
  ;; Publish a message to the subject "greet.bob". Fire and forget.
  ;; Without a subscription, this message will be lost.
  (nats.core/publish conn "greet.bob" "hello")

  ;; Subscribe to all subjects that match the pattern "greet.*".
  ;; The callback will be called for each message received on a matching subject.
  ;; The return value of subscribe is a subscription object that can be used to unsubscribe later.
  (let [sub (nats.core/subscribe conn "greet.*"
              (fn [{:keys [subject data]}]
                (println (str data " on subject " subject))))]

    ;; Notice that the first message printed is "greet.joe" and not "greet.bob".
    ;; This is because the subscription was created after the first message was published.
    ;; Subscribers must be connected showing interest in a subject for the server to relay the message to the client.
    (doseq [person ["joe" "pam" "sue"]]
      (nats.core/publish conn (str "greet." person)
        {:from person, :message "hello"}))

    ;; Unsubscribe from the subject after receiving 3 messages (max is optional).
    ;; The server will stop sending messages to the client after this point.
    (nats.core/unsubscribe sub 3)

    ;; This message will not be received by the subscriber because it has been unsubscribed.
    (nats.core/publish conn "greet.bob" "hello again")))

(defn -main [& _args]
  (util/run-example example))
