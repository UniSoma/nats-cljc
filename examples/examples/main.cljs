(ns examples.main
  "Node entry point for the examples: `node target/examples.js <name> [args...]`,
   where <name> is the example ns minus the `examples.` prefix, e.g.
   `messaging.pub-sub`. The JVM-only blocking example is deliberately absent."
  (:require [examples.messaging.pub-sub :as pub-sub]
            [examples.messaging.request-reply :as request-reply]
            [examples.messaging.json-payloads :as json-payloads]
            [examples.jetstream.limits-stream :as limits-stream]
            [examples.jetstream.interest-stream :as interest-stream]
            [examples.jetstream.workqueue-stream :as workqueue-stream]
            [examples.jetstream.pull-consumer :as pull-consumer]
            [examples.jetstream.fetch-messages :as fetch-messages]
            [examples.jetstream.ack-ack :as ack-ack]
            [examples.kv.intro :as kv-intro]
            [examples.services.intro :as services-intro]))

(def examples
  {"messaging.pub-sub"          pub-sub/-main
   "messaging.request-reply"    request-reply/-main
   "messaging.json-payloads"    json-payloads/-main
   "jetstream.limits-stream"    limits-stream/-main
   "jetstream.interest-stream"  interest-stream/-main
   "jetstream.workqueue-stream" workqueue-stream/-main
   "jetstream.pull-consumer"    pull-consumer/-main
   "jetstream.fetch-messages"   fetch-messages/-main
   "jetstream.ack-ack"          ack-ack/-main
   "kv.intro"                   kv-intro/-main
   "services.intro"             services-intro/-main})

(defn -main [& [name & args]]
  (if-let [run (get examples name)]
    (apply run args)
    (do (println (if name (str "Unknown example: " name) "Usage: node target/examples.js <name> [args...]"))
        (println "Available:")
        (doseq [k (sort (keys examples))] (println " " k))
        (set! (.-exitCode js/process) 1))))
