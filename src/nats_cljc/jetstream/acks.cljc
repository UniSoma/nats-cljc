(ns ^:no-doc nats-cljc.jetstream.acks
  "Ack-payload construction (ADR 0019): the pure msg+opts -> wire-bytes deep module
   behind the public ack family. The payloads are library-owned, version-independent
   NATS protocol constants, so building them is one portable code path — the bytes a
   verb publishes to the ack subject are byte-identical on both legs. Named `acks`,
   not `ack`: a CLJS sub-namespace and a same-named var share one path in the
   compiled JS object graph, so ns `jetstream.ack` would collide with the facade's
   `ack` verb (`nats_cljc.jetstream.ack`) and break the Node/browser legs."
  (:require [nats-cljc.codec :as codec]))

(defn ack-subject
  "The ack address of the delivered JetStream message `msg` — under `:js
   :ack-subject`, never a top-level `:reply` (ADR 0019). Throws an ex-info
   `:type :no-ack-subject` when `msg` carries none (it never came off a JetStream
   pull), the `reply`-guard precedent: fail before publishing to a nil subject."
  [msg]
  (or (get-in msg [:js :ack-subject])
      (throw (ex-info "Message has no ack subject"
                      {:type :no-ack-subject :subject (:subject msg)}))))

(defn payload
  "The wire bytes the ack verb `verb` publishes to a message's ack subject. A nak
   `:delay-ms` rides as the protocol's JSON body, converted to the nanoseconds the
   server speaks."
  [verb {:keys [delay-ms]}]
  (codec/str->bytes
   (case verb
     :ack "+ACK"
     :nak (if delay-ms
            (str "-NAK {\"delay\":" (* 1000000 delay-ms) "}")
            "-NAK")
     :working "+WPI"
     :term    "+TERM")))
