(ns nats-cljc.blocking.core-test
  "JVM-only suite for the blocking convenience layer (ADR 0008). Plain `.clj`, so
   shadow-cljs never loads it — the pull API has no ClojureScript counterpart.
   Talks to the anonymous TCP server on :4222 (same one the portable suite uses)."
  (:require [clojure.test :refer [deftest is]]
            [nats-cljc.blocking.core :as nats]))

(def ^:private server-url "nats://127.0.0.1:4222")

(def ^:private subject "blocking.tracer")
(def ^:private payload {:hello "world" :n 42 :nested [1 2 {:k :v}]})

;; A subject nothing subscribes — exercises the :no-responders failure mode.
(def ^:private no-responders-subject "blocking.no-responders")

;; A server nothing listens on — exercises the :connect-failed dial failure.
(def ^:private dead-server-url "nats://127.0.0.1:1")

(defn- wait-for
  "Poll `pred` until truthy or `timeout-ms` elapses; return the last value."
  [pred timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (or (pred)
          (when (< (System/currentTimeMillis) deadline)
            (Thread/sleep 20)
            (recur))))))

;; Tracer bullet (AC2/AC3/AC5): the named synchronous path. `connect` blocks and
;; returns a Connection; `subscribe` returns a pull handle (no handler);
;; `take-message` blocks up to the timeout and returns the next decoded message;
;; `close` blocks until closed. SUB and the PUB share one connection, so the
;; server registers the subscription before the message arrives — no flush.
(deftest named-path-connect-subscribe-take-message-close
  (let [conn (nats/connect {:servers [server-url]})
        sub  (nats/subscribe conn subject)]
    (try
      (is (some? conn) "connect blocks and returns a Connection")
      (is (some? sub) "subscribe returns a pull handle synchronously")
      (nats/publish conn subject payload)
      (let [msg (nats/take-message sub 5000)]
        (is (= subject (:subject msg)) "take-message returns the message subject")
        (is (= payload (:data msg)) "take-message returns the EDN-decoded :data"))
      (finally (nats/close conn)))))

;; A failed one-shot throws the bare canonical `ex-info` — the same `:type` the
;; async core rejects with (AC4) — not a wrapping ExecutionException. A request to
;; a subject nobody subscribes rejects with :no-responders; the blocking layer
;; peels the CompletableFuture's wrappers so the ex-info surfaces directly.
(deftest request-no-responders-throws-canonical-type
  (let [conn (nats/connect {:servers [server-url]})]
    (try
      (is (= :no-responders
             (try (nats/request conn no-responders-subject {:ping 1})
                  :no-throw
                  (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
          "request to a no-responders subject throws ex-info :type :no-responders")
      (finally (nats/close conn)))))

;; The other failed one-shot (AC4): connect itself. Dialing a server nothing
;; listens on throws the bare :connect-failed ex-info — the same unwrap path,
;; proving it is `await!`-wide, not a per-op special case.
(deftest connect-failure-throws-canonical-type
  (is (= :connect-failed
         (try (nats/connect {:servers [dead-server-url]})
              :no-throw
              (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
      "connect to a dead server throws ex-info :type :connect-failed"))

;; take-message bounds its wait by the timeout and returns nil when none arrives
;; (AC3): a timeout on an empty buffer returns nil within roughly that window, and
;; a 0 timeout polls — returns nil at once without blocking.
(deftest take-message-times-out-and-polls
  (let [conn (nats/connect {:servers [server-url]})
        sub  (nats/subscribe conn "blocking.quiet")]
    (try
      (let [start (System/currentTimeMillis)
            msg   (nats/take-message sub 200)
            took  (- (System/currentTimeMillis) start)]
        (is (nil? msg) "take-message returns nil when no message arrives within the timeout")
        (is (< took 2000) "take-message returns at roughly the timeout, not indefinitely"))
      (is (nil? (nats/take-message sub 0)) "a 0 timeout polls and returns nil on an empty buffer")
      (finally (nats/close conn)))))

;; Abrupt teardown (AC7): unsubscribe stops the sub, clears the buffer, and
;; poisons it — so a take-message parked on the now-dead sub wakes and returns nil
;; rather than hanging. End-of-stream is sticky (every later take-message also
;; returns nil), and active? flips false once the sub has ended and the buffer is
;; drained — the disambiguator for take-message's nil-means-timeout-or-end.
(deftest unsubscribe-poisons-buffer-and-active?-tracks-end
  (let [conn   (nats/connect {:servers [server-url]})
        sub    (nats/subscribe conn "blocking.poison")
        parked (promise)
        _taker (future (deliver parked (nats/take-message sub)))]
    (try
      (is (nats/active? sub) "a live subscription is active")
      (Thread/sleep 100)
      (is (not (realized? parked)) "take-message parks on the empty buffer")
      (is (nil? (nats/unsubscribe sub)) "unsubscribe returns nil synchronously")
      (let [v (deref parked 2000 ::timeout)]
        (is (not= ::timeout v) "the parked take-message wakes on the poison pill")
        (is (nil? v) "...and returns nil for end-of-stream"))
      (is (nil? (nats/take-message sub 100)) "every later take-message also returns nil (poison is sticky)")
      (is (not (nats/active? sub)) "active? is false once the sub has ended and the buffer is drained")
      (finally (nats/close conn)))))

;; messages is the ergonomic path (AC6): an IReduceInit of decoded messages that
;; reduce/run! drain, terminating on its own at end-of-stream. A reducer consumes
;; concurrently while n messages are published; once it has them all it parks for
;; more, and the abrupt teardown's poison ends the reduce.
(deftest messages-reduces-decoded-and-terminates-on-teardown
  (let [conn    (nats/connect {:servers [server-url]})
        sub     (nats/subscribe conn "blocking.messages")
        seen    (atom [])
        reducer (future (reduce (fn [_ m] (swap! seen conj (:data m)) nil)
                                nil (nats/messages sub)))
        n       5]
    (try
      (dotimes [i n] (nats/publish conn "blocking.messages" i))
      (is (wait-for #(= n (count @seen)) 5000)
          "messages yields every published message, decoded, to reduce")
      (is (= (vec (range n)) @seen) "...in publish order")
      (nats/unsubscribe sub)
      (is (not= ::timeout (deref reducer 2000 ::timeout))
          "the reduce terminates once the sub is torn down")
      (finally (nats/close conn)))))

;; Unsubscribe is idempotent (ADR 0012): unsubscribing an already-ended pull sub
;; is a silent no-op returning nil, not an error — the inner sub swallows the
;; redundant teardown and the buffer stays poisoned.
(deftest unsubscribe-is-idempotent
  (let [conn (nats/connect {:servers [server-url]})
        sub  (nats/subscribe conn "blocking.idem")]
    (try
      (nats/unsubscribe sub)
      (is (nil? (nats/unsubscribe sub)) "unsubscribe after a prior unsubscribe is a no-op returning nil")
      (is (nil? (nats/take-message sub 100)) "the buffer stays poisoned across redundant unsubscribes")
      (finally (nats/close conn)))))

;; :capacity sizes the bounded buffer (default 1024). A non-positive value is
;; caller misuse the layer rejects synchronously with a portable :invalid-capacity
;; (AC7), before constructing the queue — parallel to the core's :invalid-max-pending.
(deftest invalid-capacity-rejected
  (let [conn (nats/connect {:servers [server-url]})]
    (try
      (is (= :invalid-capacity
             (try (nats/subscribe conn "blocking.cap" {:capacity 0})
                  :no-throw
                  (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
          ":capacity 0 is rejected as :invalid-capacity")
      (is (= :invalid-capacity
             (try (nats/subscribe conn "blocking.cap" {:capacity -5})
                  :no-throw
                  (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
          "a negative :capacity is rejected as :invalid-capacity")
      (finally (nats/close conn)))))

;; Overflow is backpressure, never drop (AC7, ADR 0008): a small :capacity with a
;; slow consumer fills the buffer and parks the feeding dispatcher on .put, yet
;; every message is delivered, in order. A robustness check — no message is lost
;; under a full buffer — not a timing assertion.
(deftest small-capacity-backpressures-without-loss
  (let [n    25
        conn (nats/connect {:servers [server-url]})
        sub  (nats/subscribe conn "blocking.overflow" {:capacity 2})]
    (try
      (dotimes [i n] (nats/publish conn "blocking.overflow" i))
      (let [received (loop [acc []]
                       (if (= n (count acc))
                         acc
                         (let [m (nats/take-message sub 5000)]
                           (Thread/sleep 3) ; consume slower than messages arrive
                           (recur (conj acc (:data m))))))]
        (is (= (vec (range n)) received)
            "every message reaches a full, small buffer in order — backpressure, never drop"))
      (finally (nats/close conn)))))

;; Abrupt teardown unblocks a producer parked on a full buffer (AC7). At :capacity
;; 1 the dispatcher parks on .put with more messages waiting in jnats' own queue;
;; unsubscribe must clear + poison, return synchronously (not hang), and let the
;; sub terminate — the poison is never lost to the teardown race, and no
;; dispatcher thread is leaked re-parking on the sentinel.
(deftest unsubscribe-unblocks-a-parked-producer
  (let [conn (nats/connect {:servers [server-url]})
        sub  (nats/subscribe conn "blocking.parked" {:capacity 1})]
    (try
      (dotimes [i 6] (nats/publish conn "blocking.parked" i))
      (Thread/sleep 300) ; let the dispatcher fill the 1-slot buffer and park on .put
      (let [done (future (nats/unsubscribe sub))]
        (is (not= ::timeout (deref done 2000 ::timeout))
            "unsubscribe returns promptly with a producer parked on the full buffer")
        (is (nil? @done) "...returning nil"))
      (let [drained (future (reduce (fn [c _] (inc c)) 0 (nats/messages sub)))]
        (is (not= ::timeout (deref drained 2000 ::timeout))
            "messages terminates after teardown — the poison survived the race, no hang"))
      (is (not (nats/active? sub)) "the sub is no longer active")
      (finally (nats/close conn)))))

;; Closing the connection poisons every pull sub (AC7, ADR 0008): a take-message
;; parked on any of the connection's pull subs wakes and returns nil rather than
;; hanging once the connection is gone. The blocking connect wraps :on-status so
;; the :closed event drives the poisoning.
(deftest close-poisons-every-pull-sub
  (let [conn     (nats/connect {:servers [server-url]})
        sub-a    (nats/subscribe conn "blocking.close.a")
        sub-b    (nats/subscribe conn "blocking.close.b")
        parked-a (future (nats/take-message sub-a))
        parked-b (future (nats/take-message sub-b))]
    (Thread/sleep 150)
    (is (and (not (realized? parked-a)) (not (realized? parked-b)))
        "both takers are parked before the connection closes")
    (nats/close conn)
    (is (not= ::timeout (deref parked-a 3000 ::timeout)) "closing the connection wakes sub-a's parked taker")
    (is (nil? @parked-a) "...returning nil for end-of-stream")
    (is (not= ::timeout (deref parked-b 3000 ::timeout)) "closing the connection wakes sub-b's parked taker")
    (is (nil? @parked-b) "...returning nil for end-of-stream")
    (is (and (not (nats/active? sub-a)) (not (nats/active? sub-b)))
        "every pull sub is inactive once the connection has closed")))

;; The subscribe→register race (review.md finding #3): if the connection's :closed
;; lands after core/subscribe makes the sub live but before it joins the registry,
;; the close sweep misses it and a later take-message parks forever. The fix folds a
;; :closed? flag into the registry atom so subscribe's post-register check
;; self-poisons. Simulate the window deterministically — flip :closed? before
;; subscribe registers — so the live sub must still end (active? false), not park.
(deftest subscribe-self-poisons-when-connection-closed-in-window
  (let [conn (nats/connect {:servers [server-url]})
        reg  (:nats-cljc.blocking.core/registry (meta conn))]
    (try
      (swap! reg assoc :closed? true)
      (let [sub (nats/subscribe conn "blocking.race")]
        (is (nil? (nats/take-message sub 200))
            "a sub that registers after :closed? is poisoned, not left to park")
        (is (not (nats/active? sub))
            "...and is inactive — the close sweep missed it but the self-poison caught it"))
      (finally (nats/close conn)))))

;; Connection drain poisons pull subs too (it closes the connection, firing the
;; same :closed event) — and the connection-level drain one-shot blocks and ends
;; the subs, the synchronous twin of core/drain over a connection.
(deftest drain-connection-poisons-pull-subs
  (let [conn   (nats/connect {:servers [server-url]})
        sub    (nats/subscribe conn "blocking.drain.conn")
        parked (future (nats/take-message sub))]
    (Thread/sleep 150)
    (is (not (realized? parked)) "the taker is parked before the connection drains")
    (is (nil? (nats/drain conn)) "draining a connection blocks and returns nil")
    (is (not= ::timeout (deref parked 3000 ::timeout)) "draining the connection wakes the parked taker")
    (is (nil? @parked) "...returning nil for end-of-stream")
    (is (not (nats/active? sub)) "the pull sub is inactive once the connection has drained")))

;; Graceful drain of a pull sub flushes its buffer (AC7): unlike abrupt
;; unsubscribe, every already-buffered message is delivered before end-of-stream.
;; The buffer holds all n (capacity is ample), so drain appends the sentinel after
;; them; a later reduce sees the full set, in order, then terminates.
(deftest drain-pull-sub-flushes-buffer-then-terminates
  (let [conn (nats/connect {:servers [server-url]})
        sub  (nats/subscribe conn "blocking.draining")
        n    5]
    (try
      (dotimes [i n] (nats/publish conn "blocking.draining" i))
      (Thread/sleep 300) ; let all n arrive in the buffer (drain flushes any straggler too)
      (is (nil? (nats/drain sub)) "draining a pull sub blocks and returns nil")
      (let [seen (reduce (fn [acc m] (conj acc (:data m))) [] (nats/messages sub))]
        (is (= (vec (range n)) seen) "drain flushes the buffered messages before ending — none dropped"))
      (is (not (nats/active? sub)) "active? is false once the drained buffer is consumed")
      (finally (nats/close conn)))))

;; request/reply round-trip through the blocking layer: a responder pulls the
;; request with take-message and answers with reply, while the requester blocks on
;; request for the decoded reply. Exercises reply (a re-exported one-shot) end to
;; end alongside the pull model.
(deftest request-reply-round-trip
  (let [conn      (nats/connect {:servers [server-url]})
        responder (nats/subscribe conn "blocking.rr")
        worker    (future
                    (let [msg (nats/take-message responder 5000)]
                      (nats/reply conn msg {:pong (:n (:data msg))})))]
    (try
      (let [reply (nats/request conn "blocking.rr" {:n 7})]
        (is (= {:pong 7} (:data reply))
            "request blocks and returns the decoded reply the responder sent"))
      @worker
      (finally (nats/close conn)))))

;; The remaining one-shot (flush) and the re-exported version: flush blocks until
;; the server has processed the buffer and returns nil; version mirrors the core.
(deftest flush-blocks-and-version-re-exported
  (let [conn (nats/connect {:servers [server-url]})]
    (try
      (nats/publish conn "blocking.flush" :x)
      (is (nil? (nats/flush conn)) "flush blocks and returns nil once the buffer is processed")
      (is (string? nats/version) "version is re-exported from the core")
      (finally (nats/close conn)))))
