(ns nats-cljc.blocking.core
  "JVM-only blocking convenience layer over `nats-cljc.core` (ADR 0008): the same
   verb names, but synchronous. A caller switches semantics by swapping a single
   require. A plain `.clj` file (not a `.cljc` with reader conditionals), so
   shadow-cljs never compiles it — the pull API is JVM-only by construction and
   has no ClojureScript counterpart, which is the reason this layer exists.

   A pure consumer of the portable core: it owns no jnats interop and reaches no
   protocol primitive. One-shots block and unwrap the core's CompletableFuture so
   they throw the canonical `ex-info` directly; subscriptions get a pull model the
   portable push core structurally cannot offer."
  ;; `flush` is part of the public verb surface, shadowing clojure.core/flush.
  (:refer-clojure :exclude [flush])
  (:require [nats-cljc.core :as core])
  (:import [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit
            CompletableFuture ExecutionException CompletionException]))

(defn- await!
  "Block on a CompletableFuture from the async core and return its value. A
   rejection surfaces as the bare canonical `ex-info` the async core produced:
   `.get` reports it as an `ExecutionException`, so we peel that — and a residual
   `CompletionException` layer jnats may add — back to the cause it wraps (ADR 0008)."
  [^CompletableFuture cf]
  (try
    (.get cf)
    (catch ExecutionException e
      (let [cause (.getCause e)]
        (throw (if (instance? CompletionException cause) (or (.getCause cause) cause) cause))))))

;; End-of-stream sentinel: a unique object enqueued on teardown to wake a parked
;; `take-message`. Distinct by identity from any decoded message (always a map),
;; so `take-message` tells it apart unambiguously.
(def ^:private poison (Object.))

;; A pull handle (CONTEXT: Pull subscription): the inner async subscription, the
;; bounded queue its enqueuing handler feeds, an `ended` flag set on teardown, the
;; connection's pull-sub `registry` (so a connection close can poison it; nil when
;; the connection was not opened through this layer), and a `counter` atom holding
;; `{:received n :max m}` — `:received` is the lifetime arrival count (every message
;; that lands in the buffer), `:max` the armed auto-unsubscribe threshold (nil until
;; `(unsubscribe sub max)` arms it). `take-message`/`messages` drain the queue; the
;; teardown verbs act on the inner sub and poison the queue.
(defrecord PullSubscription [inner ^BlockingQueue queue ended registry counter])

(defn- poison-tail!
  "Seat the end-of-stream sentinel at the tail without blocking: it lands after the
   already-buffered messages, but on a full buffer evict the oldest (head) to make
   room rather than block. Bounded — the bounded-wait enqueuing handler stops the
   producer once `ended` is set, so at most one in-flight message races back in and
   this converges (ADR 0013)."
  [^BlockingQueue queue]
  (while (not (.offer queue poison))
    (.poll queue)))

(defn- poison!
  "Abrupt teardown: drop any buffered messages, then seat the end-of-stream
   sentinel. The leading `.clear` is the whole difference from graceful
   `poison-tail!` — the backlog is discarded, not delivered (a producer racing one
   last message back in after the clear is dropped the same way)."
  [^BlockingQueue queue]
  (.clear queue)
  (poison-tail! queue))

(defn- end-sub!
  "Mark a pull sub ended and poison its buffer to wake any parked `take-message`,
   without touching the inner sub — used when the connection itself is closing
   (its subscriptions are already ending)."
  [sub]
  (reset! (:ended sub) true)
  (poison! (:queue sub)))

(defn- end-after-max!
  "Graceful auto-end when a sub reaches its armed `max`: unlike abrupt
   `unsubscribe`, the N already-buffered messages should still reach the consumer,
   so this does NOT clear the buffer — `poison-tail!` seats the end-of-stream
   sentinel after them. On a full buffer it cannot both block and stay synchronous
   (ADR 0008/0012 require `unsubscribe` to return now), so it evicts the oldest to
   seat the sentinel rather than hang; exactly-N reaches the consumer only when
   `:capacity` > `max` (ADR 0013). The native inner sub has already been told to
   stop at N, so only the blocking handle is finalized. CAS-guarded on `ended` so it
   fires exactly once even if the enqueuing handler and `unsubscribe` both observe N
   reached (and the seat is skipped once another teardown has already ended the sub)."
  [sub]
  (when (compare-and-set! (:ended sub) false true)
    (some-> (:registry sub) (swap! update :subs disj sub))
    (poison-tail! (:queue sub))))

(defn connect
  "Open a connection to a NATS server and block until it is ready.

   `opts` keys:

   | key          | type                    | default       | effect |
   |--------------|-------------------------|---------------|--------|
   | `:servers`   | string or vector        | required      | Server URL(s): `nats://...` on the JVM. |
   | `:codec`     | keyword or `ICodec`     | `:edn`        | Connection default codec. |
   | `:name`      | string                  | none          | Connection label shown in server monitoring. |
   | `:auth`      | credentials map         | anonymous     | One of `{:token ...}`, `{:user ... :pass ...}`, `{:nkey ... :seed ...}`, `{:jwt ... :seed ...}`, or `{:creds ...}`. |
   | `:reconnect` | map                     | native client | `{:max <int> :wait-ms <ms> :jitter-ms <ms>}`; `0` disables, `-1` means unlimited. |
   | `:on-status` | 1-arg fn                | none          | Receives lifecycle maps such as `{:type :connected}` or `{:type :error :error e}`. |

   Returns the Connection directly, not a promise. Closing or draining it poisons
   every pull subscription opened through this namespace, waking blocked
   `take-message`/`messages` consumers (ADR 0008).

   ```clojure
   (def conn (connect {:servers [\"nats://localhost:4222\"]}))
   ```

   Throws the same `ex-info` failures that the async connect promise rejects with:
   `:type :connect-failed`, `:auth-invalid`, or `:codec-error`."
  [opts]
  (let [;; `:closed?` rides in the registry atom (not a separate atom) so marking
        ;; the connection closed and snapshotting the subs to poison is one atomic
        ;; step — this closes the subscribe→register race (a sub going live in that
        ;; window is poisoned by whichever side wins the CAS; see `subscribe`).
        registry (atom {:closed? false :subs #{}})
        opts (update opts :on-status
               (fn [user-on-status]
                 (fn [status]
                   (when (= :closed (:type status))
                     (run! end-sub! (:subs (swap! registry assoc :closed? true))))
                   (when user-on-status (user-on-status status)))))]
    ;; Stash the registry in the connection's metadata rather than a side table:
    ;; the record stays the same type (so the core's protocol dispatch and the
    ;; plain `publish`/`reply` aliases keep working on it) and it is GC'd with the
    ;; connection — no global map to leak.
    (with-meta (await! (core/connect opts)) {::registry registry})))

(def publish
  "Publish `data` to `subject` on `conn` and return nil.

   - `conn` — a Connection.
   - `subject` — destination subject string.
   - `data` — any value the effective codec can encode.
   - `opts` (optional map): `:codec` is a keyword or `ICodec`, default connection
     codec; `:headers` is a map, default none.

   Throws synchronously with the same canonical `:type`s as core publish:
   `:invalid-header`, `:codec-error`, `:max-payload-exceeded`, or
   `:connection-closed`. A publish during drain is best-effort and returns nil."
  core/publish)

(def version
  "The library's version string, e.g. `\"0.5.0\"`.

   A plain value, not a function; `nats-cljc.blocking.core/version` evaluates to
   the string directly and never throws."
  core/version)

(def reply
  "Reply to request message `msg` with `data` on `conn`, returning nil.

   `msg` is a message returned by `take-message` or delivered by a core handler;
   its `:reply` key names the subject to answer.

   `opts` (optional map): `:codec` is a keyword or `ICodec`, default connection
   codec, and applies to this reply only.

   Throws synchronously with `:type :no-reply-subject` when `msg` has no `:reply`,
   `:codec-error` when encoding fails, `:max-payload-exceeded` when the encoded
   payload is too large, or `:connection-closed` when `conn` is closed."
  core/reply)

(defn request
  "Send a request to `subject` on `conn`, block for the decoded reply, and return
   the reply message map `{:subject :data :reply}`.

   - `conn` — a Connection.
   - `subject` — request subject string.
   - `data` — any value the effective codec can encode.
   - `opts` (optional map, default `{}`): `:codec` (keyword or `ICodec`, default
     connection codec) controls request encode and reply decode; `:timeout-ms`
     (integer milliseconds, default `5000`) bounds the wait.

   ```clojure
   (:data (request conn \"greet.bob\" {:hello \"world\"} {:timeout-ms 1000}))
   ```

   Throws the bare canonical `ex-info` the async core promise would reject with:
   `:type :no-responders`, `:timeout`, `:codec-error`,
   `:max-payload-exceeded`, `:drained`, or `:connection-closed`."
  ([conn subject data] (request conn subject data {}))
  ([conn subject data opts] (await! (core/request conn subject data opts))))

(defn flush
  "Flush `conn`, blocking until the server has processed everything buffered on
   the connection.

   `conn` is a Connection. Returns nil. Failures are the native client's flush
   errors passed through by [[nats-cljc.core/flush]]; they are not normalized to a
   canonical `:type`."
  [conn]
  (await! (core/flush conn)))

(defn subscribe
  "Subscribe to `subject` on `conn`, returning an opaque pull handle.

   Unlike core subscribe, there is no handler argument. Each matching message is
   decoded and enqueued on a bounded buffer drained by [[take-message]] or
   [[messages]].

   `opts` (optional map):

   | key            | type                | default         | effect |
   |----------------|---------------------|-----------------|--------|
   | `:capacity`    | positive int        | `1024`          | Buffer size. A full buffer blocks the feeding dispatcher; there is no unbounded mode. |
   | `:codec`       | keyword or `ICodec` | conn's codec    | Codec for decoding deliveries. |
   | `:queue`       | string              | none            | Queue-group name. |
   | `:on-error`    | 1-arg fn            | none            | Async-failure sink, as in [[nats-cljc.core/subscribe]]. |
   | `:max-pending` | positive int        | native default  | Slow-consumer threshold, armed only with `:on-error`. |

   Returns a `PullSubscription` handle. Pass it to [[take-message]], [[messages]],
   [[unsubscribe]], [[drain]], or [[active?]].

   ```clojure
   (def sub (subscribe conn \"events.>\" {:capacity 256}))
   (:data (take-message sub 5000))   ; blocks up to 5s; nil on timeout
   ```

   Throws synchronously with `:type :invalid-capacity` when `:capacity` is not a
   positive integer, and `:invalid-max-pending` when `:max-pending` is invalid.
   Decode, handler, and slow-consumer failures route through `:on-error` or the
   connection `:on-status`, as in core subscribe.

   Keep `:capacity` greater than any `max` armed via [[unsubscribe]] if the
   consumer must see all N auto-unsubscribe messages; otherwise a full buffer may
   evict the oldest message to keep teardown non-blocking (ADR 0013)."
  ([conn subject] (subscribe conn subject {}))
  ([conn subject {:keys [capacity] :or {capacity 1024} :as opts}]
   ;; Fail fast on caller misuse before constructing the queue: ArrayBlockingQueue
   ;; rejects a non-positive capacity with a bare IllegalArgumentException; surface
   ;; the portable canonical type instead (parallel to the core's :max-pending).
    (when-not (pos-int? capacity)
      (throw (ex-info "subscribe :capacity must be a positive integer"
               {:type :invalid-capacity :capacity capacity})))
    (let [^BlockingQueue queue (ArrayBlockingQueue. capacity)
          ended (atom false)
          counter (atom {:received 0 :max nil})
         ;; The enqueuing handler reaches the sub it feeds only through this promise:
         ;; the record is built from `inner`, `inner` from the handler — a cycle a
         ;; forward reference breaks. Forced only once `:max` is armed (long after
         ;; the sub is delivered below), so it never actually blocks.
          self (promise)
         ;; Block for buffer space — backpressure, never drop — but bounded, so a
         ;; producer parked on a full buffer rechecks `ended` and bails on teardown
         ;; rather than leaking the dispatcher thread (e.g. re-parking forever on a
         ;; sole poison sentinel at :capacity 1). While live this is a plain block.
         ;; A successful offer is one lifetime arrival; reaching an armed `:max`
         ;; gracefully ends the sub (the N stay buffered for the consumer).
          enqueue (fn [msg]
                    (loop []
                      (when-not @ended
                        (if (.offer queue msg 100 TimeUnit/MILLISECONDS)
                          (let [{:keys [received max]} (swap! counter update :received inc)]
                            (when (and max (>= received max))
                              (end-after-max! @self)))
                          (recur)))))
          registry (::registry (meta conn))
          inner (core/subscribe conn subject enqueue opts)
          sub (->PullSubscription inner queue ended registry counter)]
      (deliver self sub)
     ;; Register, then self-poison if the connection closed during the
     ;; subscribe→register window: the close sweep and this conj hit the same atom,
     ;; so the CAS orders them — exactly one side ends the sub, never neither.
      (when-let [st (some-> registry (swap! update :subs conj sub))]
        (when (:closed? st) (end-sub! sub)))
      sub)))

(defn- pulled
  "Translate a raw dequeued value into the `take-message` result: the poison
   sentinel becomes nil (end-of-stream) and is re-offered so it stays sticky for
   the next taker; a message or a nil (timeout/empty) passes through unchanged."
  [^BlockingQueue queue v]
  (if (identical? v poison)
    (do (.offer queue poison) nil)
    v))

(defn take-message
  "Pull the next decoded message from pull subscription `sub`.

   With one arg, blocks until a message or end-of-stream arrives. With
   `timeout-ms` (integer milliseconds, `0` means poll), blocks at most that long.

   Returns the decoded message map `{:subject :data :reply}` with optional
   `:headers`, or nil for either timeout or end-of-stream. Use [[active?]] to
   distinguish those cases. Does not throw under normal operation; interruption or
   queue failures are JVM exceptions passed through unchanged."
  ([sub]
    (let [^BlockingQueue queue (:queue sub)]
      (pulled queue (.take queue))))
  ([sub timeout-ms]
    (let [^BlockingQueue queue (:queue sub)]
      (pulled queue (.poll queue timeout-ms TimeUnit/MILLISECONDS)))))

(defn messages
  "Return a reducible view of pull subscription `sub`'s decoded messages.

   The returned `IReduceInit` pulls each item with [[take-message]] and stops at
   end-of-stream. Use `reduce` or `run!`; do not use a bare `doseq`.

   Returns the reducible immediately. It has no options. Failures while reducing
   are the same JVM failures [[take-message]] can surface, plus any exception
   thrown by the reducing function."
  [sub]
  (reify clojure.lang.IReduceInit
    (reduce [_ f init]
      (loop [acc init]
        (let [m (take-message sub)]
          (if (nil? m)
            acc
            (let [acc' (f acc m)]
              (if (reduced? acc') @acc' (recur acc')))))))))

(defn active?
  "Return true while pull subscription `sub` is still active.

   Active means the subscription has not ended, or it has ended but its buffer
   still holds undelivered messages. Returns false after teardown and buffer drain.
   Takes no options and does not throw under normal operation."
  [sub]
  (or (not @(:ended sub))
    (let [head (.peek ^BlockingQueue (:queue sub))]
      (boolean (and (some? head) (not (identical? head poison)))))))

(defn unsubscribe
  "End pull subscription `sub`, returning nil synchronously.

   With one arg, teardown is abrupt: the inner subscription stops, buffered
   messages are dropped, and blocked `take-message` callers wake. The call is
   idempotent.

   With `max` (positive integer `<= 2147483647`), arm lifetime auto-unsubscribe:
   the sub ends after `max` messages have arrived. Already-buffered messages are
   delivered before end-of-stream, guaranteed when `:capacity` is greater than
   `max`; with a full smaller buffer, oldest messages may be evicted to keep
   teardown non-blocking (ADR 0013).

   Throws synchronously with `:type :invalid-max` when `max` is outside the allowed
   range. Otherwise returns nil."
  ([sub]
    (core/unsubscribe (:inner sub))
    (reset! (:ended sub) true)
    (poison! (:queue sub))
    (some-> (:registry sub) (swap! update :subs disj sub))
    nil)
  ([sub max]
    (when-not (and (pos-int? max) (<= max 2147483647))
      (throw (ex-info "unsubscribe max must be a positive integer no greater than 2147483647"
               {:type :invalid-max :max max})))
   ;; Stop the server at N (native lifetime auto-unsubscribe), then arm the buffer's
   ;; threshold so the enqueuing handler poisons gracefully at the Nth arrival. Arm
   ;; `:max` and read `:received` in one swap so a concurrent arrival can't slip past
   ;; both checks: whichever of this call and the handler observes received >= max
   ;; ends the sub (end-after-max! is fire-once). If N already arrived, end now.
    (core/unsubscribe (:inner sub) max)
    (let [{:keys [received]} (swap! (:counter sub) assoc :max max)]
      (when (>= received max)
        (end-after-max! sub)))
    nil))

(defn drain
  "Drain `conn-or-sub` and block until draining completes.

   `conn-or-sub` is either a Connection or a `PullSubscription`. Returns nil.

   For a pull subscription, drain is graceful: buffered messages are flushed before
   end-of-stream instead of dropped. Flushing requires a concurrent consumer unless
   the whole backlog fits below `:capacity`; a single-threaded `drain`-then-`consume`
   can deadlock once the backlog reaches capacity. For a Connection, drain stops
   and closes it, poisoning every pull subscription opened through this namespace.

   Connection-drain failures are the native errors passed through by
   [[nats-cljc.core/drain]]; they are not normalized to canonical `:type`s. Pull-sub
   drain can also surface those native subscription-drain failures or JVM blocking
   exceptions unchanged."
  [conn-or-sub]
  (if (instance? PullSubscription conn-or-sub)
    (let [sub conn-or-sub]
      ;; Flush pending into the buffer and end the inner sub, THEN poison at the
      ;; tail — after the flushed messages, so the buffer drains in order. ended is
      ;; set only now (not before the await), so the enqueuing handler keeps
      ;; flushing rather than bailing. Needs a concurrent consumer unless the whole
      ;; backlog fits below capacity: a backlog == capacity blocks here at `.put`,
      ;; and a backlog > capacity blocks even earlier (the enqueue handler pins the
      ;; dispatcher on the C+1-th message) — so the boundary is "reaches", not
      ;; "exceeds", capacity.
      (await! (core/drain (:inner sub)))
      (reset! (:ended sub) true)
      (some-> (:registry sub) (swap! update :subs disj sub))
      (.put ^BlockingQueue (:queue sub) poison)
      nil)
    ;; core/drain resolves to jnats' Boolean; normalize to nil so every blocking
    ;; teardown verb returns nil uniformly.
    (do (await! (core/drain conn-or-sub)) nil)))

(defn close
  "Close Connection `conn` and block until it is fully closed.

   Returns nil. Closing ends and poisons every pull subscription opened through
   this namespace on the connection. Failures are the native close failures passed
   through by [[nats-cljc.core/close]]; closing an already-closed connection is
   benign on both legs of the core contract."
  [conn]
  (await! (core/close conn)))
