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
;; bounded queue its enqueuing handler feeds, an `ended` flag set on teardown, and
;; the connection's pull-sub `registry` (so a connection close can poison it; nil
;; when the connection was not opened through this layer). `take-message`/`messages`
;; drain the queue; the teardown verbs act on the inner sub and poison the queue.
(defrecord PullSubscription [inner ^BlockingQueue queue ended registry])

(defn- poison!
  "Drop any buffered messages and make the end-of-stream sentinel land at the
   tail, even if the feeding producer races one last in-flight message back in
   after the clear (abrupt teardown drops it). The bounded-wait enqueuing handler
   stops the producer once `ended` is set, so at most one such message arrives and
   this converges."
  [^BlockingQueue queue]
  (.clear queue)
  (while (not (.offer queue poison))
    (.poll queue)))

(defn- end-sub!
  "Mark a pull sub ended and poison its buffer to wake any parked `take-message`,
   without touching the inner sub — used when the connection itself is closing
   (its subscriptions are already ending)."
  [sub]
  (reset! (:ended sub) true)
  (poison! (:queue sub)))

(defn connect
  "Open a connection (blocking): the synchronous twin of `core/connect`. Returns
   the Connection directly rather than a promise of it. Closing or draining the
   returned connection poisons every pull sub opened on it, so their parked
   `take-message`/`messages` consumers wake (ADR 0008); this is wired by wrapping
   the caller's `:on-status` to act on the `:closed` event."
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
  "Publish `data` to `subject` — already synchronous, re-exported unchanged."
  core/publish)

(def version
  "Current library version — re-exported unchanged."
  core/version)

(def reply
  "Reply to a request message — already synchronous, re-exported unchanged."
  core/reply)

(defn request
  "Send a request and block for the decoded reply (the synchronous twin of
   `core/request`). On failure throws the bare canonical `ex-info` — `:no-responders`
   or `:timeout` — the same `:type` the async core's promise rejects with. The
   3-arity defaults `opts` to `{}`."
  ([conn subject data] (request conn subject data {}))
  ([conn subject data opts] (await! (core/request conn subject data opts))))

(defn flush
  "Flush `conn` (blocking): returns once the server has processed everything
   buffered on the connection."
  [conn]
  (await! (core/flush conn)))

(defn subscribe
  "Subscribe to `subject`, returning an opaque pull handle (no handler arg). Each
   matching message is decoded with the connection's codec and enqueued on a
   bounded buffer that `take-message` drains one at a time. `opts` may set
   `:capacity` (the buffer's bound, default 1024; a non-positive value throws a
   `:type :invalid-capacity` ex-info — there is no unbounded escape hatch), plus
   the core's `:codec`/`:queue`/`:on-error`/`:max-pending`, passed through
   unchanged. A full buffer blocks the feeding dispatcher — backpressure, never
   drop (ADR 0008)."
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
         ;; Block for buffer space — backpressure, never drop — but bounded, so a
         ;; producer parked on a full buffer rechecks `ended` and bails on teardown
         ;; rather than leaking the dispatcher thread (e.g. re-parking forever on a
         ;; sole poison sentinel at :capacity 1). While live this is a plain block.
         enqueue (fn [msg]
                   (loop []
                     (when-not @ended
                       (when-not (.offer queue msg 100 TimeUnit/MILLISECONDS)
                         (recur)))))
         registry (::registry (meta conn))
         inner (core/subscribe conn subject enqueue opts)
         sub (->PullSubscription inner queue ended registry)]
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
  "Pull the next message off `sub`'s buffer, blocking the calling thread. With
   `timeout-ms`, block at most that long (0 = poll) and return nil on timeout.
   Returns the decoded `{:subject :data :reply (:headers)}` map, or nil for both a
   timeout and end-of-stream — disambiguate with `active?`."
  ([sub]
   (let [^BlockingQueue queue (:queue sub)]
     (pulled queue (.take queue))))
  ([sub timeout-ms]
   (let [^BlockingQueue queue (:queue sub)]
     (pulled queue (.poll queue timeout-ms TimeUnit/MILLISECONDS)))))

(defn messages
  "A reducible (IReduceInit) view of `sub`'s decoded messages: `reduce`/`run!`
   over it pulls each message with `take-message` and terminates on its own at
   end-of-stream (the sub's teardown). Use `reduce`/`run!`, not a bare `doseq`."
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
  "True while `sub` is still delivering: either it has not ended, or its buffer
   still holds undelivered messages. False once the sub has ended AND the buffer
   is drained (the poison sentinel sits alone at the tail)."
  [sub]
  (or (not @(:ended sub))
      (let [head (.peek ^BlockingQueue (:queue sub))]
        (boolean (and (some? head) (not (identical? head poison)))))))

(defn unsubscribe
  "End `sub` abruptly, returning nil synchronously (the lower-level sibling of
   `drain`): stop the inner subscription, drop any not-yet-taken messages, and
   poison the buffer so a parked `take-message` wakes and a parked producer
   unblocks. Idempotent.

   Only the `[sub]` arity exists here: the async core's `[sub max]`
   auto-unsubscribe-after-N is not yet supported in the pull model. Wiring it needs
   the buffer poisoned when the inner sub reaches N (a bare
   `(core/unsubscribe (:inner sub) max)` would leave the buffer un-poisoned and hang
   the next `take-message`); tracked as a follow-up."
  [sub]
  (core/unsubscribe (:inner sub))
  (reset! (:ended sub) true)
  (poison! (:queue sub))
  (some-> (:registry sub) (swap! update :subs disj sub))
  nil)

(defn drain
  "Drain a connection or a single pull sub (blocking), returning nil once draining
   completes. Draining a pull sub is graceful — the lower-bound sibling of
   `unsubscribe`: it flushes the already-buffered messages before ending, rather
   than dropping them. Flushing requires a concurrent consumer (another thread in
   `take-message`/`messages`) unless the whole backlog fits below `:capacity`; with
   the buffer at or above capacity the call blocks until a consumer makes room, so a
   single-threaded \"drain then consume\" deadlocks once the backlog reaches
   capacity. Draining a connection stops and closes it, which poisons every pull sub
   opened on it."
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
  "Close `conn` (blocking): returns once the connection is fully closed. Ends —
   and poisons — every pull sub opened on the connection."
  [conn]
  (await! (core/close conn)))
