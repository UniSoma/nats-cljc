(ns nats-cljc.protocol
  "Internal protocols (ADR 0005) the platform records implement; the public facade
   in `nats-cljc.core` sits on top and owns codec encode/decode, so everything
   here deals in raw wire bytes.

   - `Conn` — the primitive operations a platform Connection implements.
   - `Drainable` — draining, supported by a connection AND a single subscription,
     so the facade's `drain` dispatches uniformly over either.
   - `Sub` — a subscription's own lifecycle predicate.

   The lifecycle slice added -flush/-close/-drain; request/reply added -request;
   the queue-groups slice gave `-subscribe` a queue arg, returned a Subscription
   record (not the native handle) carrying `Drainable`/`Sub`, and split -drain out
   of `Conn` into `Drainable`; later slices grow it further (unsubscribe, status)."
  ;; -flush would otherwise shadow cljs.core/-flush (IWriter); no clojure.core
  ;; var by that name exists, so the exclude is a no-op on the JVM.
  (:refer-clojure :exclude [-flush]))

(defprotocol Conn
  "The primitive operations every platform Connection record implements."
  (-publish [conn subject headers bytes]
    "Publish raw `bytes` to `subject`. Fire-and-forget; return value unused.
     `headers` is the canonical portable header map `{name -> vector-of-strings}`
     (case-sensitive string names) the facade normalizes to, or nil for none.")
  (-subscribe [conn subject queue opts handler]
    "Subscribe to `subject`, returning a Subscription record synchronously. When
     `queue` is a non-nil group name the subscription joins that queue group and
     the server load-balances each matching message to exactly one member; nil is
     a plain subscription that receives every matching message. `handler` is the
     low-level handler, invoked per message with a raw map
     `{:subject <string> :bytes <platform-bytes> :reply <string-or-nil>}`,
     where `:reply` is the message's reply-to subject (nil when absent).
     `opts` is `{:on-error <fn-or-nil> :max-pending <int-or-nil>}` (ADR 0006/0007):
     `:on-error` is a 1-arg sink for this subscription's async failures (a thrown
     handler value, a decode failure, or `:slow-consumer`); when absent, handler
     and decode failures fall back to the connection's `:on-status` `:error` event
     and `:slow-consumer` is dropped. `:max-pending` is a message-count threshold
     above which `:slow-consumer` fires.")
  (-request [conn subject bytes timeout-ms]
    "Send a request: publish raw `bytes` to `subject` with a managed reply inbox,
     returning a native promise of the raw reply map
     `{:subject <string> :bytes <platform-bytes> :reply <string-or-nil>}`. The
     promise rejects with an ex-info carrying `:type :no-responders` (nobody
     subscribes `subject`) or `:type :timeout` (responders exist but none answer
     within `timeout-ms`) (ADR 0002/0006).")
  (-flush [conn]
    "Flush pending writes, returning a native promise that settles once the
     server has processed everything buffered on the connection.")
  (-close [conn]
    "Close the connection, returning a native promise that settles once it is
     fully closed. Ends all of the connection's subscriptions."))

(defprotocol Drainable
  "Draining, supported by both a connection and a single subscription, so the
   facade's `drain` dispatches uniformly over either."
  (-drain [x]
    "Drain `x`, returning a native promise that settles once draining completes.
     A connection stops its subscriptions after their pending messages are
     delivered, flushes, then closes; a subscription ends just itself, leaving
     the connection open."))

(defprotocol Sub
  "A single subscription's lifecycle, beyond draining."
  (-active? [sub]
    "True while the subscription is still delivering — not yet drained,
     unsubscribed, or ended by the connection closing."))
