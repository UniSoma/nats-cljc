(ns nats-cljc.protocol
  "Internal protocol of primitive operations a platform Connection implements
   (ADR 0005). The public facade in `nats-cljc.core` sits on top of this and owns
   codec encode/decode; everything here deals in raw wire bytes.

   The lifecycle slice adds -flush/-close (and -drain, which the facade also
   applies to a native Subscription); the request/reply slice adds -request;
   later slices grow it further (unsubscribe, status)."
  ;; -flush would otherwise shadow cljs.core/-flush (IWriter); no clojure.core
  ;; var by that name exists, so the exclude is a no-op on the JVM.
  (:refer-clojure :exclude [-flush]))

(defprotocol Conn
  "The primitive operations every platform Connection record implements."
  (-publish [conn subject bytes]
    "Publish raw `bytes` to `subject`. Fire-and-forget; return value unused.")
  (-subscribe [conn subject handler]
    "Subscribe to `subject`, returning a native Subscription synchronously.
     `handler` is the low-level handler, invoked per message with a raw map
     `{:subject <string> :bytes <platform-bytes> :reply <string-or-nil>}`,
     where `:reply` is the message's reply-to subject (nil when absent).")
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
  (-drain [conn]
    "Drain the connection — stop its subscriptions after their pending messages
     are delivered, flush, then close — returning a native promise that settles
     once draining completes.")
  (-close [conn]
    "Close the connection, returning a native promise that settles once it is
     fully closed. Ends all of the connection's subscriptions."))
