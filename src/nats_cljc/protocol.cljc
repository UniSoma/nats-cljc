(ns nats-cljc.protocol
  "Internal protocol of primitive operations a platform Connection implements
   (ADR 0005). The public facade in `nats-cljc.core` sits on top of this and owns
   codec encode/decode; everything here deals in raw wire bytes.

   This slice defines only the two primitives the connect -> publish -> subscribe
   round-trip needs; later slices grow the protocol (request, unsubscribe, flush,
   drain, close, status).")

(defprotocol Conn
  "The primitive operations every platform Connection record implements."
  (-publish [conn subject bytes]
    "Publish raw `bytes` to `subject`. Fire-and-forget; return value unused.")
  (-subscribe [conn subject handler]
    "Subscribe to `subject`, returning a native Subscription synchronously.
     `handler` is the low-level handler, invoked per message with a raw map
     `{:subject <string> :bytes <platform-bytes>}`."))
