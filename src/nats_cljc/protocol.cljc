(ns ^:no-doc nats-cljc.protocol
  "Internal protocols (ADR 0005) the platform records implement; the public facade
   in `nats-cljc.core` sits on top and owns codec encode/decode, so everything
   here deals in raw wire bytes.

   - `Conn` — the primitive operations a platform Connection implements.
   - `Drainable` — draining, supported by a connection AND a single subscription,
     so the facade's `drain` dispatches uniformly over either.
   - `Sub` — a subscription's own lifecycle predicate.
   - `JetStream` — vends the JetStream context (ADR 0017). Unlike the others, the
     platform Connection records do NOT implement it inline; the JetStream impl
     namespaces `extend` it onto them, so the `@nats-io/jetstream` import stays out
     of a core-only CLJS bundle (ADR 0016).

   The lifecycle slice added -flush/-close/-drain; request/reply added -request;
   the queue-groups slice gave `-subscribe` a queue arg, returned a Subscription
   record (not the native handle) carrying `Drainable`/`Sub`, and split -drain out
   of `Conn` into `Drainable`; the unsubscribe slice added `-unsubscribe` to `Sub`;
   later slices grow it further (status)."
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
     unsubscribed, or ended by the connection closing.")
  (-unsubscribe [sub max]
    "End this subscription abruptly, the lower-level sibling of `-drain`: tell the
     server to stop and drop any not-yet-delivered messages, returning nil
     synchronously. `max` nil unsubscribes now; a positive int auto-unsubscribes
     once the subscription has received that many messages over its lifetime
     (counted from subscription start). Idempotent: unsubscribing an
     already-ended subscription is a silent no-op (ADR 0012). The facade owns the
     arities and validates `max`."))

(defprotocol JetStream
  "Vends the JetStream context (ADR 0017), the single async handle every JetStream
   operation flows through. Defined here so the pure protocol lives in the
   core-reachable namespace, but EXTENDED onto each platform's Connection record
   from the JetStream impl namespaces (`nats-cljc.jetstream.impl.*`), never
   implemented inline on the record — so the `@nats-io/jetstream` import stays out
   of a core-only CLJS bundle (ADR 0016)."
  (-jetstream [conn]
    "Return a native promise of a JetStream context — a platform record holding
     both the data plane (publish, pull) and the management plane (stream/consumer
     admin), which the native clients split but the portable surface collapses into
     one. Obtaining it verifies JetStream is enabled by forcing a JS-info
     round-trip on both legs (native on CLJS, added inside the off-thread wrap on
     the JVM), so the promise rejects with `:type :jetstream-not-enabled` (err
     10039) at the handle when it is not — never deferred to the first operation
     (ADR 0017/0020)."))

(defprotocol StreamManager
  "JetStream stream management (ADR 0017), the management-plane verbs — EXTENDED
   onto each platform's JetStream context record from the impl namespaces, never
   implemented inline, so the `@nats-io/jetstream` import stays confined (ADR 0016).
   The facade owns the public arglists and the pre-flight validation; these deal in
   the portable closed kebab config map and the normalized info map, translating
   to/from each leg's native config inside the impl (ADR 0020)."
  (-create-stream [ctx config]
    "Create a Stream from the portable closed kebab `config` (already validated by
     the facade), returning a native promise of the normalized StreamInfo map. The
     promise rejects with an operational `:jetstream-api-error` (carrying
     `{:code :description}`) when the server rejects the config (ADR 0020).")
  (-stream-info [ctx name]
    "Return a native promise of the normalized StreamInfo map for the Stream
     `name`, rejecting with `:type :stream-not-found` when it does not exist.")
  (-delete-stream [ctx name]
    "Delete the Stream `name`, returning a native promise that resolves to nil once
     it is gone and rejects with `:type :stream-not-found` when it does not exist."))
