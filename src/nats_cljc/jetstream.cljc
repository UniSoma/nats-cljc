(ns nats-cljc.jetstream
  "Portable public facade for JetStream (aliased `jet` — CLJS reserves `js` for
   host interop), the durable half of NATS. A thin `.cljc` surface mirroring
   `nats-cljc.core`: the same consumer code compiles and runs on the JVM, the
   browser, and Node.

   Requiring this namespace loads the per-leg JetStream impl — and, on CLJS, pulls
   the `@nats-io/jetstream` bundle bytes — which a core-only consumer who never
   requires it does not pay for (ADR 0016). The impl require is for that load
   side-effect only (it `extend`s the JetStream protocol onto the platform
   Connection record); this facade calls the record through the protocol."
  (:require [nats-cljc.protocol :as proto]
            #?(:clj  [nats-cljc.jetstream.impl.jvm]
               :cljs [nats-cljc.jetstream.impl.js])))

(defn jetstream
  "Obtain the JetStream context for `conn`, returning a platform-native promise
   (CompletableFuture on the JVM, js/Promise on CLJS) that resolves to a single
   context holding both the data plane (publish, pull) and the management plane
   (stream/consumer admin) — every JetStream operation flows through it (ADR 0017).

   Obtaining it verifies JetStream is enabled by forcing a JS-info round-trip on
   both legs, so the promise rejects with an `ex-info` `:type :jetstream-not-enabled`
   (err 10039) when the server/account has JetStream disabled — at the handle,
   never deferred to the first operation (ADR 0017/0020)."
  [conn]
  (proto/-jetstream conn))
