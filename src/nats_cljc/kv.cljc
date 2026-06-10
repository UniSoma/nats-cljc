(ns nats-cljc.kv
  "Portable public facade for NATS Key/Value, the last-value registry built on
   JetStream. A thin `.cljc` surface mirroring `nats-cljc.jetstream`: the same
   consumer code compiles and runs on the JVM, the browser, and Node — and it
   speaks KV vocabulary throughout, never the stream substrate (ADR 0023):
   Buckets, not streams; `:bucket-not-found`, not `:stream-not-found`.

   Requiring this namespace loads the per-leg KV impl — and, on CLJS, pulls the
   `@nats-io/kv` bundle bytes — which a core-only consumer who never requires it
   does not pay for (ADR 0016). The impl require is for that load side-effect only
   (it `extend`s the KV protocol onto the platform Connection record); this facade
   calls the record through the protocol."
  (:require [nats-cljc.impl.protocol :as proto]
            [nats-cljc.kv.impl.bucket :as bucket]
            #?(:clj  [nats-cljc.impl.jvm :as impl]
               :cljs [nats-cljc.impl.js :as impl])
            #?(:clj  [nats-cljc.kv.impl.jvm]
               :cljs [nats-cljc.kv.impl.js])))

(defn kv
  "Obtain the KV context for `conn`, returning a platform-native promise
   (CompletableFuture on the JVM, js/Promise on CLJS) that resolves to the single
   context every Bucket-lifecycle operation flows through — the KV twin of
   `(jetstream conn)` (ADR 0017).

   KV is JetStream-backed, so obtaining it verifies JetStream is enabled by
   forcing a JS-info round-trip on both legs: the promise rejects with an
   `ex-info` `:type :jetstream-not-enabled` (err 10039) when the server/account
   has JetStream disabled — at the handle, never deferred to the first operation
   (ADR 0017/0020)."
  [conn]
  (proto/-kv conn))

(defn create-bucket
  "Create a Bucket on the KV context `ctx` from the portable, CLOSED kebab
   `config` map, returning a platform-native promise that resolves to a Bucket
   handle — the value every entry operation takes, binding the connection's
   codec. Config keys: `:bucket` (required, the Bucket's name), `:description`,
   `:history` (revisions kept per key), `:ttl-ms` (integer milliseconds),
   `:max-value-size`, `:max-bucket-size` (bytes), `:storage` (`:file` |
   `:memory`), `:replicas`, and `:compression?`. The map is closed: an
   unrecognized key rejects the promise with a validation `:type
   :unknown-config-key`, an omitted `:bucket` with `:missing-required-key`, and a
   malformed Bucket name with `:invalid-name`, all pre-flight before any native
   call (ADR 0015). A config the SERVER rejects surfaces as an operational `:type
   :jetstream-api-error` carrying `{:code :description}` (ADR 0020)."
  [ctx config]
  (-> (impl/resolved nil)
      (impl/then (fn [_] (bucket/validate-config config)))
      (impl/bind (fn [_] (proto/-create-bucket ctx config)))))

(defn open-bucket
  "Open the existing Bucket named `bucket` on the KV context `ctx`, returning a
   platform-native promise that resolves to a Bucket handle (see `create-bucket`).
   Opening VERIFIES the Bucket exists, so the promise rejects with an operational
   `:type :bucket-not-found` when it does not (ADR 0023) — at the handle, never
   deferred to the first entry operation — and pre-flight with a validation
   `:type :invalid-name` when `bucket` is malformed (ADR 0015)."
  [ctx bucket]
  (-> (impl/resolved nil)
      (impl/then (fn [_] (bucket/validate-name bucket)))
      (impl/bind (fn [_] (proto/-open-bucket ctx bucket)))))

(defn delete-bucket
  "Delete the Bucket named `bucket` on the KV context `ctx` — decommissioning the
   Bucket and every entry in it — returning a platform-native promise that
   resolves to nil once it is gone. The promise rejects with an operational
   `:type :bucket-not-found` when no such Bucket exists (ADR 0023), and
   pre-flight with a validation `:type :invalid-name` when `bucket` is malformed
   (ADR 0015)."
  [ctx bucket]
  (-> (impl/resolved nil)
      (impl/then (fn [_] (bucket/validate-name bucket)))
      (impl/bind (fn [_] (proto/-delete-bucket ctx bucket)))))
