(ns ^:no-doc nats-cljc.jetstream.impl.pub
  "Portable acked-publish pre-flight deep module (ADR 0015/0020). The pure,
   platform-neutral half of publishing into a Stream: the reserved-header guard that
   keeps the `Nats-*` header namespace sanctioned-only. `:msg-id` and `:expect` are
   the way to set those reserved headers (the impl translates them to native
   publish options), so a reserved key set directly in user `:headers` is caller
   misuse — `:reserved-header`, raised pre-flight before any native call — rather
   than a silently-honored shadow of the sanctioned path. The per-leg impl
   namespaces build the native publish options and normalize the PubAck — the
   interop half — so this is the shared seam a single no-server unit test covers,
   the publish sibling of `nats-cljc.jetstream.impl.stream`.")

(defn reserved-header?
  "True when header `name` is in the reserved `Nats-*` namespace the server owns. The
   check is case-insensitive: the wire spells them Title-Case (`Nats-Msg-Id`), but it
   is the namespace that is reserved, so any casing of the `nats-` prefix is caller
   misuse."
  [name]
  (and (string? name)
       (.startsWith #?(:clj (.toLowerCase ^String name) :cljs (.toLowerCase name))
                    "nats-")))

(defn validate-headers
  "Guard a user `:headers` map pre-flight (ADR 0015), before any native call: a key
   in the reserved `Nats-*` namespace throws `:type :reserved-header` carrying the
   offending `:keys`, since `:msg-id`/`:expect` are the sanctioned way to set those.
   A non-nil non-map `:headers` (e.g. a vector) throws `:type :invalid-header` rather
   than a raw ClassCastException from `keys`, keeping the misuse portable and typed.
   Returns `headers` unchanged (nil included) so it can sit in a promise chain stage."
  [headers]
  (when (and (some? headers) (not (map? headers)))
    (throw (ex-info "Headers must be a map of name -> value(s)"
                    {:type :invalid-header :headers headers})))
  (let [reserved (filter reserved-header? (keys headers))]
    (when (seq reserved)
      (throw (ex-info "Reserved Nats-* header(s) must be set via :msg-id/:expect"
                      {:type :reserved-header :keys (vec reserved)}))))
  headers)
