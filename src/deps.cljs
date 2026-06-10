;; npm dependencies this library brings to a CLJS consumer's build (ADR 0003).
;; @nats-io/jetstream and @nats-io/kv are unconditional and lockstep-pinned to the
;; nats-core version (ADR 0016): the library owns the exact-version pins so a
;; consumer can't dedupe a second nats-core into their tree. The JetStream JS
;; import is confined to nats-cljc.jetstream.impl.js and the KV one to
;; nats-cljc.kv.impl.js, so a core-only bundle still ships zero JetStream/KV bytes
;; despite the dependencies being declared here.
{:npm-deps {"@nats-io/nats-core" "3.3.1"
            "@nats-io/jetstream" "3.3.1"
            "@nats-io/kv"        "3.3.1"}}
