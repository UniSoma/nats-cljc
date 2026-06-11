;; npm dependencies this library brings to a CLJS consumer's build (ADR 0003).
;; @nats-io/jetstream, @nats-io/kv and @nats-io/services are unconditional and
;; lockstep-pinned to the nats-core version (ADR 0016, ADR 0026): the library owns
;; the exact-version pins so a consumer can't dedupe a second nats-core into their
;; tree. @nats-io/services pins nats-core to an exact 3.4.0, which floors the whole
;; trio at 3.4.0. The JetStream JS import is confined to nats-cljc.jetstream.impl.js
;; and the KV one to nats-cljc.kv.impl.js (services likewise, once it lands), so a
;; core-only bundle still ships zero JetStream/KV/services bytes despite the
;; dependencies being declared here.
{:npm-deps {"@nats-io/nats-core" "3.4.0"
            "@nats-io/jetstream" "3.4.0"
            "@nats-io/kv"        "3.4.0"
            "@nats-io/services"  "3.4.0"}}
