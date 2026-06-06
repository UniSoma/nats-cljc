# JetStream acks are sugar over publish, not native ack methods

A delivered JetStream message is a **pure-data map** `{:subject :data :headers :js {…}}` — no native message object retained. Acknowledgement (`ack` / `nak` / `term` / `working`, and double-ack) is implemented as **publish/request of the protocol payload (`+ACK` / `-NAK` / `+WPI` / `+TERM`) to the message's ack subject**, exactly as core `reply` is sugar over publish to a message's reply subject — *not* by delegating to the native clients' `.ack()` methods.

The delivered message is built by a per-leg lift that reads the native metadata and captures the ack-subject string, then discards the native object. So metadata is still extracted by each client's native accessor (avoiding fragile `$JS.ACK.*` token parsing, whose layout varies by server version), but everything downstream is pure data and the ack path is one code path, byte-identical on both legs.

Why this over the obvious "just call `.ack()`":

- **Consistency.** The library already treats delivered messages as pure data and `reply` as sugar over publish (taking the connection explicitly). Acks follow the same shape; retaining a native handle on JS messages would make them the one delivery that isn't plain data.
- **Portability.** The ack payloads are version-independent NATS protocol, so publishing them is identical on the JVM and CLJS. Delegating instead means two ack code paths (`jnats Message.ack` vs `nats.js JsMsg.ack`) and normalizing two native metadata shapes at the ack layer.
- **Idempotency for free.** A redundant ack is a harmless publish the server ignores, so "ack is idempotent / no-throw" needs no mutable per-message state — which a pure-data map could not hold anyway. This dissolves the cross-leg question of whether the JVM client no-ops a second terminal ack.

The ack address lives under `:js`, not as a top-level `:reply`, so a mistaken `(reply conn js-msg …)` raises `:no-reply-subject` rather than publishing garbage to the ack subject.

double-ack returns a `Promise<bool>` and is sugar over `request` to the ack subject (the server replies to confirm). It is named `double-ack` (the NATS-community term), not `ack-sync` (jnats' name), because ours is asynchronous — calling it "sync" would be a portability lie.

## Considered options

- **Delegate to native `.ack()` / `.nak()` methods, carrying the native message handle on the delivered map.** Rejected: it makes JS messages the only non-pure-data delivery, forks the ack path per leg, forces normalization of two native metadata shapes, and reintroduces the unresolved question of JVM second-ack idempotency — with no benefit, since the ack payloads are trivial, stable protocol and metadata is already lifted natively.
- **Parse the `$JS.ACK.*` ack subject ourselves to build metadata.** Rejected: the token layout differs across server versions (V1 vs V2, optional domain / account hash), so hand-parsing is a version-fragility trap. We capture the subject verbatim for publishing acks but read metadata via each client's native accessor at lift time.

## Consequences

- The library owns the ack-payload constants (`+ACK` / `-NAK` / `+WPI` / `+TERM`, `-NAK {"delay":ns}`); these are version-independent and stable.
- `ack` / `nak` / `term` / `working` are synchronous and return nil; double-ack returns `Promise<bool>`. All are idempotent; `working` is exempt (a repeatable progress signal).
- Delivered JS messages are pure data (no opaque native object), inspectable and loggable like core messages.
- A new internal per-leg lift (a `js-msg->raw` counterpart to `msg->raw`) carries the JetStream metadata and ack subject.
