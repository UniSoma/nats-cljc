# Service application errors are reply payloads, not normalized Errors

A Service that answers a request with an error — jnats `respondStandardError(conn, message, code)`, nats.js `respondError(code, description, data?)`, both setting the `Nats-Service-Error` / `Nats-Service-Error-Code` headers — produces a **successful request carrying an error payload**, not a normalized *Error* (ADR 0006). `core/request` resolves its promise normally with the reply Message, error headers intact; a caller opts into reading them with `(service/error msg) → nil | {:code … :description …}`. Service application errors do **not** join the canonical `:type` set the way JetStream's operational conditions did (ADR 0020). A future reader who expects `(:type (ex-data e))` to report "code 400" will not find it there — by design.

## Why they stay out of the Error set

The canonical *Error* set is **transport and protocol failures** a consumer dispatches on in production: `:timeout`, `:no-responders`, `:permissions-violation`, `:connection-closed`, and the JetStream/KV operational extensions. A service replying "code 400, bad input" is none of those. The request reached a responder, the responder ran, and it sent back a reply. That the reply *means* "your input was bad" is application semantics living in the payload and two headers — the same category as any domain "no, because…" answer, not a NATS failure.

The decisive structural reason: a consumer **invokes a service endpoint with an ordinary `core/request`**. There is no special "call-endpoint" verb that could know about service-error headers. To fold service errors into the Error model, `core/request` itself would have to sniff `Nats-Service-Error` on every reply and **reject** when present — pushing a services-only concern into the core pub/sub path that every non-service request also travels. That is a leak in the wrong direction: core must not learn about services.

So the split is clean: transport failures of the request (no responder, timeout, closed connection) remain normalized Errors that reject the `core/request` promise; an application error *encoded in a delivered reply* is data the caller reads. `:no-responders` in particular stays a normalized Error — it is transport (nobody is hosting the subject) and is exactly how a client learns no Service is up.

## The server-side counterpart

The same boundary governs the hosting side. A handler that throws or returns a rejected promise does **not** reach `:on-error` (a Service has no such sink) — it auto-replies a service error (code 500, description from the exception) and is counted in the endpoint's `num_errors` / `last_error` natively. The thrown value is the *cause* of an error reply, never a normalized Error delivered to a connection-level channel. `(service/respond-error conn msg code description)` is the explicit form of the same act.

## Considered options

- **Reinterpret `Nats-Service-Error` headers as a canonical `:type`** (e.g. `:service-error`) and reject the `core/request` promise. Rejected: it forces core request-reply to inspect headers for a feature it must not depend on, and it miscategorizes a delivered reply as a transport failure — a caller who retries transient Errors would wrongly retry a deterministic "400 bad input."
- **A dedicated `service/request` verb** that does sniff the headers and rejects on a service error, leaving `core/request` untouched. Rejected for v1: it splits request-reply into two verbs for a difference the `(service/error msg)` reader already expresses on the existing one, and most callers want to branch on the structured error payload anyway, not catch a thrown ex-info. Can be revisited if a reject-on-service-error ergonomic proves wanted.

## Consequences

- The canonical *Error* set is unchanged by services; the *Validation error* set gains `:invalid-version` and `:duplicate-endpoint` (caller-misuse at `create`, ADR 0015), but no new *Error* `:type`.
- A caller reads a service error with `(service/error msg)` on the reply Message; absence (`nil`) means a normal reply. Branching on a domain error is reading data, identical in shape on both legs.
- The `:code` is an integer (jnats `int`, nats.js `number`); the wire header value is its string form. `(service/error msg)` returns the integer.
