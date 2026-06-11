# Service application errors are reply payloads, not normalized Errors

A Service that answers a request with an error — jnats `respondStandardError(conn, message, code)`, nats.js `respondError(code, description, data?)`, both setting the `Nats-Service-Error` / `Nats-Service-Error-Code` headers — produces a **successful request carrying an error payload**, not a normalized *Error* (ADR 0006). `core/request` resolves its promise normally with the reply Message, error headers intact; a caller opts into reading them with `(service/error msg) → nil | {:code … :description …}`. Service application errors do **not** join the canonical `:type` set the way JetStream's operational conditions did (ADR 0020). A future reader who expects `(:type (ex-data e))` to report "code 400" will not find it there — by design.

## Why they stay out of the Error set

The canonical *Error* set is **transport and protocol failures** a consumer dispatches on in production: `:timeout`, `:no-responders`, `:permissions-violation`, `:connection-closed`, and the JetStream/KV operational extensions. A service replying "code 400, bad input" is none of those. The request reached a responder, the responder ran, and it sent back a reply. That the reply *means* "your input was bad" is application semantics living in the payload and two headers — the same category as any domain "no, because…" answer, not a NATS failure.

The decisive structural reason: a consumer **invokes a service endpoint with an ordinary `core/request`**. There is no special "call-endpoint" verb that could know about service-error headers. To fold service errors into the Error model, `core/request` itself would have to sniff `Nats-Service-Error` on every reply and **reject** when present — pushing a services-only concern into the core pub/sub path that every non-service request also travels. That is a leak in the wrong direction: core must not learn about services.

So the split is clean: transport failures of the request (no responder, timeout, closed connection) remain normalized Errors that reject the `core/request` promise; an application error *encoded in a delivered reply* is data the caller reads. `:no-responders` in particular stays a normalized Error — it is transport (nobody is hosting the subject) and is exactly how a client learns no Service is up.

## The server-side counterpart

The same boundary governs the hosting side. A handler that throws or returns a rejected promise does **not** reach `:on-error` (a Service has no such sink) — it auto-replies a service error (code 500, description from the exception) and is counted in the endpoint's `num_errors` / `last_error` natively. The thrown value is the *cause* of an error reply, never a normalized Error delivered to a connection-level channel. `(service/respond-error conn msg code description)` is the explicit error *reply*, but not the same act: it is **not terminal** — the handler keeps running after it, exactly as after `respond` / `core/reply` — and it does **not** move `num_errors` (see the amendment below).

## Amendment: respond-error is not terminal and does not count an endpoint error

`respond-error` briefly shipped (unreleased) as *terminal*: after sending the reply it threw, so the native dispatch would count the request in the endpoint's `num_errors`. That throw put a **second** error reply on the wire — both natives' catch-all answers an uncaught handler failure with an auto-500, after `respond-error` had already sent the explicit reply. The terminality was also an invention: neither this ADR nor 0024 asked for it, and it existed only to drive the counter.

The constraint that decides it (verified against jnats 2.25.3): jnats' per-endpoint `num_errors` is a private counter whose **only** public lever is its dispatcher's catch-all — and that catch-all *inseparably* auto-500s (`ServiceMessage` has no already-responded guard; every respond is a bare publish to the reply subject). So "terminal + counted + exactly one reply" cannot all hold on the JVM. One of the three had to go, and the wire is the contract: **exactly one reply per `respond-error`**.

The decision: `respond-error` is non-terminal and uncounted. `num_errors` counts **uncaught handler failures only** — a throw or a rejected promise, the auto-500 path — which is precisely what both natives themselves count (jnats and nats.js each tally an endpoint error only on a handler throw, never on an error reply). The portable stats relay that native truth rather than forcing a throw to synthesize a richer contract. A consumer who needs application-error visibility reads `(service/error reply)` per call; `num_errors` / `last_error` report genuine handler failures.

Rejected alternatives: keeping the throw with the second reply suppressed (impossible on the JVM through public jnats API); suppressing it on the JS leg only (we drive that dispatch ourselves, but it breaks leg parity); reflection into jnats' private counter (fragile across versions).

## Considered options

- **Reinterpret `Nats-Service-Error` headers as a canonical `:type`** (e.g. `:service-error`) and reject the `core/request` promise. Rejected: it forces core request-reply to inspect headers for a feature it must not depend on, and it miscategorizes a delivered reply as a transport failure — a caller who retries transient Errors would wrongly retry a deterministic "400 bad input."
- **A dedicated `service/request` verb** that does sniff the headers and rejects on a service error, leaving `core/request` untouched. Rejected for v1: it splits request-reply into two verbs for a difference the `(service/error msg)` reader already expresses on the existing one, and most callers want to branch on the structured error payload anyway, not catch a thrown ex-info. Can be revisited if a reject-on-service-error ergonomic proves wanted.

## Consequences

- The canonical *Error* set is unchanged by services; the *Validation error* set gains `:invalid-version` and `:duplicate-endpoint` (caller-misuse at `create`, ADR 0015), but no new *Error* `:type`.
- A caller reads a service error with `(service/error msg)` on the reply Message; absence (`nil`) means a normal reply. Branching on a domain error is reading data, identical in shape on both legs.
- The `:code` is an integer (jnats `int`, nats.js `number`); the wire header value is its string form. `(service/error msg)` returns the integer.
