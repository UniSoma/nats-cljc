# Services joins the unconditional NATS family (3.4.0 floor)

The services framework ships as a **hard, unconditional** dependency, extending the rule ADR 0016 set for JetStream (and KV / Object Store). On the JVM it is automatic — `io.nats:jnats` carries `io.nats.service` in-jar. On the CLJS leg it is a separate npm package, `@nats-io/services`, declared **unconditionally** in `src/deps.cljs` `:npm-deps` and pinned in lockstep with the rest of the nats-io trio. Adding it **floors the whole trio at `3.4.0`** (`@nats-io/services@3.4.0` peer-requires `@nats-io/nats-core@3.4.0`), so `nats-core`, `@nats-io/jetstream`, and `@nats-io/kv` all move to `3.4.0` in the same change. ADR 0016 named "core + JetStream + KV + Object Store" as the one logical product; services postdates that list but is the same shape, so this records that it joins — and that a version floor moved as a result.

## Why unconditional, and why the floor is a tested gate

The 0016 reasoning transfers intact: `@nats-io/services` is **first-party** NATS, authored and versioned by the same maintainers as nats-core, not a third-party add-on the way the Transit/JSON codecs are. Making it opt-in would (a) break JVM/CLJS parity, since the JVM gets it in-jar for free, and (b) offload the exact-version lockstep pin onto the consumer, manufacturing the duplicate-`nats-core` hazard 0016 exists to prevent (`@nats-io/services` pins `nats-core` to an *exact* version, no peer range).

The new wrinkle 0016 did not anticipate is the **floor bump**. Lifting `nats-core` `3.3.1 → 3.4.0` is not a services-only change — it moves the version under the already-shipped core, JetStream, and KV surfaces. Per the project's "verify the toolchain, don't infer it" rule, the bump's blast radius is an **implementation gate, not an assumption**: the full suite must pass on both local legs (JVM + Node) against the `3.4.0` trio *before* this lands. A mismatched or behavior-breaking bump is a release bug caught by us, not the consumer — the lockstep maintenance burden 0016 already accepted, now exercised.

## Bundle weight stays namespace-structure, not dependency-declaration

The one real cost of unconditional — shipping service bytes to a browser app that only does core pub/sub — is avoidable for free, exactly as 0016 spells out. `:npm-deps` only makes the package *available* in `node_modules`; it enters a consumer's browser bundle only if a CLJS namespace they `require` imports it. So the `@nats-io/services` import is confined to a **service-specific impl namespace** (`nats_cljc.service.impl.js`), never folded into the shared `nats_cljc.impl.js` that core pub/sub pulls in. A new `:services` entry in the shadow-cljs `:core-bundle-check` / `:externs-check` guards enforces this, so a consumer who never requires the service facade ships zero service bytes despite the unconditional dependency.

## Considered options

- **Opt-in, mirroring the codecs (ADR 0004).** Rejected for the same reasons 0016 rejected it for JetStream: the codec precedent is for genuinely third-party trees, services is first-party, and opt-in offloads the lockstep pin and its duplicate-`nats-core` hazard onto the consumer.
- **Amend ADR 0016 in place** rather than a standalone ADR. Rejected: the `3.4.0` floor is a new, dated consequence that 0016 did not foresee, and a future reader bisecting a version bump is better served by a decision they can find at the bump than by an edit buried in an older ADR.

## Consequences

- `src/deps.cljs` `:npm-deps` grows `@nats-io/services`, lockstep-pinned with the trio; root `package.json` grows the same dep so the Node and browser test legs resolve it.
- The trio's pinned version moves to `3.4.0` in one change, gated on a green JVM + Node suite — the lockstep burden 0016 placed on the maintainer, now invoked.
- The `@nats-io/services` import is constrained to `nats_cljc.service.impl.js`, with a `:services` bundle/externs guard, so core-only CLJS bundles stay service-free.
- Documentation should state the `@nats-io/services` requirement and its pinned version plainly even though install is automatic, so a consumer auditing their tree understands why it is present.
