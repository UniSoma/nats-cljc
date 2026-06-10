# JetStream is an unconditional NATS-family dependency

JetStream (and later KV / Object Store) ship as a **hard, unconditional** dependency, not an opt-in like the third-party codecs. On the JVM this is automatic — `io.nats:jnats` bundles JetStream, KV, and Object Store in-jar. On the CLJS leg JetStream lives in a *separate* npm package, `@nats-io/jetstream`, so nats-cljc declares it **unconditionally** in `src/deps.cljs` `:npm-deps` (alongside the existing `@nats-io/nats-core`), version-pinned in lockstep, so every downstream CLJS consumer auto-installs it.

The governing rule from ADR 0002/0004 — "force no dependency beyond the native NATS clients" — reads as *non-**NATS** dependency*. The NATS family (core + JetStream + KV + Object Store) is **one logical product**: jnats vends it as a single jar, and nats-cljc brings the CLJS half to parity rather than fragmenting it. JetStream is not a third-party add-on the way Transit (Jackson/msgpack tree) or a JSON library is; it is the other half of the same NATS product, authored and versioned by the same maintainers as nats-core.

This also **removes a footgun rather than offloading one**. `@nats-io/jetstream`'s `dependencies` pin `@nats-io/nats-core` to an *exact* version (`3.3.1`↔`3.3.1`, `3.4.0`↔`3.4.0`, no peer range). If the consumer owned that install, a version they picked that didn't match nats-cljc's core pin would dedupe a **second** nats-core into their tree — a duplicate-client hazard. Declaring both pins together in `src/deps.cljs` makes nats-cljc the single owner of the lockstep, so the hazard cannot arise from consumer choice.

## Bundle weight is namespace-structure, not dependency-declaration

The one real cost of going unconditional — shipping JetStream bytes to a browser app that only does core pub/sub — is **avoidable for free**, and the distinction matters: `:npm-deps` only makes the package *available* in `node_modules`. An npm package enters a consumer's *browser bundle* only if a CLJS namespace they actually `require` does `(:require ["@nats-io/jetstream"])`. So the JetStream JS import must stay confined to a **JetStream-specific impl namespace**, never folded into the shared `nats_cljc.impl.js` that core pub/sub already pulls in. A consumer who never requires the JetStream facade keeps a JetStream-free bundle — shadow-cljs's module graph excludes the unreachable npm dep. Unconditional *dependency* ≠ unconditional *bundle weight*.

## Considered options

- **Opt-in, mirroring the codecs** (ADR 0004) — the consumer `npm install`s `@nats-io/jetstream` themselves when they want it. Rejected: the codec precedent does not transfer. Codecs are genuinely *third-party* (Transit drags Jackson/msgpack/javassist; JSON needs a library) — outside NATS, so a polyglot-only consumer should not pay for them. JetStream is *first-party* NATS, the other half of the product jnats already bundles for free; making the CLJS leg opt-in would break JVM/CLJS parity. Worse, opt-in offloads the exact-version lockstep pin onto the consumer, manufacturing the duplicate-nats-core hazard this decision exists to prevent.
- **Unconditional, but with the import folded into `nats_cljc.impl.js`** — simplest to write. Rejected: it would force JetStream bytes into *every* CLJS bundle, including core-only browser apps, for no benefit. The JetStream-specific-namespace structure costs nothing and keeps core bundles lean.

## Consequences

- `src/deps.cljs` `:npm-deps` grows `@nats-io/jetstream` now and `@nats-io/kv` (Phase 3) / `@nats-io/obj` (Phase 4) — each pinned in lockstep with the nats-core version. Root `package.json` (dev/test toolchain) grows the same dep so our Node and browser test legs resolve it.
- **The lockstep pin is nats-cljc's maintenance burden**: bumping nats-core means bumping JetStream (and KV/OS) to the matching release in the same change. A mismatched pair is a release bug, caught by us, not the consumer.
- The JetStream JS import is constrained to JetStream namespaces (an internal-structure obligation; see ADR 0005), so core-only CLJS consumers ship zero JetStream bytes despite the unconditional dependency.
- Documentation should still *state* the `@nats-io/jetstream` requirement and its pinned version plainly, even though install is automatic — so a consumer auditing their tree understands why it is there and never hand-installs a conflicting version.
