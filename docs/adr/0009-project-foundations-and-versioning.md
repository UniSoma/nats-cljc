# Project foundations and versioning contract

Foundational, hard-to-reverse-once-published choices:

- **License: Apache-2.0.** Permissive, with an explicit patent grant, and the friendliest option for a library other organizations embed.
- **Clojars coordinate: `io.github.unisoma/nats-cljc`** — a verified-group coordinate, **all lowercase**. Clojars verifies `io.github.<owner>` groups against the GitHub identity, and because GitHub logins are case-insensitive (the org displays as `UniSoma` but `github.com/unisoma/…` resolves to the same repo), Clojars lowercases the owner when it creates the verified group. The verified group is therefore `io.github.unisoma`, and since Clojars only accepts publishes to a verified group, that lowercase string — not the GitHub display case — is the binding, permanent coordinate. (The mixed-case `io.github.UniSoma` was never publishable; an earlier draft of this ADR recorded it with the rationale "must match the GitHub org login exactly," which is wrong: the verified group fixes the case to lowercase.) The code namespace root stays `nats-cljc.*` (ADR 0005) — independent of the coordinate. The SCM `<url>` may keep any case (GitHub resolves it), but is kept lowercase for pom consistency.
- **Versioning: semver, with the normalized vocabularies as the public contract.** The error `:type` set (ADR 0006), the status `:type` set (ADR 0008 connection lifecycle), the message-map keys (ADR-less, in `CONTEXT.md`), and the verb return contracts (the Core verb table) are all part of the contract. **Adding** a member is a minor bump; **renaming or removing** one is a major bump.

## Why this matters

The write-once-run-both promise only holds across releases if the normalized surfaces are stable. Treating them as the semver contract makes "the same `.cljc` keeps working" an enforceable guarantee, not an aspiration.

## Considered options

- **EPL-1.0** (the Clojure-ecosystem default) — reasonable, but Apache-2.0's explicit patent grant and broad corporate acceptance won.

## Consequences

- Relicensing later requires every contributor's agreement.
- The coordinate is permanent once published.
- Maintainers cannot rename or remove a normalized vocabulary member without a major version, even when the underlying native client changes its own naming — the normalization layer absorbs that.
