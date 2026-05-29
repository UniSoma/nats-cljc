# Project foundations and versioning contract

Foundational, hard-to-reverse-once-published choices:

- **License: Apache-2.0.** Permissive, with an explicit patent grant, and the friendliest option for a library other organizations embed.
- **Clojars coordinate: `io.github.UniSoma/nats-cljc`** — a verified-group coordinate. The `io.github.<owner>` segment must match the GitHub org login exactly (Maven coordinates are case-sensitive), so the group case must track the real org name before the first publish.
- **Versioning: semver, with the normalized vocabularies as the public contract.** The error `:type` set (ADR 0006), the status `:type` set (ADR 0008 connection lifecycle), the message-map keys (ADR-less, in `CONTEXT.md`), and the verb return contracts (the Core verb table) are all part of the contract. **Adding** a member is a minor bump; **renaming or removing** one is a major bump.

## Why this matters

The write-once-run-both promise only holds across releases if the normalized surfaces are stable. Treating them as the semver contract makes "the same `.cljc` keeps working" an enforceable guarantee, not an aspiration.

## Considered options

- **EPL-1.0** (the Clojure-ecosystem default) — reasonable, but Apache-2.0's explicit patent grant and broad corporate acceptance won.

## Consequences

- Relicensing later requires every contributor's agreement.
- The coordinate is permanent once published.
- Maintainers cannot rename or remove a normalized vocabulary member without a major version, even when the underlying native client changes its own naming — the normalization layer absorbs that.
