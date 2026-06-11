# KV speaks KV vocabulary, not its stream substrate

A KV Bucket is a JetStream Stream under the hood (a `KV_*` stream, entries as messages, revisions as stream sequences, deletes as header-marked tombstones) — and the wrapped native clients surface that substrate freely in their own errors and types. `nats-cljc.kv` does **not**: the portable surface speaks only KV language, so a consumer dispatching on KV outcomes never needs to know KV is stream-backed. Concretely:

- **`:wrong-revision`, not `:wrong-last-sequence`.** A compare-and-set write losing its race — an `update` whose expected Revision is stale, or a `create` on a key that already exists — rejects with the new canonical Error `:type :wrong-revision` (carrying the `:key`), even though the wire-level condition is the very same wrong-last-sequence rejection Phase 2's `:expect` publishes surface as `:wrong-last-sequence`. One server condition, two canonical faces, chosen per layer's vocabulary. `create` and `update` failures share the one `:type` (a `create` *is* an update expecting revision 0, which is how both native clients model it).
- **`:bucket-not-found`, not `:stream-not-found`.** Opening a Bucket that does not exist rejects with its own `:type`, not the stream-level one the substrate raises.
- **Revision, not sequence.** The CAS currency is named Revision throughout the API, options (`:revision` guards on `delete`/`purge`), and docs; "sequence" stays a stream-layer word.
- **`get` on an absent key resolves to `nil`, not an Error.** Key absence (never written, deleted, or purged) is a normal domain outcome to branch on — unlike a missing *bucket*, which is misaddressed infrastructure and rejects. Entries are full maps, so a stored-nil value (`{:value nil …}`) stays distinguishable from absence.

## Considered options

- **Reuse the existing stream-layer vocabulary** (`:wrong-last-sequence`, `:stream-not-found`) — no new canonical members, and the JetStream `:expect` precedent already names the CAS condition. Rejected: it leaks the substrate into the one layer whose entire point is the key/value abstraction, and it would force KV consumers to catch stream-flavored types for key-level outcomes. The glossary already declares *sequence* and *stream* avoid-words in KV definitions; the error vocabulary should obey the same discipline.
- **`get` rejects with a `:key-not-found` Error.** Rejected: it would wrap the most common read pattern in try/catch on every platform for an outcome that is not operationally exceptional.

## Consequences

- The canonical Error set grows `:bucket-not-found` and `:wrong-revision` — minor-bump vocabulary additions per ADR 0009.
- The KV impl layer owns the mapping from each native client's stream-flavored failures to the KV-flavored canonical `:type`s; tests assert the mapping on both legs.
- Future KV-adjacent layers (Object Store, Phase 4) inherit the principle: each facade speaks its own domain's language and re-faces substrate conditions rather than re-exporting them. *(Amended: services took Phase 4 — see ADR 0026 — moving Object Store to Phase 5; ADRs 0024/0025 apply this same principle to the services facade.)*
