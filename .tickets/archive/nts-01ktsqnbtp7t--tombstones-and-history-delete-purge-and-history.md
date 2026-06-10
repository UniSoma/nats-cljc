---
id: nts-01ktsqnbtp7t
title: 'Tombstones and history: delete, purge, and history'
status: closed
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:22.480610370Z'
updated: '2026-06-10T23:40:30.528501771Z'
closed: '2026-06-10T23:40:30.528501771Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: delete resolves to nil; the key then reads as absent via get, while history retains the Tombstone with its :operation visible
  done: true
- title: purge resolves to nil and erases the key's history down to a single purge marker
  done: true
- title: delete and purge accept an optional :revision guard; a stale guard rejects with :wrong-revision carrying :key
  done: true
- title: history resolves to a vector of Entries oldest-to-newest including Tombstones and purge markers
  done: true
- title: :delta appears on history Entries only if verified meaningful on both natives, otherwise omitted
  done: true
- title: Portable facade tests pass on both legs
  done: true
deps:
- nts-01ktsqmtyszc
---

## Description

The destructive verbs and the history read. `(delete bucket key)` writes a Tombstone: the key subsequently reads as absent (get resolves to nil) while history is retained, so deletion stays observable to history readers and watchers. `(purge bucket key)` erases the key's history leaving a single purge marker, reclaiming space for good. Both resolve to nil and accept an optional `:revision` guard that rejects with `:wrong-revision` when stale, so operators only remove what they believe they are removing.

`(history bucket key)` resolves to a fully-realized vector of Entries oldest-to-newest including Tombstones and purge markers, each marker's `:operation` visible (server-bounded at 64 per key).

`:delta` semantics are settled empirically here: include it on history Entries only if both natives populate it meaningfully — verify, don't infer.

## Notes

**2026-06-10T23:40:30.528501771Z**

Shipped delete, purge, and history on the KV facade. delete writes a Tombstone (resolves nil; get reads the key as absent; history retains the Tombstone with :operation :delete); purge erases the key's history to a single :purge marker. Both take an optional {:revision n} guard routed through the shared CAS seam, so a stale guard rejects :wrong-revision carrying :key on both legs (verified 10071 from jnats and nats.js). history resolves to a fully-realized vector of Entries oldest-to-newest — live values decoded through the Bucket's Codec, markers carrying :value nil — each with :delta, which probing confirmed both natives populate meaningfully (distance from the newest revision), so it is included. New protocol verbs -kv-delete/-kv-purge/-kv-history; JVM rides jnats' delete/purge/history off-thread, CLJS drains kv.history's QueuedIterator. Suite green on JVM and Node; clj-kondo clean.
