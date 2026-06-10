---
id: nts-01ktsqnbtp7t
title: 'Tombstones and history: delete, purge, and history'
status: open
type: feature
priority: 1
mode: afk
created: '2026-06-10T21:40:22.480610370Z'
updated: '2026-06-10T21:40:22.480610370Z'
parent: nts-01ktsner23xc
tags:
- kv
- phase-3
acceptance:
- title: delete resolves to nil; the key then reads as absent via get, while history retains the Tombstone with its :operation visible
  done: false
- title: purge resolves to nil and erases the key's history down to a single purge marker
  done: false
- title: delete and purge accept an optional :revision guard; a stale guard rejects with :wrong-revision carrying :key
  done: false
- title: history resolves to a vector of Entries oldest-to-newest including Tombstones and purge markers
  done: false
- title: :delta appears on history Entries only if verified meaningful on both natives, otherwise omitted
  done: false
- title: Portable facade tests pass on both legs
  done: false
deps:
- nts-01ktsqmtyszc
---

## Description

The destructive verbs and the history read. `(delete bucket key)` writes a Tombstone: the key subsequently reads as absent (get resolves to nil) while history is retained, so deletion stays observable to history readers and watchers. `(purge bucket key)` erases the key's history leaving a single purge marker, reclaiming space for good. Both resolve to nil and accept an optional `:revision` guard that rejects with `:wrong-revision` when stale, so operators only remove what they believe they are removing.

`(history bucket key)` resolves to a fully-realized vector of Entries oldest-to-newest including Tombstones and purge markers, each marker's `:operation` visible (server-bounded at 64 per key).

`:delta` semantics are settled empirically here: include it on history Entries only if both natives populate it meaningfully — verify, don't infer.
