---
id: nts-01kt87wgzfc3
title: Implement the `subject` builder
status: open
type: feature
priority: 2
mode: afk
created: '2026-06-04T02:37:34.575702642Z'
updated: '2026-06-04T02:53:24.972660048Z'
tags:
- review
- subject
acceptance:
- title: '`(subject "orders" id "created")` returns `"orders.<id>.created"` on both legs'
  done: false
- title: Parts are stringified and dot-joined into a valid Subject
  done: false
- title: Tests cover the documented example on both legs
  done: false
- title: clj-kondo clean; suite green on JVM and Node
  done: false
---

## Description

The README Core API table and CONTEXT.md (the string is canonical; a builder helper composes one from parts) document a public `(subject & parts) -> String` builder — example `(subject "orders" id "created")` — but no such verb exists in `core.cljc`.

Implement the portable `subject` builder that joins parts into a dot-delimited Subject string, with tests on both legs covering the documented example.
