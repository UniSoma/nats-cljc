---
id: nts-01kt87wgzfc3
title: Implement the `subject` builder
status: closed
type: feature
priority: 2
mode: afk
created: '2026-06-04T02:37:34.575702642Z'
updated: '2026-06-04T20:01:02.129430270Z'
closed: '2026-06-04T20:01:02.129430270Z'
tags:
- review
- subject
acceptance:
- title: '`(subject "orders" id "created")` returns `"orders.<id>.created"` on both legs'
  done: true
- title: Parts are stringified and dot-joined into a valid Subject
  done: true
- title: Tests cover the documented example on both legs
  done: true
- title: clj-kondo clean; suite green on JVM and Node
  done: true
---

## Description

The README Core API table and CONTEXT.md (the string is canonical; a builder helper composes one from parts) document a public `(subject & parts) -> String` builder — example `(subject "orders" id "created")` — but no such verb exists in `core.cljc`.

Implement the portable `subject` builder that joins parts into a dot-delimited Subject string, with tests on both legs covering the documented example.

## Notes

**2026-06-04T20:01:02.129430270Z**

Implemented the portable (subject & parts) builder in core.cljc as (str/join "." parts) — str/join stringifies each part, so numbers/non-strings dot-join into a canonical Subject with no extra map. Test-first, two RED->GREEN cycles: (1) the documented (subject "orders" id "created") => "orders.123.created", (2) a non-string part is stringified. Both are pure .cljc tests (no server/async), so they run identically on both legs. clj-kondo clean; suite green JVM (92t/198a) and Node (69t/126a).
