---
id: nts-01kvtzksb69t
title: Audit all public docstrings for self-containment (ADR 0027)
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-23T19:34:15.646097214Z'
updated: '2026-06-23T19:43:13.601760279Z'
tags:
- docs
- examples
links:
- nts-01ktwyk19r7p
---

## Description

Apply the self-contained-docstring standard (ADR 0027, decided during the messaging-examples ergonomics exercise nts-01ktwyk19r7p) to every public (non-^:no-doc) var across the API surface: nats-cljc.core (the remaining fns beyond connect — publish/subscribe/request/unsubscribe/flush/drain/close/subject), jetstream, kv, service, and codec.

nats.core/connect is the role model (rewritten in this pass): see its docstring in src/nats_cljc/core.cljc.

Per-docstring checklist (ADR 0027):
1. Purpose (one line).
2. Every parameter and every option key the fn reads, each with type, default, and effect — no silent keys.
3. Return shape.
4. Failure behavior — the canonical error/validation :type(s) this call throws or rejects with (ADR 0006/0015).
5. A usage example only where the call shape is non-obvious.

Reconciliation rule: (ADR 00NN)/CONTEXT: X references are supplemental; the docstring must be correctly-callable without following them.

Done when: every public var in the listed namespaces satisfies the checklist; clj-kondo clean; spot-check that each documented option list matches the fn's actual destructuring.

## Notes

**2026-06-23T19:43:13.601760279Z**

Formatting requirement added to ADR 0027 (verified by rendering connect through cljdoc's flexmark+metagetta pipeline): docstrings are Markdown on cljdoc. The audit must produce cljdoc-correct formatting, not just self-contained content — no space-aligned columns (use Markdown tables for complex option maps, e.g. connect's :auth, or prose bullet lists), code examples in fenced ```clojure blocks (not indentation-only, which collapses below cljdoc's 4-space threshold), backtick all args/keywords, and [[ns/var]] wikilinks for sibling-var references. connect's docstring is the corrected role model.
