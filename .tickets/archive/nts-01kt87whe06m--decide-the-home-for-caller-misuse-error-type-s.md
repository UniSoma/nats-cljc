---
id: nts-01kt87whe06m
title: Decide the home for caller-misuse error `:type`s
status: closed
type: task
priority: 2
mode: hitl
created: '2026-06-04T02:37:35.040613364Z'
updated: '2026-06-05T21:07:00.056261089Z'
closed: '2026-06-05T21:07:00.056261089Z'
tags:
- review
- error
- contract
acceptance:
- title: 'A decision is recorded: canonical-set extension versus a separate validation-error category'
  done: false
- title: ADR 0006 and CONTEXT.md reflect the decision and enumerate the validation `:type`s or their category
  done: false
- title: The five existing validation `:type`s are accounted for
  done: false
links:
- nts-01kt87wg6qvj
- nts-01kt87wghgg4
---

## Description

The code emits public error `:type`s absent from the canonical set documented in ADR 0006 / CONTEXT.md: `:invalid-header`, `:invalid-max`, `:invalid-max-pending`, `:no-reply-subject` (core), and `:invalid-capacity` (blocking) — all caller-misuse validation errors thrown synchronously. A consumer matching `(:type (ex-data e))` against the documented set will not find them, and nothing signals that caller-misuse validation lives outside the normalized NATS error model.

Decide the contract: either add these to the canonical `:type` set, or document them as a separate validation-error category distinct from the normalized NATS errors. Update ADR 0006 + CONTEXT.md to match.

HITL — this is a public-contract decision. Reconciles the validation types emitted by the Header-validation and Reconnect-`:max`-guard tickets (linked).

## Notes

**2026-06-05T21:07:00.056261089Z**

Decided: caller-misuse validation errors are a SEPARATE category, not an extension of the canonical NATS set. Recorded in new ADR 0015 (separate-category over canonical-extension / a :category marker / a frozen set), with a cross-ref pointer added to ADR 0006 and a new 'Validation error' glossary term in CONTEXT.md. The five types (:invalid-header, :invalid-max, :invalid-max-pending, :no-reply-subject, :invalid-capacity) are all accounted for: enumerated but open + diagnostic-first, raised on the operation's own channel (sync ops throw, connect rejects) before any native call, never to a sink. auth-invalid stays canonical (operational, validates the NATS security model). Docs-only decision — all emitting code already shipped. Also closed the one coverage gap (added reply-without-reply-subject-rejected test, both legs green) and refreshed the README error section (completed the canonical list, added the validation-category paragraph). JVM 102 / Node 78 tests, 0 failures; clj-kondo clean.
