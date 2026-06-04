---
id: nts-01kt87whe06m
title: Decide the home for caller-misuse error `:type`s
status: open
type: task
priority: 2
mode: hitl
created: '2026-06-04T02:37:35.040613364Z'
updated: '2026-06-04T02:53:25.434405381Z'
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
