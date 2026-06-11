---
id: nts-01ktw1th72xx
title: Decide respond-error terminality, fix the double error reply
status: closed
type: task
priority: 1
mode: hitl
created: '2026-06-11T19:16:26.459359797Z'
updated: '2026-06-11T20:31:48.871771863Z'
closed: '2026-06-11T20:31:48.871771863Z'
tags:
- services
- review
acceptance:
- title: Terminality decision made and recorded in an ADR (amendment or new)
  done: true
- title: Exactly one error reply reaches the wire per respond-error call, proven by a test watched red against the current double-reply
  done: true
- title: Endpoint num-errors still counts the error on both legs
  done: true
- title: If the throw stays, it carries a canonical :type instead of a bare host exception
  done: true
links:
- nts-01ktvn87why4
---

## Description

Both review axes converged on this. (service/respond-error conn msg code description) currently replies and then throws a bare RuntimeException/js-Error (no :type), making the verb terminal — handler code after it never runs, and the native auto-500 machinery then sends a SECOND error reply on the wire. The terminal contract is consumer-visible but lives only in the docstring and CHANGELOG; ADRs 0024/0025 describe respond-error as 'consistent with core/reply' with no terminality, and neither the epic nor the errors slice asked for a throw (it exists to make the endpoint's num-errors stat count the error).

Decision needed (HITL): keep the terminal throw — then give it a proper :type, suppress the redundant second reply, and amend ADR 0024/0025 to record terminality — or drop the throw and find another route to a correct num-errors count. The double reply on the wire is a bug under either decision.

## Notes

**2026-06-11T20:21:41.457628971Z**

HITL decision (grill session): Option A — drop the throw. respond-error is non-terminal like core/reply. Verified via jnats 2.25.3 bytecode: EndpointContext's private numErrors moves ONLY via its catch-all, which inseparably auto-500s (ServiceMessage has no responded guard), so terminal+counted+single-reply was structurally impossible on the JVM. num-errors is redefined to count only uncaught handler failures (both natives' own semantics); explicit respond-error does not count. AC3 renegotiated accordingly. Record: amend ADR 0025. CHANGELOG 0.5.0 rewritten in place (Clojars latest is 0.4.0 — never shipped).

**2026-06-11T20:31:38.778542212Z**

AC3 satisfied as renegotiated: num-errors counts uncaught handler failures (throw/rejected promise) on both legs — the natives' own semantics; an explicit respond-error is deliberately uncounted (single-reply wins, see ADR 0025 amendment). AC4 moot: the throw was dropped entirely.

**2026-06-11T20:31:48.871771863Z**

respond-error is NOT terminal: dropped the throw — it is core/reply-shaped, sends exactly one reply on the wire, and handler code after it runs. Decision forced by a verified jnats 2.25.3 constraint: its private num_errors moves only via the dispatcher catch-all, which inseparably auto-500s, so terminal+counted+single-reply could not all hold on the JVM. num-errors redefined to count uncaught handler failures only (both natives' own semantics); explicit respond-error is uncounted. Recorded as an amendment to ADR 0025; CHANGELOG 0.5.0 rewritten in place (never shipped — Clojars latest is 0.4.0) and README updated. New wire-level test (hand-rolled publish-with-reply + reply-box subscription) watched red against the double reply on the JVM, then green; stats test flipped rerr num-errors 1→0. Full suite green on JVM + Node, lint clean.
