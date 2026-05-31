---
id: nts-01kstx9snk41
title: Queue groups (competing consumers)
status: open
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:22:27.507161086Z'
updated: '2026-05-31T02:19:24.429307506Z'
acceptance:
- title: Multiple subscriptions sharing a `:queue` group each receive a disjoint share — each message delivered to exactly one member
  done: false
- title: Verified on JVM, browser, and Node against a real server
  done: false
- title: A non-queue subscription on the same subject still receives every message
  done: false
deps:
- nts-01kstx8ysgv5
---

## Description

Competing consumers. `(nats/subscribe conn subject handler {:queue "workers"})` joins a named queue group; the server load-balances each matching message so it reaches exactly one member of the group.

CONTEXT: Queue group. ADR 0007 (delivery).

## Notes

**2026-05-31T02:19:24.429307506Z**

Review follow-up (Finding 3, native subscription leak): subscribe currently returns the *native* handle (jnats Subscription / nats.js Sub), so core/drain forks on (satisfies? proto/Conn ...) to tell a connection from a subscription (core.cljc:60), and tests reach .isActive/.isClosed on the handle directly. This slice adds the :queue option on subscribe and the public contract calls for 'a Subscription' — natural point to introduce a Subscription record wrapping the native handle (impl/jvm.clj:20-26, impl/js.cljs:12-18). Doing so collapses drain's satisfies? branch into a uniform proto/-drain, gives unsubscribe/:on-error a home, and removes the test-only reach-ins. Treated as a direct prerequisite for this work, not a standalone refactor.
