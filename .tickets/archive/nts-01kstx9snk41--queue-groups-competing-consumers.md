---
id: nts-01kstx9snk41
title: Queue groups (competing consumers)
status: closed
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:22:27.507161086Z'
updated: '2026-06-02T00:58:24.244599102Z'
closed: '2026-06-02T00:58:24.244599102Z'
acceptance:
- title: Multiple subscriptions sharing a `:queue` group each receive a disjoint share — each message delivered to exactly one member
  done: true
- title: Verified on JVM, browser, and Node against a real server
  done: true
- title: A non-queue subscription on the same subject still receives every message
  done: true
deps:
- nts-01kstx8ysgv5
---

## Description

Competing consumers. `(nats/subscribe conn subject handler {:queue "workers"})` joins a named queue group; the server load-balances each matching message so it reaches exactly one member of the group.

CONTEXT: Queue group. ADR 0007 (delivery).

## Notes

**2026-05-31T02:19:24.429307506Z**

Review follow-up (Finding 3, native subscription leak): subscribe currently returns the *native* handle (jnats Subscription / nats.js Sub), so core/drain forks on (satisfies? proto/Conn ...) to tell a connection from a subscription (core.cljc:60), and tests reach .isActive/.isClosed on the handle directly. This slice adds the :queue option on subscribe and the public contract calls for 'a Subscription' — natural point to introduce a Subscription record wrapping the native handle (impl/jvm.clj:20-26, impl/js.cljs:12-18). Doing so collapses drain's satisfies? branch into a uniform proto/-drain, gives unsubscribe/:on-error a home, and removes the test-only reach-ins. Treated as a direct prerequisite for this work, not a standalone refactor.

**2026-06-01T22:30:49.388594363Z**

API grounding (verified against jnats 2.25.3 + @nats-io/nats-core in node_modules):

JVM queue subscribe — jnats Dispatcher has a queue overload:
  subscribe(String subject, MessageHandler) -> Subscription        (current, no queue)
  subscribe(String subject, String queue, MessageHandler) -> Subscription   (queue group)
So -subscribe threads queue as the 2nd arg; nil/blank queue keeps the non-queue overload.

CLJS queue subscribe — nats-core SubOpts has 'queue?: string' alongside 'callback':
  client.subscribe(subject, #js {:queue q :callback ...}) -> Subscription
So add :queue to the existing #js opts; omit when nil.

Subscription surface for the record refactor (both confirmed):
  jnats Subscription:    .isActive .drain(Duration) .unsubscribe .getQueueName .getSubject
  nats-core Subscription: .isClosed .drain()->Promise .unsubscribe(max?) .getReceived .getSubject

**2026-06-01T23:09:56.882637511Z**

Implemented via TDD (red→green→refactor).

Feature: subscribe gained a 4-arity (conn subject handler {:queue ...}); proto/-subscribe threads queue → jnats 3-arg Dispatcher.subscribe overload (JVM) / nats.js SubOpts.queue (CLJS).

Refactor (the bundled prerequisite): subscribe now returns a Subscription record (JvmSubscription/JsSubscription) wrapping the native handle. Split -drain out of Conn into a Drainable protocol implemented by both Connection and Subscription, so core/drain collapsed from a (satisfies? proto/Conn ...) fork to a uniform (proto/-drain x). Added a Sub/-active? predicate, which let the test's sub-ended? helper drop its native .isActive/.isClosed reach-ins. Removed the now-dead impl/drain-subscription on both platforms.

Tests: queue-group-load-balances (AC#1, disjoint+complete share, both members get a share) and non-queue-subscription-receives-all-alongside-a-queue-group (AC#3).

Status: green on JVM (27 tests / 48 assertions) and Node (27 / 50); clj-kondo clean. AC#2 browser leg is CI-only (ADR 0010) — left unticked until CI is green. Not committed (awaiting user).

**2026-06-02T00:58:24.153879118Z**

CI green on all three platforms (JVM, browser-headless, Node) — AC#2 verified, ticket closed.

Post-implementation review hardening: the :queue option is normalized once in the core facade via (when-not (str/blank? queue) queue), so nil/empty/whitespace all collapse to nil (plain subscription) before reaching either native layer. Without it the platforms diverged on a blank queue — JVM took jnats' 3-arg overload (blank-named queue group) while nats.js' own truthiness dropped it (plain sub). Single normalization point keeps the portable contract honest.

**2026-06-02T00:58:24.244599102Z**

Queue groups (competing consumers) shipped and CI-green on JVM, browser, and Node. nats/subscribe gained a :queue opt that joins a named group so the server load-balances each message to exactly one member (jnats 3-arg Dispatcher.subscribe / nats.js SubOpts.queue); a nil-or-blank :queue normalizes to a plain subscription in the core facade. Bundled the Subscription-record prerequisite: subscribe returns JvmSubscription/JsSubscription wrapping the native handle, -drain split out of Conn into a Drainable protocol (collapsing core/drain's satisfies? fork into a uniform proto/-drain), and a new Sub/-active? predicate removed the tests' native reach-ins.
