---
id: nts-01kstxatbw6k
title: Error-model completion across both clients
status: closed
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:23:00.986432509Z'
updated: '2026-06-02T23:52:52.626604980Z'
closed: '2026-06-02T23:52:52.626604980Z'
acceptance:
- title: Each canonical error `:type` is reproduced and asserted with identical shape on both `io.nats:jnats` and `@nats-io/nats-core`
  done: true
- title: A handler that throws is caught and routed to `:on-status :error` (or the subscription `:on-error`) without killing the subscription
  done: true
- title: A decode failure on a subscription is routed to the per-sub `:on-error` / status `:error` sink as `:codec-error`
  done: true
- title: '`:slow-consumer` is routed to the subscription''s `:on-error` (per-sub, not `:on-status`) and `:max-pending` is honored — signal portable, drop native'
  done: true
deps:
- nts-01kstx9hs32y
- nts-01kstx9pbqe5
- nts-01kstzmd6d2v
- nts-01kstzmd96ms
---

## Description

Complete the canonical error model across both native clients so portable code always reads `(:type (ex-data e))` instead of branching on host exception types. Earlier slices delivered `:timeout`/`:no-responders` (request), `:codec-error` (codec production), and `:auth-invalid` (connect pre-flight). This slice normalizes the rest — `:connect-failed`, `:connection-closed`, `:permissions-violation`, `:max-payload-exceeded`, `:protocol-error`, `:drained` — and wires every async failure to its sink: a throwing handler or a decode failure to a per-subscription `:on-error` (else the connection `:on-status :error`), and `:slow-consumer` to `:on-error` only, honoring `:max-pending`.

**The contract is fully specified in the docs — treat them as the source of truth:** ADR 0006 (normalized error model, routing table, sink shapes), ADR 0007 (delivery semantics + "Realizing the contract: native consumption (road 2)"), CONTEXT.md (*Error*, *Status event*).

**This is one indivisible change.** AC#2 (throwing handler → sink), AC#3 (decode failure → `:codec-error` sink), AC#4 (`:slow-consumer` + `:max-pending`), and the `-subscribe` road-2 rework share the same rewritten dispatch — they ship together. The rework **supersedes** (does not revert) the promise-return tail-chain: that chain is correct for ordering/backpressure, but it keeps both native queues empty, so slow-consumer detection can never trip.

## Design

### Routing matrix (the anchor — full detail in ADR 0006)

| Failure | Channel | `(:type (ex-data …))` |
|---|---|---|
| handler throws | `:on-error` if set, else `:on-status :error` | raw thrown value (no canonical type) |
| decode failure | `:on-error` if set, else `:on-status :error` | `:codec-error` |
| slow consumer | `:on-error` **only** (drop if unset) | `:slow-consumer` |
| permissions / protocol | `:on-status :error` **only** | `:permissions-violation` / `:protocol-error` |

- The `:on-status` `:error` event is the lone non-bare lifecycle event: `{:type :error :error <ex-info>}`. `:on-error` receives the **bare ex-info**. Override is **strict** (never both fire). Both sinks nil ⇒ caught + dropped, subscription survives.

### Plumbing

- Connection record gains an `on-status` field: `[client codec on-status]`. `connect` stashes it (today it only reaches the listener/pump closure); it is the subscribe-dispatch fallback sink.
- `-subscribe` grows an **opts map**: `(-subscribe [conn subject queue opts handler])`, where `opts = {:on-error <fn-or-nil> :max-pending <int-or-nil>}`. `:max-pending` is a message **count** (single int); `:on-error` is a **1-arg** fn.
- Facade stays thin: it passes `opts` + the existing decode-wrapping handler `(fn [raw] (handler (decode-msg codec raw)))` down. `decode-msg` throws the `:codec-error` ex-info **synchronously** (the handler never sees garbage). All routing lives in the impl dispatch: `catch e → (if on-error (on-error e) (when on-status (on-status {:type :error :error e})))`.
- **Swallow points to replace:** JVM `impl/jvm.clj` `.exceptionally (apply [_ _] nil)` and CLJS `impl/js.cljs` `.catch (fn [_] js/undefined)` currently discard the handler-wrapper exception — that is exactly where the routing wires in.

### JVM (road 2)

- `onMessage` **blocks** the dispatcher thread on the handler's `CompletionStage`, **no timeout** (slowness must accumulate so jnats' queue fills). Delete the `tail` atom — blocking serializes naturally (ordered / one-at-a-time for free).
- `setPendingLimits(max-pending, -1)` (bytes unlimited) **only** when `:max-pending` is set; absent ⇒ leave jnats' 512K-msg / 64 MB defaults.
- Connection-level `ErrorListener` + an atom `{dispatcher → :on-error}` registry: `-subscribe` assocs (when `:on-error` non-nil); `slowConsumerDetected(conn, consumer)` looks up and calls `(on-error {:type :slow-consumer :subject … :max-pending … :pending …})` or drops; unsubscribe/drain dissocs (no leak).
- `errorOccurred(conn, string)`: substring-match jnats' exact `"Permissions Violation"` → `:permissions-violation`, else → `:protocol-error`; both → `:on-status :error`.
- ⚠ **Verify at impl:** the `consumer`-arg identity (dispatcher vs subscription) before keying the registry.

### CLJS (road 2)

- Drop `{:callback}`; consume the sub as an **async-iterable** via a detached `.next` recursion loop (mirror `pump-status!`), `await`-ing the handler before the next `.next`.
- Uniform error funnel: `(-> (js/Promise.resolve) (.then #(handler m)) (.then continue) (.catch route))` — a sync decode-throw, a sync handler-throw, and a rejecting promise all hit one `.catch`; `route` then **continues** the loop (subscription survives).
- `:max-pending` set ⇒ create the sub with `slow?: max-pending` + `sub.setSlowNotificationFn(fn)` (iterator-only API; threw under `{:callback}`). Route `{:type :slow-consumer …}` to `:on-error` else drop. Absent ⇒ no signal, unbounded buffer, no auto-drop (ADR 0007 divergence).
- Teardown by iterable **completion** (drain/unsubscribe/close ends it); the `.next` `.catch` swallows the close-race. `JsSubscription` `-drain`/`-active?` unchanged.
- ⚠ **Verify at impl:** the `slow?` / `setSlowNotificationFn` surface against the installed nats-core 3.3.1.

### Sync / one-shot types

- `:max-payload-exceeded` → **sync throw** from `publish` (fire-and-forget, no promise). Normalize in impl `-publish` (catch native `IllegalArgumentException` / `NatsError` → `{:type :max-payload-exceeded :subject … :size … :max …}`).
- `:connection-closed` → ops on a closed conn; normalize on `publish`/`subscribe` (sync throw) + `request` (reject). **Not** every op — `flush`/`drain`/`close`-after-close left native (accepted gap).
- `:drained` → op refused during the drain **window** (a *don't-retry* signal, distinct from `:connection-closed`, which is retry-able). Mirror the op's channel. After drain *completes* it is `:connection-closed`.

### Tests + infra — AC#1 (each type reproduced + asserted, identical shape, both legs)

| Type | Trigger | Channel | New infra |
|---|---|---|---|
| `:connect-failed` | connect to dead port `127.0.0.1:1` | reject | none |
| `:connection-closed` | `close`, then `request` | reject | none |
| `:drained` | slow handler holds the drain window; `request` during it | reject | none |
| `:max-payload-exceeded` | `publish` ~1.1 MB vs default 1 MB | sync throw | none |
| `:permissions-violation` | connect as restricted user; subscribe a forbidden subject | `:on-status :error` | restricted user |
| `:protocol-error` | classifier unit-test (no clean e2e trigger) | — | none |
| `:codec-error` | sub `:edn`, publish raw non-EDN bytes | `:on-error` + `:on-status` fallback | none |
| `:slow-consumer` | `:max-pending 1` + slow handler + flood | `:on-error` | none |
| handler-throw | throwing handler | `:on-error` + `:on-status` fallback | none |

- **Restricted user:** add a 3rd entry to `ci/nats-users.conf` (`{user: "restricted", password: …, permissions: {subscribe: {deny: "forbidden.>"}}}`) + matching creds in the test; leaves the `app`/nkey users untouched, so no 5th server.
- **`:slow-consumer` ex-info shape:** `{:type :slow-consumer :subject :max-pending :pending}` — `:pending` is best-effort/native-approximate (shape, not cadence); `:dropped` is omitted (the drop is native, not portable).
- Async types use an `:on-error` collector alongside the existing `status-collector` / `wait-for` harness.

## Notes

**2026-06-02T23:52:52.626604980Z**

Canonical error model completed across both clients (TDD, 9 new error-model tests + a classifier unit, green on JVM 57/109 and Node 57/104; clj-kondo clean; browser CI-only per ADR 0010).

Dispatch reworked to road 2 (ADR 0007), superseding the promise-return tail-chain so each native client's slow-consumer detection engages: JVM onMessage BLOCKS the dispatcher thread on the handler's CompletionStage (.join, no timeout); CLJS consumes the sub as an async-iterable via a detached .next loop (mirrors pump-status!), awaiting the handler. proto -subscribe grew an opts map {:on-error :max-pending}; the connection record gained on-status (subscribe-dispatch fallback sink) and, on JVM, a dispatcher->sink registry (atom) for ErrorListener.slowConsumerDetected -> originating sub. All async failures route strictly via (if on-error (on-error e) (when on-status (on-status {:type :error :error e}))): handler-throw (raw value), decode (:codec-error, thrown synchronously by decode-msg), :slow-consumer (per-sub only, :max-pending via setPendingLimits / setSlowNotificationFn). One-shot/sync types normalized in -publish/-request: :max-payload-exceeded (sync throw), :connection-closed (publish throw + request reject), :drained (request reject in the drain window — jnats keeps getStatus CONNECTED so classified by message; nats.js raises ClosedConnectionError so classified by isDraining). Connection-level :permissions-violation / :protocol-error share a server-error-type classifier (JVM ErrorListener.errorOccurred string; CLJS status() error event) -> :on-status :error only. Added a restricted user (deny subscribe forbidden.>) to ci/nats-users.conf — no 5th server.
