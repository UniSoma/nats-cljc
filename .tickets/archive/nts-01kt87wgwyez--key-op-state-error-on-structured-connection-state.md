---
id: nts-01kt87wgwyez
title: Key `op-state-error` on structured connection state
status: closed
type: bug
priority: 3
mode: afk
created: '2026-06-04T02:37:34.493956165Z'
updated: '2026-06-05T18:57:24.244637387Z'
closed: '2026-06-05T18:57:24.244637387Z'
tags:
- review
- error
acceptance:
- title: JVM op-state-error classifies via (.getStatus client) against Connection$Status — CLOSED → :connection-closed, CONNECTED → :drained — and never reads the exception message text
  done: true
- title: A simulated jnats message reword (e.g. "Connection is Draining" → "drain in progress") no longer changes the classification
  done: true
- title: A non-drain/closed IllegalStateException (status RECONNECTING/DISCONNECTED, e.g. reconnect-buffer-full) still falls through to :else and rethrows raw — not misclassified as :drained
  done: true
- title: clj-kondo clean; suite green on JVM and Node — existing request-during-drain-window-rejects-drained and connection-closed-normalized tests stay green unchanged
  done: true
---

## Description

JVM `op-state-error` classifies drained-versus-closed by grepping jnats' exception message for `"Draining"`/`"Closed"`. A jnats reword (`"drain in progress"`, `"Connection closing"`) would make the JVM leg misclassify or fall through to `:else` and rethrow a raw `IllegalStateException`, breaking the retry-able signal.

Drive the classification from jnats' **structured connection state** instead.

### jnats API (verified 2026-06-05 against a live server, gated-handler reproduction)

jnats' `io.nats.client.Connection` exposes **no** `.isDraining`/`.isClosed` — those are nats.js methods, not jnats (the original framing was wrong). The only structured signal is `getStatus()` → `Connection$Status`, and that enum has **no `DRAINING` value**:

    DISCONNECTED, CONNECTED, CLOSED, RECONNECTING, CONNECTING

The two states are still distinguishable by status at throw time:

| moment                              | getStatus  | raw ISE message          | maps to            |
|-------------------------------------|------------|--------------------------|--------------------|
| request during the drain window     | CONNECTED  | "Connection is Draining" | `:drained`         |
| op after drain completes / close()  | CLOSED     | "Connection is Closed"   | `:connection-closed` |

A publish/request `IllegalStateException` only fires in those two situations plus the reconnect-buffer-full case (status RECONNECTING/DISCONNECTED, a different message), which today falls through to `:else` and rethrows raw — and must keep doing so.

So: `CLOSED → :connection-closed`, `CONNECTED → :drained`, anything else → rethrow raw. No message text is read; reword-proof.

### Scope

JVM-only (JS already reads structured state). A pure robustness refactor with **zero behavior change** on the pinned tests: an in-window request stays `:drained` (CONNECTED), a post-close op stays `:connection-closed` (CLOSED). Per ADR 0014, publish during drain returns nil (never throws the Draining ISE), so publish reaches `op-state-error` only via the CLOSED branch and the CONNECTED→`:drained` branch is request-only — but the one classifier still serves both ops.

Out of scope: `error/server-error-type`'s "Permissions Violation" grep — ADR 0006 accepts the server string as the only available signal (there the server, not jnats, owns the wording).

## Design

JVM-only. Verified call sites and scope below (`clj-surgeon`/REPL/jar grep, 2026-06-05, jnats 2.25.3).

### src/nats_cljc/impl/jvm.clj

`op-state-error` (currently L96) reads the message:

    (defn- op-state-error [subject ^Throwable e]
      (let [msg (str (.getMessage e))]
        (cond (str/includes? msg "Draining") <:drained ex-info, cause e>
              (str/includes? msg "Closed")   <:connection-closed ex-info, cause e>
              :else e)))

Replace with a status-keyed classifier (it needs the `Connection` to call `getStatus`):

    (defn- op-state-error [^Connection client subject ^Throwable e]
      (condp = (.getStatus client)
        Connection$Status/CLOSED    (ex-info "Connection is closed"   {:type :connection-closed :subject subject} e)
        Connection$Status/CONNECTED (ex-info "Connection is draining" {:type :drained :subject subject} e)
        e))   ;; RECONNECTING/DISCONNECTED/CONNECTING (e.g. reconnect-buffer-full): return e -> caller rethrows raw

- Import the nested enum: add `Connection$Status` to the `[io.nats.client ...]` `:import` vector (same nested form as the existing `ConnectionListener$Events`).
- Returning the original `e` for the non-CLOSED/CONNECTED case preserves the `(identical? x e)` rethrow contract both call sites rely on.
- Both call sites pass `client` (a `JvmConnection` record field, in scope in every method):
  - `-publish` L171: `(throw (op-state-error subject e))` → `(throw (op-state-error client subject e))`
  - `-request` L264: `(let [x (op-state-error subject e)] ...)` → `(op-state-error client subject e)`
- `clojure.string` (`str/includes?`) likely becomes unused after this — grep for other `str/` usage in the file and drop the require only if none remain (clj-kondo will flag it either way).
- Rewrite the docstring around the status table (drop the message-grep description).

### Why `CONNECTED → :drained` is sound (the load-bearing invariant — do not skip)

Out of context the branch reads alarmingly ("connected means draining?"). It is sound because of its **precondition**: `op-state-error` is called ONLY from inside `(catch IllegalStateException e ...)` at both sites, so it never asks "is this connection draining?" in the abstract — only "given that publish/request just threw an `IllegalStateException`, which kind was it?"

jnats' `NatsConnection` raises an ISE from the publish/request path at exactly **three** sites (strings verified in the jnats 2.25.3 class file):

| ISE message                                                        | jnats condition       | getStatus() at throw      |
|--------------------------------------------------------------------|-----------------------|---------------------------|
| `Connection is Closed`                                             | `isClosed()`          | CLOSED                    |
| `Connection is Draining`                                           | drain-block flag set  | CONNECTED (not yet closed)|
| `Unable to queue any more messages during reconnect, max buffer is N` | reconnect buffer full | RECONNECTING / DISCONNECTED |

The reconnect-buffer guard can only fire while NOT connected (a CONNECTED publish writes straight to the socket and never touches that buffer). So among the only three ISEs reachable here, a CONNECTED status uniquely selects the drain one. The `:else` returns `e`, so the reconnect-buffer ISE rethrows raw unchanged — matching today's behavior. (In practice the CONNECTED branch is request-only: per ADR 0014 a publish during drain returns nil and never throws, so only request reaches it.)

### Accepted nuance (state honestly; do not try to fix)

`getStatus()` is read a hair AFTER jnats stamped the exception (the message-based code reads throw-time wording). At the drain→closed flip an in-flight request could read CLOSED where the message said "Draining" → `:connection-closed` — the same sub-ms non-deterministic window ADR 0014 already documents, and a harmless direction (both mean "shutting down"). The only wrong-direction hole — a reconnect-buffer ISE whose status flips DISCONNECTED→CONNECTED between throw and read → mislabelled `:drained` — needs a reconnect to land inside a same-microtask gap and is effectively unreachable. The gated-handler test holds the window open, so it stays deterministic there. This reword-proof-for-slightly-stale-state trade is the deliberate point of the ticket.

### test/nats_cljc/core_test.cljc (JVM branch only)

The existing `request-during-drain-window-rejects-drained` (in-window → `:drained`, status CONNECTED) and `connection-closed-normalized` (close() → `:connection-closed`, status CLOSED) already pin the two live paths — they must stay green unchanged. That green run IS the no-behavior-change proof.

For the reword-independence AC, add a focused JVM-only unit test against the private fn via its var, `(#'nats-cljc.impl.jvm/op-state-error client subj ise)` (the ns already requires `[nats-cljc.impl.jvm :as impl]`):
- a CLOSED client (connect then `@(nats/close conn)`, reach `(:client conn)`) + an `IllegalStateException` with a NON-"Closed" message → still `:connection-closed`
- a CONNECTED client (freshly connected) + an `IllegalStateException` with a NON-"Draining" message → still `:drained`
Wrap in `#?(:clj ...)` with no CLJS branch — JS already reads structured state, so there is nothing to mirror.

### Verify

clj-kondo --lint src test ; JVM: clojure -X:test ; Node: npx shadow-cljs compile node && node target/node-tests.js

## Notes

**2026-06-05T17:08:33.894728175Z**

Corrected during nts-01kt87wgm75d (publish-during-drain) work. Original premise was wrong on two counts: (1) jnats has no .isDraining/.isClosed (nats.js methods, not jnats) and Connection$Status has no DRAINING value; (2) but getStatus IS a usable structural signal — verified live: in-window request throws while CONNECTED, post-close while CLOSED. So the fix stands (now feasible for BOTH branches via getStatus), just with the right API. Also narrowed: per ADR 0014 the CONNECTED→:drained branch is request-only now. Design + verified call sites filled in; ACs sharpened. Zero behavior change — pure robustness refactor.

**2026-06-05T18:57:24.244637387Z**

Rekeyed JVM op-state-error from jnats' exception message text to structured connection state (getStatus). The classifier is now (condp = (.getStatus client) CLOSED->:connection-closed, CONNECTED->:drained, else->original e), taking ^Connection as a new first arg; both call sites (-publish, -request) pass the in-scope client field. Imported Connection$Status; dropped the now-unused clojure.string require. A jnats reword can no longer flip the mapping, and a reconnect-buffer ISE (RECONNECTING/DISCONNECTED) still rethrows raw. Added a JVM-only unit test op-state-error-classifies-by-connection-status-not-message hitting the private fn with deliberately reworded messages across CLOSED/CONNECTED/RECONNECTING(proxy). Pure robustness refactor, zero behavior change: pinned request-during-drain-window-rejects-drained + connection-closed-normalized stay green. clj-kondo clean; JVM 101 tests / Node 77 tests, 0 failures.
