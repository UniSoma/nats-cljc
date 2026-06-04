# Writing tests

How to *author* a test here. For how to *run* one (servers, JVM/Node commands, lint), see [running-tests.md](running-tests.md).

One portable `.cljc` suite (`test/nats_cljc/`) is the whole story: the same `deftest` compiles and runs on the JVM and on Node (browser is CI-only, ADR 0010). Every test talks to a **real** ws-enabled `nats-server` — there are no mocks.

## One suite, two legs (the `#?` fork)

A test namespace pulls `clojure.test` on the JVM and `cljs.test` on Node via a reader conditional, plus the platform impl under one alias:

```clojure
(ns nats-cljc.core-test
  (:require #?(:clj  [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is async]])
            [nats-cljc.core :as nats]
            #?(:clj  [nats-cljc.impl.jvm :as impl]
               :cljs [nats-cljc.impl.js :as impl])
            #?(:cljs [promesa.core :as p])))
```

Each behavior is **one** `deftest` whose body forks on `#?`: the `:clj` leg is synchronous (`promise` + `(deref p 5000 ::timeout)`), the `:cljs` leg is asynchronous (`(async done …)` + promesa `p/deferred` / `p/resolve!` / `p/timeout`). The two legs assert the same thing; only the concurrency plumbing differs.

```clojure
(deftest publish-subscribe-round-trip
  #?(:clj
     (with-conn {:servers [server-url]}
       (fn [conn]
         (let [received (promise)
               sub      (nats/subscribe conn subject #(deliver received %))]
           (nats/publish conn subject payload)
           (let [msg (deref received 5000 ::timeout)]
             (is (= payload (:data msg)) "handler receives EDN-decoded :data")))))
     :cljs
     (async done
            (with-conn {:servers [server-url]} done
              (fn [conn]
                (let [received (p/deferred)]
                  (nats/subscribe conn subject #(p/resolve! received %))
                  (nats/publish conn subject payload)
                  (-> (p/timeout received 5000)
                      (p/then (fn [msg]
                                (is (= payload (:data msg)) "handler receives EDN-decoded :data")))))))))))
```

## Real servers → a distinct subject per test

Because the legs share a handful of long-lived servers, two tests on the same subject would cross-feed. Give each behavior its **own** subject, declared as a `^:private` fixture at the top of the file and commented with the ADR/CONTEXT rationale:

```clojure
;; Unsubscribe subjects (ADR 0012). Distinct per behavior so the shared server
;; doesn't cross-feed between tests.
(def ^:private unsub-subject "unsub.abrupt")
(def ^:private unsub-max-subject "unsub.max")
```

## Reuse the shared scaffolding

The connect/settle/teardown envelope is already factored out — don't re-roll it. The per-leg fork stays at the **call site** (each test writes its own blocking vs. async body); only the boilerplate is shared:

- **`with-conn`** — `(opts f)` on the JVM, `(opts done f)` on CLJS. Connects, runs `(f conn)`, closes in a `finally`; on CLJS it then calls `done`.
- **`status-collector` / `error-collector`** — return `[atom handler]`; hand the handler to `:on-status` / `:on-error` and assert against the atom.
- **`wait-for`** — a JVM poll and its CLJS promise twin, to *await* a status `:type` rather than race it (status events arrive on the client's own schedule).
- **`close!`** — test-only teardown that closes the native client.

Tests that expect `connect` *itself* to reject (connect-failed, mismatched-nkey) or need gated teardown (slow-consumer, drain-window) keep their own shape instead of `with-conn`.

## Asserting errors: `(:type (ex-data e))`

Portable consumer code reads the canonical `:type` off the ex-info rather than branching on host exception types — so tests assert exactly that. Catch `clojure.lang.ExceptionInfo` on the JVM and `:default` on CLJS, and return the `:type`:

```clojure
(is (= :invalid-header
       (try (nats/publish conn "headers.invalid" payload {:headers {"Bad:Name" "x"}})
            :no-throw
            (catch #?(:clj clojure.lang.ExceptionInfo :cljs :default) e
              (:type (ex-data e)))))
    "a header name containing a colon is rejected as :invalid-header")
```

The `:no-throw` sentinel makes a missing throw fail the `=` instead of silently passing.

## Pure verbs run on both legs for free

A verb with no connection — like the `subject` builder — needs **no** fork at all: a plain `(deftest … (is …))` with no `#?`, no `async`, no server still runs on both legs straight from the `.cljc`. Prefer this shape whenever the unit under test is pure.

```clojure
(deftest subject-builds-the-documented-example
  (is (= "orders.123.created" (nats/subject "orders" "123" "created"))
      "(subject \"orders\" id \"created\") dot-joins its parts"))
```

## The JVM-only blocking layer

Pull subscriptions are JVM-only (CONTEXT: *Pull subscription*), so their tests live in `test/nats_cljc/blocking/core_test.clj` — a plain `.clj`, fully synchronous, no reader conditionals.

## The TDD loop

1. **Map the file first.** `core_test.cljc` is ~1700 lines — `clj-surgeon :op :ls :file <path>` before reading (the [AGENTS.md](../../AGENTS.md) hard rule), then `Read` only the ranges you need.
2. **Red → green one test at a time** against the warm REPL ([clojure-repl-evaluation.md](clojure-repl-evaluation.md)) — reload and run just the new var, no full-suite cold start:

   ```bash
   clj-nrepl-eval -p 7888 '(do (require (quote nats-cljc.core) :reload)
                                (require (quote nats-cljc.core-test) :reload)
                                (clojure.test/run-test-var (var nats-cljc.core-test/my-new-test)))'
   ```

   Reaching localhost/servers needs the Bash sandbox disabled.
3. **Verify before commit** — full suite on **both** legs and lint clean ([running-tests.md](running-tests.md), [linting-and-formatting.md](linting-and-formatting.md)). A pure-`.cljc` test only proves out on a leg once that leg's full suite has run.

## Checklist

- [ ] One `deftest` per behavior; forked with `#?` only if it touches a connection
- [ ] Distinct, `^:private`, rationale-commented subject per test
- [ ] Reuse `with-conn` / collectors / `wait-for`; fork at the call site, not in the helper
- [ ] Errors asserted via `(:type (ex-data e))`, `clojure.lang.ExceptionInfo` / `:default`
- [ ] Every `is` carries a behavior-describing message
- [ ] Pure verbs tested without a fork
