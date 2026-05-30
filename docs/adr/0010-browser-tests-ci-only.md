# Browser tests run in CI only

The portable `.cljc` suite targets three platforms (ADR 0001), but ClojureScript on Node and in the browser run the **same code over the same transport**: one impl namespace (`nats-cljc.impl.js`) wrapping `@nats-io/nats-core`'s `wsconnect` WebSocket client, which serves Node and the browser from one package (ADR 0003). Local Node therefore already exercises every line of the CLJS facade, codec, and impl plus the `ws://` path; the JVM leg covers TCP. The **only** thing the browser adds is a real browser's WebSocket and sandbox — and that we verify in **CI**, which runs the identical suite under headless Chrome against a websocket-enabled `nats-server` on every push and PR.

So local development runs **two of the three legs — JVM and Node** — and **does not provision a browser**.

## Considered options

- **Provision a headless browser in the dev image** — rejected. It buys little over the Node leg (same CLJS code, same transport) while costing real maintenance: image weight, a Debian `chromium` apt pin that rotates out of the security mirror (so the build breaks until the version is bumped), and a wrapper to work around the dev-image home overlay being mounted `nouserxattr`, which trips chromium's crashpad DB init (`chrome_crashpad_handler: --database is required` → SIGTRAP). High upkeep for a path Node already covers.
- **Drop the browser leg entirely** — rejected: the browser's native WebSocket/sandbox is the one CLJS surface Node can't stand in for, and it's a first-class target (ADR 0001). It stays — just in CI.

## Consequences

- A change to the CLJS path is trusted locally once Node is green, and confirmed on the browser by CI before merge. Contributors don't need a browser or GUI libraries.
- **CI is the single source of browser truth**, so its Chrome provisioning must stay reliable. The browser job currently leans implicitly on the runner image shipping Chrome; making that explicit is tracked separately.
- The portable suite itself is unchanged — one `.cljc` file still drives all three legs; only *where* each leg executes differs. See `docs/agents/running-tests.md`.
