# Running the tests

One portable `.cljc` suite (`test/nats_cljc/`) runs on every platform. **Locally you run the JVM and Node legs; the browser leg is CI-only** (ADR 0010).

## Prerequisite: a websocket-enabled nats-server

Every leg talks to a real server — no mocks. Start one with the CI config (TCP `:4222` + `ws://:8080`):

```bash
nats-server -c ci/nats.conf &
```

`nats-server` ships in the dev image. The JVM leg uses TCP; Node uses `ws://` (ADR 0001).

## JVM (TCP)

```bash
clojure -X:test
```

## Node (WebSocket)

```bash
npx shadow-cljs compile node      # -> target/node-tests.js
node target/node-tests.js
```

Node exercises the full CLJS path — facade, codec, and `nats-cljc.impl.js` over `@nats-io/nats-core` `wsconnect` — which is byte-for-byte the code the browser runs (ADR 0003). That's why local Node coverage is enough and we don't provision a browser locally.

## Browser (CI only)

The `:karma` target (headless Chrome over `ws://`) runs in CI, not locally — see ADR 0010 for the rationale. The commands the CI job runs are in `.github/workflows/ci.yml`.

## Lint

```bash
clj-kondo --lint src test
```
