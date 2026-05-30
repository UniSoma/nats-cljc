# Running the tests

One portable `.cljc` suite (`test/nats_cljc/`) runs on every platform. **Locally you run the JVM and Node legs; the browser leg is CI-only** (ADR 0010).

## Prerequisite: websocket-enabled nats-servers

Every leg talks to a real server — no mocks. NATS forces anonymous, token, and operator/JWT auth onto separate servers, but static users combine, so four servers cover every leg:

```bash
nats-server -c ci/nats.conf       &   # anonymous            — TCP :4222 / ws :8080
nats-server -c ci/nats-token.conf &   # token                — TCP :4223 / ws :8081
nats-server -c ci/nats-users.conf &   # user/pass + nkey     — TCP :4224 / ws :8082
nats-server -c ci/nats-jwt.conf   &   # jwt + creds          — TCP :4225 / ws :8083
```

`ci/nats-users.conf` holds both a password user and an nkey user in one `users` array; `ci/nats-jwt.conf` is operator mode and serves both the jwt and creds legs. `bb pre_start` starts all of these idempotently. `nats-server` ships in the dev image. The JVM leg uses TCP; Node uses `ws://` (ADR 0001).

The nkey / JWT / creds fixtures (server configs in `ci/`, matching seeds/JWTs/creds in the test) were generated once with [`nsc`](https://github.com/nats-io/nsc) — it's in the dev image (`.aishell/Dockerfile`). CI does not need `nsc`; it only starts `nats-server` against the checked-in configs. To regenerate: `nsc generate nkey --user` for the standalone nkey, and an operator → account → user chain (`nsc add operator/account/user`, then `nsc generate config --mem-resolver` and `nsc generate creds`) for the JWT/creds leg.

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
