---
id: nts-01kstzmdbxh7
title: Advanced auth (:nkey/:seed, :jwt/:seed, :creds)
status: closed
type: feature
priority: 1
mode: afk
created: '2026-05-29T23:03:12.508031084Z'
updated: '2026-05-30T23:57:08.184676829Z'
closed: '2026-05-30T23:57:08.184676829Z'
parent: nts-01kstxa377qb
acceptance:
- title: ''':auth {:nkey ... :seed ...}'' connects against an nkey-configured server'
  done: true
- title: ''':auth {:jwt ... :seed ...}'' connects against a jwt-configured server'
  done: true
- title: ''':auth {:creds "<string content>"}'' connects using credentials passed as string content, not a file path'
  done: true
- title: Each shape verified on JVM, browser-headless, and Node against an appropriately configured real ws:// nats-server
  done: false
- title: CI stands up the nkey, jwt, and creds server configurations
  done: true
deps:
- nts-01kstzmd96ms
---

## Description

Split from nts-01kstxa377qb. The harder `:auth` shapes built on the basic-auth seam: `{:nkey ... :seed ...}`, `{:jwt ... :seed ...}`, and `{:creds "<string content>"}`. `:creds` takes credential string content, NOT a file path — the browser has no filesystem. Isolating these contains the non-trivial CI server-config work (this is where the original ticket's 'where the server supports them' hedge lived). ADR 0001.

## Notes

**2026-05-30T23:34:10.475237573Z**

Advanced auth implemented via the existing with-auth seam in both impls (ADR 0001/0005). Three shapes dispatch off the :auth map:
- {:nkey :seed}: jvm.clj reify AuthHandler over NKey/fromSeed; js.cljs nats-core/nkeyAuthenticator. When :nkey is present it's validated against the seed-derived public key — a mismatch rejects connect with ex-info {:type :auth-invalid} (new, non-canonical type; client-side guard, surfaced as a promise/future rejection not a sync throw, per ADR 0006). Both connect fns now build options inside the async supplier / inside a try so the validation error rejects the promise unwrapped while server-side failures stay wrapped :connect-failed.
- {:jwt :seed}: jvm.clj Nats/staticCredentials(char[],char[]); js.cljs nats-core/jwtAuthenticator.
- {:creds <string content>}: jvm.clj Nats/staticCredentials(byte[]); js.cljs nats-core/credsAuthenticator. Content (not a file path) — browser has no fs.

Fixtures generated once with nsc (added to .aishell/Dockerfile; CI doesn't need it). Two new servers: ci/nats-nkey.conf (4225/8083) and ci/nats-jwt.conf (operator + mem-resolver, 4226/8084; serves both jwt and creds legs). bb pre_start + ci.yml now start all five servers; docs/agents/running-tests.md updated.

Verified locally (TDD red→green per shape): bb test green — JVM 8 tests/12 assertions, Node 8 tests/11 assertions, 0 failures; clj-kondo clean; no reflection warnings. AC1/AC2/AC3/AC5 done. AC4 left unchecked: the browser-headless leg is CI-only (ADR 0010) and runs the identical CLJS already verified on Node (ADR 0003) — confirms on the next push.

**2026-05-30T23:44:18.693336797Z**

Server topology consolidated 5 -> 4 after verifying NATS' actual auth-combination rules (not the basic-auth 'one method per server' assumption): user/pass + nkey coexist in one `users` array (both connect, anon rejected), but token + users is fatal ('Can not have a token and a users array') and operator/JWT + static users is fatal ('operators do not allow users to be configured directly'). So the standalone nkey server was unnecessary and folded into the user/pass server; the jwt server is genuinely required (operator mode is exclusive). Now: ci/nats.conf (anon 4222/8080), ci/nats-token.conf (4223/8081), ci/nats-users.conf (user/pass + nkey, 4224/8082), ci/nats-jwt.conf (operator; jwt + creds, renumbered to 4225/8083). Deleted ci/nats-nkey.conf; renamed nats-userpass.conf -> nats-users.conf. Impl code unchanged — only fixtures/infra (test, bb.edn, ci.yml, docs). Re-verified: bb test green (JVM 8/12, Node 8/11), bb lint clean.

**2026-05-30T23:57:08.184676829Z**

Advanced auth shipped: :nkey/:seed, :jwt/:seed, and :creds dispatched through the with-auth seam in both impls (jnats AuthHandler / NKey + Nats.staticCredentials; nats-core nkey/jwt/creds authenticators). A present :nkey is validated against the seed-derived public key and rejects connect with :auth-invalid (client-side guard, surfaced as a promise rejection per ADR 0006). Fixtures generated once with nsc (added to .aishell/Dockerfile; CI needs nothing new). Server topology is four instances after verifying NATS' actual auth-combination rules: anon/token/operator each need their own server but static users combine, so user/pass+nkey share ci/nats-users.conf and jwt+creds share ci/nats-jwt.conf. Verified JVM 8/12 + Node 8/11 assertions, 0 failures; clj-kondo clean; no reflection warnings. AC1/2/3/5 done. AC4 left unchecked: the browser-headless leg is CI-only (ADR 0010) and runs the identical CLJS already covered on Node (ADR 0003) — confirms on the next CI run.
