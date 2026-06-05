# Cutting a release

How to publish a new version to [Clojars](https://clojars.org/io.github.unisoma/nats-cljc)
and [cljdoc](https://cljdoc.org/d/io.github.unisoma/nats-cljc). The decisions
behind the coordinate, license, and versioning contract live in
[ADR 0009](../adr/0009-project-foundations-and-versioning.md); this is the
operational sequence.

## One-time prerequisites

- A [Clojars](https://clojars.org/) account that is a member of the **verified**
  `io.github.unisoma` group.
- A Clojars **deploy token** (Clojars → Settings → Deploy Tokens), exported into
  the shell that runs the deploy:
  ```sh
  export CLOJARS_USERNAME=<your-clojars-username>
  export CLOJARS_PASSWORD=<deploy-token>   # the token, NOT your account password
  ```

## Why the order matters

The pom embeds `<scm><tag>v<version></tag></scm>` (`build.clj`), and **cljdoc
fetches the articles** — README, CHANGELOG, CONTEXT, the ADRs, and
`docs/cljdoc.edn` — **from GitHub at that exact tag**, not from the jar. So the
`v<version>` tag must already be pushed and point at a commit containing the final
docs *before* cljdoc ingests the Clojars release. Hence: tag and push **before**
deploy.

## cljdoc must be able to load every analysed namespace

cljdoc documents a library by **`require`-ing each namespace** on a classpath built
from the *published pom* — runtime deps only (`clojure` + `jnats`). A namespace that
pulls a dependency we deliberately keep out of the pom — the opt-in `:json` /
`:transit` codecs need `data.json` / `transit-*` (ADR 0004's clean forced footprint)
— cannot load there and fails the **whole** doc build (it broke the `0.1.0` build;
fixed in `0.1.1`). Mark such namespaces `^:no-doc` at the ns level: cljdoc filters
those out *before* it tries to load them. Rule of thumb: if a new namespace requires
anything outside `deps.edn`'s `:deps`, it must be `^:no-doc`.

## Per-release steps

The version lives in two spots — `build.clj` (`version`) and `nats-cljc.core/version`
— plus the CHANGELOG heading. `version_test.clj` fails the suite if
`nats-cljc.core/version` and the latest CHANGELOG heading disagree, so a half-done
bump can't silently ship. (`clj -T:build deploy` derives the jar path from
`build.clj`'s `version`, so there's no separate artifact path to keep in sync.)

1. **Bump the version** in `build.clj` (`version`) and `src/nats_cljc/core.cljc`
   (`version`); add the new `## [x.y.z] - <date>` section to `CHANGELOG.md` (move
   items out of `## [Unreleased]`; update the bottom compare links).
2. **Lint + full suite, both legs** — the guard test passes once the two `version`
   constants and the CHANGELOG heading agree:
   ```sh
   clj-kondo --lint src test
   clojure -X:test                 # JVM
   npx shadow-cljs compile node && node target/node-tests.js   # Node
   ```
3. **Verify the cljdoc article paths** resolve:
   ```sh
   curl -fsSL https://raw.githubusercontent.com/cljdoc/cljdoc/master/script/verify-cljdoc-edn | bash -s docs/cljdoc.edn
   ```
4. **Commit** the version bump + CHANGELOG.
5. **Tag** the release commit: `git tag v<version>`.
6. **Push commit + tag**: `git push && git push --tags` — the articles must exist
   at the tag before cljdoc looks.
7. **Build the jar**: `clojure -T:build jar` (the pom now carries `<tag>v<version>`
   and the SCM block).
8. **Deploy to Clojars**: `clojure -T:build deploy` (reads `CLOJARS_USERNAME` /
   `CLOJARS_PASSWORD` from the env; deps-deploy reads coordinates from the pom that
   `jar` wrote under `target/classes`; unsigned — Clojars no longer requires signing).
9. **Verify**: cljdoc polls Clojars (~60s) and builds at the tag — check
   `https://cljdoc.org/d/io.github.unisoma/nats-cljc/<version>` and that the
   README badges resolve. Trigger the build manually from the cljdoc page if it
   hasn't appeared.
