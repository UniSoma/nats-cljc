---
id: nts-01ksxaghgkg0
title: Make the CI browser-test job's Chrome provisioning explicit
status: closed
type: task
priority: 2
mode: afk
created: '2026-05-30T20:51:48.881664312Z'
updated: '2026-05-30T21:42:54.611820095Z'
closed: '2026-05-30T21:37:24.861753695Z'
tags:
- infra
- ci
- browser-test
acceptance:
- title: The browser CI job provisions Chrome via an explicit setup/install step (not the runner image default), ideally version-pinned
  done: true
- title: The browser-headless karma suite is green in CI after the change
  done: true
links:
- nts-01kstx8ysgv5
---

## Description

Per ADR 0010, the browser-headless leg runs only in CI, so CI is the single source of browser truth. The browser job in `.github/workflows/ci.yml` has **no Chrome-install step** — it leans implicitly on `ubuntu-latest` shipping `google-chrome-stable`, which karma-chrome-launcher discovers via PATH/CHROME_BIN. Make that dependency explicit (a setup/install step — e.g. `browser-actions/setup-chrome`, or apt — ideally version-pinned) so the only place the browser is ever verified cannot silently break when the runner image changes.

Context: local dev provisions no browser (JVM + Node only); rationale and the Node/browser code-parity argument are in ADR 0010, run commands in `docs/agents/running-tests.md`. Background on why local provisioning was rejected (Debian chromium apt-pin churn; dev-image home overlay mounted `nouserxattr` trips chromium crashpad init -> SIGTRAP) is preserved in the ticket history.

## Notes

**2026-05-30T21:29:08.132709338Z**

Implemented: added an explicit 'Set up Chrome' step (browser-actions/setup-chrome@v2) pinned to chrome-version '149' (current stable major; action resolves latest patch within it), gated on matrix.platform=='browser'. Wired CHROME_BIN=${{ steps.setup-chrome.outputs.chrome-path }} into the karma step so karma-chrome-launcher uses the provisioned binary rather than discovering one on PATH. Pinned to the major (not a full version) for reproducible behavior that survives Chrome-for-Testing patch removals. Action kept on the @v2 tag to match existing tag-pin style in the workflow. AC1 done. AC2 (karma green) can only be confirmed by a CI run — browser leg is CI-only per ADR 0010, not runnable locally.

**2026-05-30T21:37:24.861753695Z**

Browser CI job now provisions Chrome explicitly: a version-pinned 'Set up Chrome' step (browser-actions/setup-chrome@v2, chrome-version '149') replaces the implicit reliance on ubuntu-latest's bundled Chrome, with CHROME_BIN wired into the karma step so karma-chrome-launcher uses the provisioned binary. AC1 (explicit, version-pinned provisioning) complete. AC2 (karma suite green) is left unchecked deliberately: the browser leg is CI-only per ADR 0010 and not runnable locally, so it will be confirmed by the next CI run on push — reopen if that run is red.

**2026-05-30T21:42:54.611820095Z**

CI run on push is all green, including the browser-headless karma leg with the explicit version-pinned setup-chrome step. AC2 confirmed.
