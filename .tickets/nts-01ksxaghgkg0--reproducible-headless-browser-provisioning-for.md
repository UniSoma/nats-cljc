---
id: nts-01ksxaghgkg0
title: Make the CI browser-test job's Chrome provisioning explicit
status: open
type: task
priority: 2
mode: afk
created: '2026-05-30T20:51:48.881664312Z'
updated: '2026-05-30T21:03:14.729163797Z'
tags:
- infra
- ci
- browser-test
acceptance:
- title: The browser CI job provisions Chrome via an explicit setup/install step (not the runner image default), ideally version-pinned
  done: false
- title: The browser-headless karma suite is green in CI after the change
  done: false
links:
- nts-01kstx8ysgv5
---

## Description

Per ADR 0010, the browser-headless leg runs only in CI, so CI is the single source of browser truth. The browser job in `.github/workflows/ci.yml` has **no Chrome-install step** — it leans implicitly on `ubuntu-latest` shipping `google-chrome-stable`, which karma-chrome-launcher discovers via PATH/CHROME_BIN. Make that dependency explicit (a setup/install step — e.g. `browser-actions/setup-chrome`, or apt — ideally version-pinned) so the only place the browser is ever verified cannot silently break when the runner image changes.

Context: local dev provisions no browser (JVM + Node only); rationale and the Node/browser code-parity argument are in ADR 0010, run commands in `docs/agents/running-tests.md`. Background on why local provisioning was rejected (Debian chromium apt-pin churn; dev-image home overlay mounted `nouserxattr` trips chromium crashpad init -> SIGTRAP) is preserved in the ticket history.
