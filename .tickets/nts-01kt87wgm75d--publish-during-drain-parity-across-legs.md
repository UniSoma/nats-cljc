---
id: nts-01kt87wgm75d
title: Publish-during-drain parity across legs
status: open
type: bug
priority: 2
mode: afk
created: '2026-06-04T02:37:34.215563795Z'
updated: '2026-06-04T02:53:24.590762929Z'
tags:
- review
- drain
acceptance:
- title: A publish issued during the drain window returns nil on both legs, no thrown `:drained`
  done: false
- title: A publish after the connection is fully closed still surfaces a closed/drained error as before
  done: false
- title: A drain-window publish test covers both legs; an ADR note records the contract
  done: false
- title: clj-kondo clean; suite green on JVM and Node
  done: false
---

## Description

A publish issued during the connection drain window diverges: jnats allows it (publish is fire-and-forget during drain) and returns nil, but nats.js rejects it and `-publish` maps that to a thrown `:drained` ex-info — so portable shutdown code that publishes while `(drain conn)` is in flight silently succeeds on the JVM and throws on Node/browser.

Make the legs agree by treating publish-during-drain as allowed/best-effort on both: on JS, swallow the draining-publish rejection and return nil to match jnats. Add a publish-during-drain test on both legs (the existing drain-window test issues a request, not a publish). Record the chosen contract in a short ADR note.
