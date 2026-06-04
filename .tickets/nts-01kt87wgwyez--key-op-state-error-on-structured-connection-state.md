---
id: nts-01kt87wgwyez
title: Key `op-state-error` on structured connection state
status: open
type: bug
priority: 3
mode: afk
created: '2026-06-04T02:37:34.493956165Z'
updated: '2026-06-04T02:53:24.881989579Z'
tags:
- review
- error
acceptance:
- title: JVM drained-versus-closed classification is driven by structured connection state, not message text
  done: false
- title: A simulated jnats message reword no longer changes the classification
  done: false
- title: clj-kondo clean; suite green on JVM and Node
  done: false
---

## Description

JVM `op-state-error` classifies drained-versus-closed by grepping jnats' exception message for "Draining"/"Closed", while JS reads structured `(.isDraining client)`/`(.isClosed)`. A jnats reword ("drain in progress", "Connection closing") would make the JVM leg misclassify or fall through to `:else` and rethrow a raw `IllegalStateException`, breaking the retry-able signal.

Switch JVM `op-state-error` to query structured connection state (`.isDraining`/`.isClosed`) instead of message-string matching, matching the JS leg. Out of scope: `error/server-error-type`'s "Permissions Violation" grep — ADR 0006 accepts the server string as the only available signal.
