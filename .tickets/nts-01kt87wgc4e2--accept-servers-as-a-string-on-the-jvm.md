---
id: nts-01kt87wgc4e2
title: Accept `:servers` as a string on the JVM
status: open
type: bug
priority: 1
mode: afk
created: '2026-06-04T02:37:33.956744682Z'
updated: '2026-06-04T02:53:24.323296492Z'
tags:
- review
- connect
acceptance:
- title: '`(connect {:servers "wss://..."})` connects on the JVM with no array-mismatch throw, matching Node and browser'
  done: false
- title: A vector `:servers` continues to work unchanged
  done: false
- title: A test exercises the string form on both legs
  done: false
- title: clj-kondo clean; suite green on JVM and Node
  done: false
---

## Description

The README quick-start documents `:servers` as a string (the one non-portable value) — `{:servers "wss://demo.nats.io:8443"}` — and nats.js normalizes a string to a one-element list, but JVM connect does `(.servers (into-array String servers))`, which seqs a string into characters and throws `IllegalArgumentException "array element type mismatch"`.

Normalize a string `:servers` to a one-element vector on the JVM before building the server list, so the documented quick-start works identically on both legs. Add a test passing a string `:servers` on both legs.
