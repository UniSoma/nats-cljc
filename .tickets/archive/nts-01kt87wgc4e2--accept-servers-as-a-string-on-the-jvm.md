---
id: nts-01kt87wgc4e2
title: Accept `:servers` as a string on the JVM
status: closed
type: bug
priority: 1
mode: afk
created: '2026-06-04T02:37:33.956744682Z'
updated: '2026-06-04T19:54:21.547905803Z'
closed: '2026-06-04T19:54:21.547905803Z'
tags:
- review
- connect
acceptance:
- title: '`(connect {:servers "wss://..."})` connects on the JVM with no array-mismatch throw, matching Node and browser'
  done: true
- title: A vector `:servers` continues to work unchanged
  done: true
- title: A test exercises the string form on both legs
  done: true
- title: clj-kondo clean; suite green on JVM and Node
  done: true
---

## Description

The README quick-start documents `:servers` as a string (the one non-portable value) — `{:servers "wss://demo.nats.io:8443"}` — and nats.js normalizes a string to a one-element list, but JVM connect does `(.servers (into-array String servers))`, which seqs a string into characters and throws `IllegalArgumentException "array element type mismatch"`.

Normalize a string `:servers` to a one-element vector on the JVM before building the server list, so the documented quick-start works identically on both legs. Add a test passing a string `:servers` on both legs.

## Notes

**2026-06-04T19:54:21.547905803Z**

Normalized a bare-string :servers to a one-element vector inside the JVM connect supplier ((cond-> servers (string? servers) vector)), before (into-array String ...) seqs the string into chars and throws 'array element type mismatch'. nats.js already normalizes a string to a one-element list, so both legs now accept the README quick-start string form; vector :servers unchanged. Test-first: new connect-accepts-string-servers deftest runs the string form on both legs (RED reproduced the mismatch at jvm.clj:372, GREEN after). clj-kondo clean; JVM 90 / Node 67 tests green.
