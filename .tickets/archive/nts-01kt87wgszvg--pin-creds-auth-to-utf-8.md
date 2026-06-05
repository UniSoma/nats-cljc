---
id: nts-01kt87wgszvg
title: Pin `:creds` auth to UTF-8
status: closed
type: bug
priority: 2
mode: afk
created: '2026-06-04T02:37:34.399920643Z'
updated: '2026-06-05T00:52:13.645185549Z'
closed: '2026-06-05T00:52:13.645185549Z'
tags:
- review
- auth
acceptance:
- title: The JVM `:creds` path encodes credentials as UTF-8 regardless of `file.encoding`, matching the JS leg
  done: true
- title: clj-kondo clean; suite green on JVM and Node
  done: true
---

## Description

The JVM `:creds` auth path uses `(.getBytes ^String creds)` with no charset — platform-default — the lone unqualified `.getBytes` in the codebase, while `codec/str->bytes` is pinned to UTF-8 and the JS `:creds` path uses TextEncoder (always UTF-8). On a JVM whose `file.encoding` is not UTF-8, a creds blob with non-ASCII bytes yields platform-default bytes to `Nats/staticCredentials`, diverging from the JS leg.

Pass `StandardCharsets/UTF_8` explicitly.

## Notes

**2026-06-05T00:52:13.645185549Z**

Pinned the JVM :creds auth path to UTF-8: added a [java.nio.charset StandardCharsets] import to impl/jvm.clj and passed StandardCharsets/UTF_8 explicitly to (.getBytes ^String creds ...), so Nats/staticCredentials now receives the same bytes the JS leg's TextEncoder produces regardless of the JVM's file.encoding. Was the lone unqualified .getBytes in the codebase. clj-kondo clean (0/0); JVM (95 tests/201 assertions) and Node (72 tests/129 assertions) suites green.
