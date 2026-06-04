---
id: nts-01kt87wgszvg
title: Pin `:creds` auth to UTF-8
status: open
type: bug
priority: 2
mode: afk
created: '2026-06-04T02:37:34.399920643Z'
updated: '2026-06-04T02:53:24.785217030Z'
tags:
- review
- auth
acceptance:
- title: The JVM `:creds` path encodes credentials as UTF-8 regardless of `file.encoding`, matching the JS leg
  done: false
- title: clj-kondo clean; suite green on JVM and Node
  done: false
---

## Description

The JVM `:creds` auth path uses `(.getBytes ^String creds)` with no charset — platform-default — the lone unqualified `.getBytes` in the codebase, while `codec/str->bytes` is pinned to UTF-8 and the JS `:creds` path uses TextEncoder (always UTF-8). On a JVM whose `file.encoding` is not UTF-8, a creds blob with non-ASCII bytes yields platform-default bytes to `Nats/staticCredentials`, diverging from the JS leg.

Pass `StandardCharsets/UTF_8` explicitly.
