---
id: nts-01kt87wg6qvj
title: Validate Header tokens before the native call
status: closed
type: bug
priority: 1
mode: afk
created: '2026-06-04T02:37:33.783090137Z'
updated: '2026-06-04T17:51:13.853837144Z'
closed: '2026-06-04T17:51:13.853837144Z'
tags:
- review
- headers
acceptance:
- title: Publishing with an invalid header name such as `"Bad:Name"` rejects with `{:type :invalid-header}` on both legs, never `:max-payload-exceeded`
  done: true
- title: A non-ASCII header value is handled identically on both legs
  done: true
- title: A valid header (printable-ASCII name, scalar or vector value) still publishes and round-trips unchanged
  done: true
- title: clj-kondo clean; suite green on JVM and Node
  done: true
links:
- nts-01kt87whe06m
---

## Description

`normalize-headers` validates only `string?`, never token structure, so an invalid header name (e.g. one containing a colon) passes through and the native throw is caught by publish's max-payload handler and mislabeled `:max-payload-exceeded` — a payload-size error for a header-name typo.

Validate header names as printable-ASCII tokens (no colon, no control chars) in `normalize-headers` and throw `:invalid-header` before jnats/nats.js see them, on both legs. Also pin a single cross-leg rule for header values: today a non-ASCII value is rejected by jnats but published by nats.js — reject non-printable-ASCII (and CR/LF) values uniformly in `normalize-headers` so both legs agree (the publish docstring already promises printable-ASCII tokens).

Note: `:invalid-header` is a caller-misuse type currently outside the canonical error set — its home is decided in the linked contract ticket.

## Notes

**2026-06-04T17:51:13.853837144Z**

Validated header tokens in normalize-headers (shared cljc) before the native call, test-first, two RED→GREEN cycles. Names must be non-empty printable-ASCII with no colon (#"[\x20-\x39\x3B-\x7E]+"); values must be printable ASCII, CR/LF and non-ASCII rejected (#"[\x20-\x7E]*"). Both throw a portable :type :invalid-header on every leg, so a header-name typo no longer leaks to the client (was mislabeled / raw native throw) and a non-ASCII value no longer diverges (jnats rejected, nats.js published). New tests headers-invalid-name-rejected + headers-non-ascii-value-rejected green on JVM+Node; existing happy-path header round-trips unchanged. Suite green JVM 87/189, Node 66/123; clj-kondo clean. Note: :invalid-header home still pending in linked nts-01kt87whe06m.
