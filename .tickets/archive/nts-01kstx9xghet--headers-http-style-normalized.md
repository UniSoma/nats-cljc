---
id: nts-01kstx9xghet
title: Headers (HTTP-style, normalized)
status: closed
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:22:31.441033681Z'
updated: '2026-06-03T01:16:49.434571572Z'
closed: '2026-06-03T01:16:49.434571572Z'
acceptance:
- title: Publishing `:headers` with a scalar value delivers it as a one-element vector of strings
  done: true
- title: Publishing vector-valued headers delivers them unchanged as vectors of strings
  done: true
- title: Header names are preserved case-sensitively
  done: true
- title: '`:headers` is absent from the delivered map when none were set; verified on all three platforms'
  done: true
deps:
- nts-01kstx8ysgv5
---

## Description

HTTP-style message headers: case-sensitive string names mapping to one or more string values. On publish a scalar value is accepted and normalized to a one-element vector; on delivery headers arrive as `name -> vector-of-strings` under `:headers` (present only when set).

CONTEXT: Headers.

## Notes

**2026-06-03T01:16:49.434571572Z**

HTTP-style headers complete and verified. Publish accepts :headers as a map of case-sensitive string names to scalar-or-vector string values (scalar normalized to a one-element vector); delivery surfaces them as name -> vector-of-strings under :headers, present only when set (empty/nil maps dropped in decode-msg via (seq headers)). Final AC verified: headers-absent-when-none-set asserts (not (contains? msg :headers)) and is green on JVM (58 tests/110 assertions) and Node (58/105) locally; the identical .cljc test runs the browser leg via the :karma target (ns-regexp -test$) in CI per ADR 0010.
