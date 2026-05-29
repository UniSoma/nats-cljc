---
id: nts-01kstx9xghet
title: Headers (HTTP-style, normalized)
status: open
type: feature
priority: 1
mode: afk
created: '2026-05-29T22:22:31.441033681Z'
updated: '2026-05-29T22:34:55.151010465Z'
acceptance:
- title: Publishing `:headers` with a scalar value delivers it as a one-element vector of strings
  done: false
- title: Publishing vector-valued headers delivers them unchanged as vectors of strings
  done: false
- title: Header names are preserved case-sensitively
  done: false
- title: '`:headers` is absent from the delivered map when none were set; verified on all three platforms'
  done: false
deps:
- nts-01kstx8ysgv5
---

## Description

HTTP-style message headers: case-sensitive string names mapping to one or more string values. On publish a scalar value is accepted and normalized to a one-element vector; on delivery headers arrive as `name -> vector-of-strings` under `:headers` (present only when set).

CONTEXT: Headers.
