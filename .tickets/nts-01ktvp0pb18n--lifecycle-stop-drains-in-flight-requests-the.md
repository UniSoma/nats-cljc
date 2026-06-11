---
id: nts-01ktvp0pb18n
title: 'Lifecycle: stop drains in-flight requests, the :stopped promise'
status: in_progress
type: feature
priority: 2
mode: afk
created: '2026-06-11T15:50:05.402799533Z'
updated: '2026-06-11T16:59:55.979478787Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: (stop svc) returns a Promise that resolves after teardown
  done: false
- title: A request in flight when stop is called still receives its reply (drain verified with a deliberately slow handler)
  done: false
- title: The handle's :stopped promise resolves to nil once the Service stops
  done: false
- title: After stop, a request to the Service's endpoint rejects with :no-responders
  done: false
- title: Portable facade tests pass on both legs
  done: false
deps:
- nts-01ktvnzj8kwp
---

## Description

Graceful shutdown for a hosted Service. `(stop svc)` returns a Promise and drains in-flight requests before tearing down — a request being handled when stop is called still receives its reply, never dropped mid-request. The Service handle carries a `:stopped` promise that resolves (to nil) when the Service stops for any reason, paralleling the Watch handle's `:initialized` — react-to-shutdown without polling. No `reset` in v1.

The drain is verified with a deliberately slow handler: fire a request, call stop while it is in flight, and assert the caller still gets the reply. After stop, the Service no longer answers its endpoints and a caller sees the standard `:no-responders`.
