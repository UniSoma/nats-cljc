---
id: nts-01ktvnzz2ejg
title: 'Service errors: respond-error, auto-500, and the service/error reader'
status: closed
type: feature
priority: 2
mode: afk
created: '2026-06-11T15:49:41.576355891Z'
updated: '2026-06-11T16:42:48.052604318Z'
closed: '2026-06-11T16:42:48.052604318Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: (respond-error conn msg code description data?) reaches the caller as a reply whose (service/error msg) is {:code … :description …}; a success reply reads nil
  done: true
- title: A handler that throws or returns a rejected promise auto-replies code 500 with the exception's description, on both legs
  done: true
- title: core/request resolves normally (does not reject) on a service error reply
  done: true
- title: A request to a subject no Service hosts rejects with the normalized :no-responders Error
  done: true
- title: Portable facade tests pass on both legs
  done: true
deps:
- nts-01ktvnzj8kwp
---

## Description

ADR 0025 end-to-end: service application errors are reply payloads, not normalized transport Errors.

Server side: `(respond-error conn msg code description data? opts?)` sends a first-class structured error reply (`code` is an integer), conn threaded as in `respond`. A handler that throws or returns a rejected promise auto-replies a service error — code 500, description from the exception — so a bug never leaves a caller hanging until timeout. The native framework counts it in that endpoint's error stats; the counter itself is asserted in the discovery slice, where stats become readable.

Caller side: `core/request` is unchanged and must not learn about `Nats-Service-Error` headers — it resolves normally with the reply Message even when the Service answered with an error, so an application error is data the caller branches on, not a thrown transport failure. `(service/error msg)` returns `nil` or `{:code … :description …}` from a reply Message — the opt-in reader. `:no-responders` remains a normalized Error when nobody hosts the subject.

## Notes

**2026-06-11T16:42:48.052604318Z**

Shipped service/respond-error (conn msg code description data? opts?), service/error reader (nil | {:code :description}), and auto-500 on a thrown/rejected handler; added -respond-error to the Service protocol. core/request unchanged (resolves on a service-error reply, never sniffs headers); :no-responders stays a normalized Error. JVM auto-500 is free via jnats EndpointContext; JS impl awaits the returned promise to auto-500 on rejection (nats.js only does so on a sync throw). Verified: lint clean, JVM 723 / Node 626 assertions 0 failures, bundle:check + externs:check green; JS reject path watched red-before-green.
