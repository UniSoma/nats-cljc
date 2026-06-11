---
id: nts-01ktvnzz2ejg
title: 'Service errors: respond-error, auto-500, and the service/error reader'
status: open
type: feature
priority: 2
mode: afk
created: '2026-06-11T15:49:41.576355891Z'
updated: '2026-06-11T15:49:41.576355891Z'
parent: nts-01ktvn87why4
tags:
- services
- phase-4
acceptance:
- title: (respond-error conn msg code description data?) reaches the caller as a reply whose (service/error msg) is {:code … :description …}; a success reply reads nil
  done: false
- title: A handler that throws or returns a rejected promise auto-replies code 500 with the exception's description, on both legs
  done: false
- title: core/request resolves normally (does not reject) on a service error reply
  done: false
- title: A request to a subject no Service hosts rejects with the normalized :no-responders Error
  done: false
- title: Portable facade tests pass on both legs
  done: false
deps:
- nts-01ktvnzj8kwp
---

## Description

ADR 0025 end-to-end: service application errors are reply payloads, not normalized transport Errors.

Server side: `(respond-error conn msg code description data? opts?)` sends a first-class structured error reply (`code` is an integer), conn threaded as in `respond`. A handler that throws or returns a rejected promise auto-replies a service error — code 500, description from the exception — so a bug never leaves a caller hanging until timeout. The native framework counts it in that endpoint's error stats; the counter itself is asserted in the discovery slice, where stats become readable.

Caller side: `core/request` is unchanged and must not learn about `Nats-Service-Error` headers — it resolves normally with the reply Message even when the Service answered with an error, so an application error is data the caller branches on, not a thrown transport failure. `(service/error msg)` returns `nil` or `{:code … :description …}` from a reply Message — the opt-in reader. `:no-responders` remains a normalized Error when nobody hosts the subject.
