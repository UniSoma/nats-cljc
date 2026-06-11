---
id: nts-01ktw1v1n20p
title: Discovery :id-only narrowing parity
status: closed
type: bug
priority: 2
mode: afk
created: '2026-06-11T19:16:43.283895355Z'
updated: '2026-06-11T22:02:37.567692033Z'
closed: '2026-06-11T22:02:37.567692033Z'
tags:
- services
- review
acceptance:
- title: :id without :name returns exactly the matching instance(s) on both legs, via broadcast + client-side filter
  done: true
- title: JS no longer builds a control subject with an empty name token
  done: true
- title: Facade docstring states the story-20 contract (narrow by :name, :id, or both)
  done: true
- title: A portable test narrows by :id alone with two instances of the same Service running, watched red on each leg first
  done: true
deps:
- nts-01ktw1t03s6y
links:
- nts-01ktvn87why4
---

## Description

The epic's story 20 says a consumer can narrow any discovery call by :name and :id, but :id without :name diverges per leg: the JVM silently ignores it and broadcasts (its narrowing cond has no id-only arm), while JS interpolates an empty name into the control subject, producing a malformed token. The facade docstring quietly narrowed the contract to ':id (with :name)' instead.

Honor :id-only identically on both legs — broadcast plus client-side filter on the :id is the spec-faithful route, since the $SRV control subjects only encode name.id — and widen the facade docstring back to the story's contract.

Depends on the JVM Discovery offload slice — same code area.

## Notes

**2026-06-11T21:54:12.399134465Z**

Implemented :id-only narrowing parity: both legs broadcast and filter client-side on the instance :id (narrow-id in each impl); JS verbs no longer interpolate empty control-subject tokens; facade ping docstring widened to ':id alone or together with :name'. New portable test discovery-narrows-by-id-alone (two instances of one Service) watched red on both legs first — JVM returned both instances, Node rejected with 'control subject name name required'. Lint clean; JVM 236/808 green, Node 206/704 green. Not committed (review step follows).

**2026-06-11T22:02:37.567692033Z**

Both legs now honor :id without :name identically via broadcast plus client-side filter (private narrow-id helper in each impl), since the $SRV control subjects only encode name[.id]. JS verbs no longer interpolate empty control-subject tokens, and the facade ping docstring is widened back to ':id alone or together with :name'. A portable test narrows by :id alone across two instances of one Service, watched red on both legs first. Shipped in 90f4338.
