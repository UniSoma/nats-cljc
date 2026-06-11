---
id: nts-01ktw1tq610j
title: Portable endpoint :metadata contract
status: closed
type: bug
priority: 2
mode: afk
created: '2026-06-11T19:16:32.570749218Z'
updated: '2026-06-11T22:39:16.959575864Z'
closed: '2026-06-11T22:39:16.959575864Z'
tags:
- services
- review
acceptance:
- title: JS leg passes endpoint :metadata to the native addEndpoint
  done: true
- title: Host-side metadata serializes to the same wire form on both legs (string keys, no leading-colon artifacts)
  done: true
- title: Lifted metadata in ping/info/stats has one documented key shape, identical across legs
  done: true
- title: A portable facade test round-trips endpoint and service metadata through create -> info on JVM + Node, watched red against the current divergence
  done: true
links:
- nts-01ktvn87why4
---

## Description

The spec review found endpoint :metadata broken in two ways, with zero test coverage. (1) The JS leg drops it entirely: the endpoint destructuring never passes :metadata to addEndpoint, while the JVM leg sets it — silently divergent hosting. (2) The lifted shape diverges: discovery on the JVM produces string-keyed metadata maps while JS keywordizes, breaking the epic's 'byte-identical across legs' normalization story; on the host side the JVM passes the raw Clojure map to jnats (keyword keys would serialize with a leading colon) while JS goes through clj->js.

End-to-end: a Service created with endpoint :metadata on either leg exposes that metadata identically (same key shape, same values) through service/info on both legs.

## Notes

**2026-06-11T22:19:39.016231459Z**

Implemented: facade-level wire-metadata normalizer in nats-cljc.service (string keys/values, keyword -> name) applied to service- and endpoint-level :metadata before either impl; JS add-endpoint! now passes :metadata to addEndpoint; JS discovery lifts (ping/info/stats/info-endpoint) now lift :metadata string-keyed, matching the JVM. Contract documented in create + ping docstrings. New portable test metadata-round-trips-create-to-discovery watched red on both legs (JVM showed ":region" leading-colon artifact; Node showed keywordized lift + endpoint metadata dropped to nil), now green. Full suites green on JVM (clojure -X:test) and Node; clj-kondo clean. Not committed, not closed.

**2026-06-11T22:39:16.959575864Z**

Endpoint/service :metadata now has one portable wire shape across legs. A facade-level normalizer converts portable maps to string-keyed string-valued wire form (keywords contribute their name) before either impl hosts them; the JS leg now actually passes endpoint :metadata to addEndpoint and lifts discovery :metadata string-keyed to match the JVM. Contract documented in the create and ping docstrings; a portable round-trip test (create -> ping/info/stats) was watched red on both legs and is green. The JVM impl needed no code change.
