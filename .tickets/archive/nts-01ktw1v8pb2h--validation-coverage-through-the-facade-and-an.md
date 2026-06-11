---
id: nts-01ktw1v8pb2h
title: Validation coverage through the facade and an executed semver native gate
status: closed
type: task
priority: 2
mode: afk
created: '2026-06-11T19:16:50.500381430Z'
updated: '2026-06-11T23:09:51.187632113Z'
closed: '2026-06-11T23:09:51.187632113Z'
tags:
- services
- review
acceptance:
- title: All four validation :types and their carried keys asserted through service/create's rejected promise on JVM + Node
  done: true
- title: Borderline semver versions exercised in an executed test, not a comment; the chosen seam for proving native agreement is stated in the test
  done: true
- title: No remaining direct test requires of nats-cljc.service.impl.config, or any remaining one is justified inline
  done: true
links:
- nts-01ktvn87why4
---

## Description

The epic's testing decision says a good test exercises behaviour through the public nats-cljc.service facade only — never an impl namespace. Today the config-validation and semver accept-set tests drive nats-cljc.service.impl.config directly, and of the four validation :types only :missing-required-key is asserted through the facade's rejected promise. Separately, the epic's verification gate 2 ('the semver pre-flight's accept-set must match both natives on borderline versions, e.g. "1.0" and "1.2.3-rc1+build"') is satisfied only by a comment claiming native agreement, not an executed red-watchable check.

Move the validation assertions to the facade (create's rejected promise) for all four :types (:missing-required-key, :invalid-name, :invalid-version, :duplicate-endpoint) with their carried keys, and turn the native-agreement claim into an executed check. Note the tension: the portable pre-flight fires before the native ever sees a borderline version, so proving native agreement may require probing the natives beneath the facade — resolve this deliberately and document the choice in the test.

## Notes

**2026-06-11T22:53:08.947688582Z**

Implemented: replaced the two deep-module tests and the single facade rejection test with (1) create-rejects-each-invalid-config-shape — all four validation :types (:missing-required-key x2 keys, :invalid-name service/whitespace/endpoint, :invalid-version, :duplicate-endpoint) with carried keys, asserted through service/create's rejected promise on both legs; (2) semver-borderlines-match-the-native-gate — the borderline pair ("1.0" rejected, "1.2.3-rc1+build" accepted) executed through the facade AND against each native's own gate (jnats Validator/validateSemVer — the call ServiceBuilder makes; nats.js parseSemVer from @nats-io/nats-core/internal — the call the Service constructor makes), seam choice documented in the test and ns docstring. Direct require of nats-cljc.service.impl.config removed from tests. Red-watched on both legs (flipped expectations failed as expected), then green: JVM 236 tests/806 assertions 0 failures; Node 206 tests/701 assertions 0 failures. clj-kondo clean. Not committed.

**2026-06-11T23:09:51.187632113Z**

Validation coverage now flows through the public facade: create-rejects-each-invalid-config-shape asserts all four validation :types and their carried keys via service/create's rejected promise on JVM and Node, and semver-borderlines-match-the-native-gate executes the borderline pair through the facade while proving native agreement by probing each native's own gate (jnats Validator/validateSemVer, nats.js parseSemVer) beneath it. The seam choice is documented in the test and ns docstring since the portable pre-flight fires before the native sees the version. Direct test requires of nats-cljc.service.impl.config and the orphaned thrown-type/thrown-data helpers are gone; no src changes. Shipped in 0a53d05.
