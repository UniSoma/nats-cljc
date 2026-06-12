---
id: nts-01ktwsa68gvy
title: Refresh docs/cljdoc.edn ADR sidebar (stale past 0015)
status: open
type: chore
priority: 4
mode: afk
created: '2026-06-12T02:06:56.784715588Z'
updated: '2026-06-12T02:06:56.784715588Z'
tags:
- docs
---

## Description

The cljdoc.edn sidebar lists ADRs only up to 0015; ADRs 0016-0026 (through Phase-4 services: 0024 no-context, 0025 errors-as-payloads, 0026 nats-io 3.4.0 floor) are missing from the rendered docs nav. Pre-existing debt noted at the close of the Phase-4 epic (nts-01ktvn87why4). Add the missing entries and verify the cljdoc config renders.
