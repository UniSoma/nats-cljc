---
id: nts-01ktt758sg38
title: 'CLJS watch :initialized: tombstone-only :keys match resolves before markers replay'
status: open
type: task
priority: 3
mode: afk
created: '2026-06-11T02:11:12.303953667Z'
updated: '2026-06-11T02:11:12.303953667Z'
parent: nts-01ktsner23xc
tags:
- kv
- cljs
---

## Description

On the CLJS leg, @nats-io/kv 3.x exposes no initialized callback, so kv/impl/js.cljs derives the watch :initialized signal: a delta-0 delivery, or — when nothing replays — a filtered live-key probe. The probe counts LIVE keys, so a :keys filter matching only tombstoned keys resolves :initialized early, and the markers still replay after it (the code comment at the probe site records the trade). Revisit when nats.js grows an initialized/init_done signal for filtered watches, or normalize by counting markers in the probe (kvKeys includes no deleted keys, so this likely needs the history-based probe instead). Until then this is a known limitation: it only bites a :deliver :latest/:history watch whose :keys patterns match nothing but tombstones, and only for marker deliveries (cache builders ignoring deletes are unaffected). Add the missing edge test alongside the fix.
