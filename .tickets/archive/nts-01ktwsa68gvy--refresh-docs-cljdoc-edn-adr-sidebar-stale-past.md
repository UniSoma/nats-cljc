---
id: nts-01ktwsa68gvy
title: Replace cljdoc ADR sidebar list with a single ADR index entry
status: closed
type: chore
priority: 4
mode: afk
created: '2026-06-12T02:06:56.784715588Z'
updated: '2026-06-12T02:48:52.184041799Z'
closed: '2026-06-12T02:48:52.184041799Z'
tags:
- docs
links:
- nts-01ktwvf42qxj
---

## Description

The cljdoc.edn sidebar lists each ADR individually (stale past 0015), and on the narrow sidebar each multi-line title eats ~3 lines, pushing the namespace list out of scroll. Instead of refreshing the flat list, collapse it to one entry.

Decided (grilling session 2026-06-12):
- Create docs/adr/README.md: a flat numeric index, one line per ADR ([00NN · Title](file.md)), listing all 26 ADRs including 0005 and 0010 — the 'intentionally GitHub-only' omission is retired, since index links resolve to GitHub blob URLs anyway. GitHub auto-renders the README when browsing docs/adr/, so one file serves both audiences. No grouping, no status column (no ADR carries a status field).
- cljdoc.edn: replace the per-ADR entries with a single "Design decisions" entry pointing at docs/adr/README.md. Rewrite the header comment: both current rationales (number-prefixed titles for docstring findability; 0005/0010 omitted) are retired. New rationale: ADR content deliberately leaves cljdoc (no rendered pages / full-text search there; cljdoc rewrites the index's relative links to GitHub) in exchange for sidebar space — verified that cljdoc renders nested article trees fully expanded, so nesting would not have solved it.
- Add a JVM-leg drift-guard test: every docs/adr/[0-9]*.md filename must appear in docs/adr/README.md. This drift already happened once (0016-0026 missing); the test turns 'remember to update the index' into a build failure.
- No ADR for this decision (trivially reversible); rationale lives in the cljdoc.edn header comment.
- README's 'Design docs' section already links docs/adr/ — GitHub renders the new index there automatically, no README change needed.
- Verify the cljdoc config renders.

## Notes

**2026-06-12T02:48:52.184041799Z**

Replaced the stale per-ADR cljdoc sidebar list with a single 'Design decisions' entry publishing docs/adr/README.md, a flat numeric index of all 26 ADRs (0005/0010 omission retired — index links resolve to GitHub blob URLs, so ADR content is GitHub-only now). Added adr-index-test (JVM .clj, version-test pattern) asserting every docs/adr/NNNN-*.md appears in the index; verified red on a dummy ADR before green. cljdoc.edn parses, all four :file entries and all 26 index links resolve. Full suites green on JVM (237 tests) and Node (206). Shipped in 1556d57. Follow-up split out: nts-01ktwvf42qxj (README JetStream section).
