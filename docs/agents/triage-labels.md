# Triage Labels

The skills speak in terms of five canonical triage roles. knot has no
free-form "labels"; instead it has a first-class `mode` dimension
(`afk`/`hitl`), a `status` workflow, and `tags`. This file maps each
canonical role onto the knot mechanism that expresses it.

| Canonical role    | knot expression      | How to apply it                                  |
| ----------------- | -------------------- | ------------------------------------------------ |
| `needs-triage`    | tag `needs-triage`   | `knot update <id> --add-tag needs-triage` (or leave it in the `open` intake lane) |
| `needs-info`      | tag `needs-info`     | `knot update <id> --add-tag needs-info`          |
| `ready-for-agent` | **`--mode afk`**     | `knot update <id> --mode afk` — then `knot ready --mode afk` surfaces it |
| `ready-for-human` | **`--mode hitl`**    | `knot update <id> --mode hitl` (the default mode) |
| `wontfix`         | close with a summary | `knot close <id> --summary "wontfix: <reason>"` (knot has no `wontfix` status; closing is the idiomatic "won't action") |

When a skill mentions a role (e.g. "apply the AFK-ready triage label"), use
the corresponding knot mechanism from this table. Edit this table if you
adopt different tag names.
