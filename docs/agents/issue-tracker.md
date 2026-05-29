# Issue tracker: knot

Issues and PRDs for this repo are tracked with **knot**, a local file-based
ticket tracker. Each ticket is a markdown file with YAML frontmatter under
`.tickets/`; config lives in `.knot.edn` at the repo root. Closed tickets
auto-move to `.tickets/archive/`.

## The one rule: use the CLI

Read and write tickets **only** through the knot CLI. Never `cat`, `grep`,
`ls`, `Edit`, or `Write` against `.tickets/` directly — knot keeps
`:updated`, the dependency graph, and archive placement consistent on every
write, and resolves ids across both live and archived tickets. The
`.tickets/` directory is an implementation detail; the CLI is the contract.

(The installed `knot` skill has the full command reference — defer to it for
anything beyond the operations below.)

## Conventions

- **Types**: `bug`, `feature`, `task`, `epic`, `chore`
- **Statuses**: `open` → `in_progress` → `closed` (`closed` is terminal)
- **Mode**: `afk` (an agent can run it end-to-end) or `hitl` (needs a human).
  This is how `ready-for-agent` / `ready-for-human` are expressed — see
  `triage-labels.md`.
- **Priority**: 0 (highest) … 4; default 2

## When a skill says "publish to the issue tracker"

Create a knot ticket:

    knot create "<title>" -t <type> -p <priority> --mode <afk|hitl> \
      --description "..." [--acceptance "..."] [--dep <id>] [--link <id>]

Pass `--description` whenever there's context worth saving, and use
`--acceptance "<title>"` (repeatable) for acceptance criteria. Use `--json`
to get the full created ticket (including its id) back in one call — don't
chain a `knot show` afterward.

## When a skill says "fetch the relevant ticket"

    knot show <id>            # works on archived tickets too

The user will normally pass a full or partial id (e.g. `01kqa9`); pass it
through verbatim — knot resolves it across live + archive.

## Other common operations

| Skill intent | knot command |
|---|---|
| List open work | `knot list --json` (filter with `--type`, `--mode`, `--tag`, `--status`, `--priority`) |
| What can an agent grab? | `knot ready --mode afk --json` |
| Comment / add context | `knot add-note <id> "..."` |
| Apply a triage role | `knot update <id> --add-tag <tag>` or `--mode <afk\|hitl>` (see `triage-labels.md`) |
| Close as shipped | `knot close <id> --summary "<what shipped>"` |
| Mark won't-fix | `knot close <id> --summary "wontfix: <reason>"` |
| Validate project integrity | `knot check` |
