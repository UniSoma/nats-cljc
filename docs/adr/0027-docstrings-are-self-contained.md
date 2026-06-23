# Public docstrings are self-contained

Every public (non-`^:no-doc`) var's docstring must let a competent caller invoke
it **correctly without leaving the docstring**. `(ADR 00NN)` and `CONTEXT: X`
cross-references are **supplemental reading** — pointers to the rationale or the
deeper contract — never where a caller has to go to learn *how to call the
function*. The trigger was `nats.core/connect`: it accepts `:servers`, `:codec`,
`:name`, `:auth`, `:reconnect`, and `:on-status`, but its docstring named only
`:servers` and `:codec`, so the whole auth/reconnect/status surface was
discoverable only from the glossary and the impl — while `publish`/`subscribe`
already enumerated their options inline. `connect` is now the reference
implementation of the standard below.

The standard — every public docstring carries:

1. **Purpose** — one line.
2. **Every parameter and every option key the fn reads** — each with its type,
   default, and effect. No silent keys: if `connect` reads six, all six appear.
3. **Return shape** — e.g. "a platform-native promise resolving to a Connection".
4. **Failure behavior** — what it throws or rejects with (the canonical error /
   validation `:type`s relevant to *this* call), per ADR 0006 / 0015.
5. **A usage example** — only where the call shape is non-obvious (`connect`,
   `subscribe`, `request`), not on every one-liner.

The reconciliation rule with this repo's heavy cross-reference culture: a docstring
**may** cite `(ADR 00NN)` / `CONTEXT: X` for rationale or the full contract, but
must already contain points 1–5. If removing every cross-reference would leave a
caller unable to call the function correctly, the docstring is incomplete.

## Formatting: docstrings are Markdown (cljdoc)

cljdoc renders docstrings as **Markdown by default** (no opt-in needed), and that
is our published API doc target — so formatting must survive cljdoc's pipeline,
not just look aligned in source. cljdoc strips the *common-minimum* leading indent
(the conventional alignment under the opening quote is safe), but indentation
*beyond* that minimum is then interpreted by Markdown, and HTML collapses runs of
whitespace. The rules, verified by rendering `connect` through cljdoc's flexmark +
metagetta pipeline:

- **No space-aligned columns** — they collapse into ragged prose. Use a real
  **Markdown table** for a complex option map (e.g. `connect`'s `:auth` variants),
  or a `- ` bullet list with prose descriptions.
- **Code examples in fenced ` ```clojure ` blocks**, never indentation-only — an
  indented example below cljdoc's 4-space code threshold renders as a collapsed
  paragraph, not code.
- **Backtick-quote** every arg, keyword, and code span.
- Reference sibling vars with `[[ns/var]]` wikilinks (cljdoc resolves them); prose
  `(ADR 00NN)` / `CONTEXT: X` references stay prose — they are not vars.

These conventions also read acceptably in raw source and the REPL, so there is no
source-vs-cljdoc tradeoff to manage.

## Considered options

- **Keep the cross-reference-heavy status quo** (terse docstring + "see ADR/CONTEXT
  for the rest"). Rejected: it makes the glossary and ADR set *required* reading to
  use the library — a real adoption tax, since a caller cannot discover that an
  option even exists from the API surface.
- **Duplicate the full glossary entry into each docstring.** Rejected as the
  opposite excess: deep contract prose (e.g. the Status-event cadence divergences)
  is rationale, not call-site need. The line is "correctly callable", not
  "exhaustively explained" — depth stays in CONTEXT/ADR, reached by the
  supplemental references.

## Consequences

- The authoring checklist that operationalizes this ADR lives at
  [docs/agents/writing-docstrings.md](../agents/writing-docstrings.md). Reach for
  that when writing a docstring; this ADR records the decision behind it.
- The standard governs the **entire** public surface (`nats.core`, `jetstream`,
  `kv`, `service`, `codec`); docstrings that predate it and fall short are out of
  spec, to be brought into line as follow-up work rather than in one sweep.
- A docstring's option list and the fn's actual destructuring must stay in sync: a
  new option key is not "done" until its docstring entry lands.
- The CONTEXT glossary is unchanged by this ADR — it records a documentation
  convention, not a domain term.
