# Writing Docstrings

How to write a public docstring in this repo. The *decision and rationale* live in
[ADR 0027](../adr/0027-docstrings-are-self-contained.md); this is the authoring
checklist that operationalizes it. cljdoc is our published API target and renders
docstrings as **Markdown by default**, so everything below is written to survive
that pipeline — not just to look aligned in source.

## The bar: a public docstring is self-contained (ADR 0027)

Every public (non-`^:no-doc`) var's docstring must let a competent caller invoke it
**correctly without leaving the docstring**. `(ADR 00NN)` / `CONTEXT: X` references
are *supplemental* — rationale and deep contract, never where a caller goes to
learn *how to call the function*.

Every public docstring carries:

1. **Purpose** — one line.
2. **Every parameter and every option key the fn reads** — each with type,
   default, and effect. No silent keys: if the fn destructures six options, all six
   appear. The docstring's option list and the fn's actual destructuring must stay
   in sync — a new option key is not "done" until its docstring entry lands.
3. **Return shape** — e.g. "a platform-native promise resolving to a `Connection`".
4. **Failure behavior** — what it throws or rejects with (the canonical error /
   validation `:type`s relevant to *this* call), per ADR 0006 / 0015.
5. **A usage example** — only where the call shape is non-obvious (`connect`,
   `subscribe`, `request`), not on every one-liner.

`nats.core/connect` is the reference implementation of this standard — read it
before writing a new public docstring.

The test: if removing every cross-reference would leave a caller unable to call the
function correctly, the docstring is incomplete.

## Markdown formatting for cljdoc (verified, ADR 0027)

cljdoc strips the common-minimum leading indent (alignment under the opening quote
is safe), but indentation *beyond* that is interpreted as Markdown, and HTML
collapses whitespace runs. Verified by rendering `connect` through cljdoc's
flexmark + metagetta pipeline:

- **No space-aligned columns** — they collapse into ragged prose. Use a real
  **Markdown table** for a complex option map, or a `- ` bullet list with prose
  descriptions.

  | key       | type   | default | effect                          |
  |-----------|--------|---------|---------------------------------|
  | `:queue`  | string | none    | Queue group for load-balancing. |
  | `:max`    | int    | none    | Auto-unsubscribe after N msgs.  |

- **Code examples in fenced ` ```clojure ` blocks**, never indentation-only — an
  indented example below cljdoc's 4-space code threshold renders as a collapsed
  paragraph, not code.
- **Backtick-quote** every arg, keyword, and code span (`` `coll` ``, `` `:timeout` ``).
- **Cross-reference sibling vars with `[[ns/var]]` wikilinks** — cljdoc resolves
  them to links. Prose `(ADR 00NN)` / `CONTEXT: X` references stay prose; they are
  not vars.

These conventions also read acceptably in raw source and the REPL, so there is no
source-vs-cljdoc tradeoff to manage.

## .cljc note

This is one portable `.cljc` API, so each docstring renders **once** on cljdoc for
both Clojure and ClojureScript. Where a var's behavior diverges by platform (e.g. a
JVM-only blocking path, or different status-event cadence), call that out
explicitly in the docstring — the reader cannot tell which leg they're on from the
signature.

## Template

```clojure
(defn subscribe
  "Subscribe to `subject`, delivering each message to `handler`.

  Returns a [[nats.core/Subscription]]; pass it to [[nats.core/unsubscribe!]].

  `opts` (optional map):

  | key      | type   | default | effect                          |
  |----------|--------|---------|---------------------------------|
  | `:queue` | string | none    | Queue group for load-balancing. |
  | `:max`   | int    | none    | Auto-unsubscribe after N msgs.  |

  ```clojure
  (subscribe conn \"greet.*\" prn {:queue \"workers\"})
  ```

  Rejects with a `:nats/validation` error (ADR 0015) if `subject` is blank.
  See CONTEXT: Subscription for delivery semantics."
  ([conn subject handler] ...)
  ([conn subject handler opts] ...))
```
