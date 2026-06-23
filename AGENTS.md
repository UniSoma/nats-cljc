# AGENTS.md

Agent configuration for the `nats-cljc` repo, a [NATS](https://nats.io) for Clojure and ClojureScript under one portable `.cljc` API.

## Hard rules (every task)

- **Map `.clj` files before reading them.** Run `clj-surgeon :op :ls :file <path>` from the shell (it's a CLI tool with EDN-pair args, *not* a Clojure ns — don't `(require)` it) first on any `.clj` file over ~500 lines, then `Read` only the line ranges you need. ~150× more token-efficient than blind reads. Full op reference: the `clj-surgeon` skill.
- **Lint before commit.** Run `clj-kondo --lint src test`. See [docs/agents/linting-and-formatting.md](docs/agents/linting-and-formatting.md).
- **Write public docstrings to the standard.** Every public (non-`^:no-doc`) var's docstring must be self-contained — correctly callable without leaving the docstring (ADR 0027). See [docs/agents/writing-docstrings.md](docs/agents/writing-docstrings.md) for the authoring checklist and cljdoc Markdown rules.
- **Test on JVM + Node before commit.** Run the suite on both local legs against a ws-enabled `nats-server`; the browser leg is CI-only (ADR 0010). See [docs/agents/running-tests.md](docs/agents/running-tests.md) to run it, and [docs/agents/writing-tests.md](docs/agents/writing-tests.md) for the conventions when authoring one.
- **Prefer nREPL for evaluation.** `clj-nrepl-eval -p 7888 '<form>'` over `bb -cp src -e '<form>'` for sanity checks and exploration — a warm JVM Clojure REPL with `src` + jnats + `:test` deps loaded, so it runs real JVM interop (which babashka's SCI can't) with no per-call cold-start; persistent session (state survives between calls), `:reload`-aware. See [docs/agents/clojure-repl-evaluation.md](docs/agents/clojure-repl-evaluation.md).
- **Verify toolchain behavior; don't infer it.** Compiler/runtime effects (`:advanced` externs/DCE, interop return shapes, macroexpansion) are hypotheses until a build/REPL/test confirms them — a nearby convention is not proof. Before trusting a green, watch the check go red on a known-bad input.

## Agent skills

### Issue tracker

Issues and PRDs are tracked with **knot**, a local file-based tracker (`.knot.edn` + `.tickets/`). Use the knot CLI; never touch `.tickets/` directly. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles map onto knot's native `mode` dimension (`afk`/`hitl`) and tags. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.

# Behavioral guidelines

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.
