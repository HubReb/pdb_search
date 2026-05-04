# Implementation Plan: UX Polish — Letter Aliases & Search-then-Act

**Branch**: `002-ux-polish` | **Date**: 2026-05-04 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/002-ux-polish/spec.md`

## Summary

Two related UX polish items on the modernized CLI:

1. **Letter aliases on every numbered menu.** Generalize `paper_sorts.cli.prompts.ask_choice` so each option accepts a single-letter alias in addition to its 1-indexed digit. Aliases are auto-derived from the first alphabetic char of the label by default, case-insensitive, unique within a menu, rendered with parens on the label (e.g. `(P)apers`). Collisions are rejected at construction time; callers disambiguate with explicit per-option aliases or by opting individual options out (alias=None).

2. **Search-then-act on `update` and `delete`.** Replace the legacy raw-id prompt on `pdbsearch update` (papers table only) and `pdbsearch delete` with the existing search dialog plus a shared disambiguation helper (`cli/search._disambiguate` promoted to public `cli/search.disambiguate`). A non-interactive `--id <N>` flag is added to `update` and retained on `delete`; the flag skips only the search step — table, field, and new-value collection remain interactive on `update`.

Constitution amendment to v1.4.0 (Principle III, broader letter-alias rule) **already landed** as commit `8ebfa9d`, so the implementation work below does not transiently violate the live constitution.

End-to-end change set: six files modified — `cli/prompts.py`, `cli/app.py`, `cli/update.py`, `cli/delete.py`, `cli/search.py` (helper promotion), and the existing `specs/001-modernize-stack/contracts/cli-commands.md` (per FR-016). No new modules, no schema migrations, no new dependencies.

## Technical Context

**Language/Version**: Python ≥ 3.11 (unchanged from 001)
**Primary Dependencies**: existing only — Typer, rich, SQLAlchemy 2.x, pydantic-settings 2.x, psycopg v3. **No new dependencies introduced.**
**Storage**: PostgreSQL (unchanged; this feature does not touch the persistence layer)
**Testing**: pytest, pytest-postgresql (unchanged); new cases extend existing test files under `tests/unit/test_prompts.py` and `tests/integration/test_{update,delete}.py`
**Target Platform**: Linux/macOS desktop, single-user, offline. CLI only.
**Project Type**: Single-project CLI tool, `src/` layout. Same shape as 001.
**Performance Goals**: Constitution Principle IV non-regression criterion applies. The new search-then-update flow *composes* operations that are individually unchanged (search by title/author + update field); no new SQL paths are introduced. The composite latency increases by the cost of one search query — that is **not** a per-operation regression and does not require waivers.
**Constraints**: Same as 001 — single user, local Postgres, no async drivers / no caching / no read replicas / no new connection-pool changes. The constitution-mandated boundary (`cli/prompts.py` is the only `rich.prompt` consumer) is preserved — no new module imports `rich.prompt`.
**Scale/Scope**: Personal-library-sized dataset (current corpus). Code delta is small: ≈ 6 files modified, no new modules.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

This feature implements (rather than amends) constitutional rules. The v1.4.0 amendment to Principle III authorising broader letter aliases was ratified ahead of this plan (commit `8ebfa9d`, 2026-05-04). Each principle is checked below:

| Principle | Touched? | Status under this plan |
|-----------|----------|------------------------|
| I. Code Quality | Yes | PASS. New code in `prompts.py`, `update.py`, `delete.py`, `search.py` carries full type hints + docstrings. The persistence-layer-isolation rule is preserved — only `db/` imports `sqlalchemy` (this feature touches none of `db/`). The "no module-level constants" preference (per the user's prior feedback on 001) is honoured: alias derivation lives inside the `ask_choice` function body, not as a top-level constant. |
| II. Testing Standards | Yes | PASS. New unit tests for `ask_choice` alias paths (default first-letter, explicit override, None opt-out, case-insensitive match, collision rejection at construction time, digit-input non-regression) extend `tests/unit/test_prompts.py`. New integration tests for the search-then-update and search-then-delete flows extend `tests/integration/test_update.py` / `test_delete.py` and run against the existing `pytest-postgresql` ephemeral DB — no mocking the SQLAlchemy session, no mocks of `PaperRepository`. |
| III. UX Consistency | Yes | PASS. This feature is the implementation of the v1.4.0 amendment. The new bullet (constitution.md §III, fourth bullet, lines 210–218) is the authorising rule. The other three Principle III rules (prompt routing through `cli/prompts`, 1-indexed-with-abort, dual-form confirmation) carry forward unchanged — `ask_choice` remains the single prompt helper, abort/quit slot remains last on every menu, and `ask_confirm` is untouched. |
| IV. Performance | Yes | PASS. No new operations are introduced (no new repository method, no new SQL). The search-then-update flow is composed of operations whose individual non-regression has already been verified by 001's baseline. Adding a search step to a flow that previously had none does not regress *the search operation* — it just inserts the operation into a different composite path. No new connection pooling, async drivers, caches, or denormalised tables. |

**Gate result**: PASS. No Complexity Tracking entries required. The constitution amendment is the only meta-deliverable, and it is already merged.

## Project Structure

### Documentation (this feature)

```text
specs/002-ux-polish/
├── plan.md                  # This file
├── research.md              # Phase 0 — alias-collision policy, helper location, signature evolution
├── data-model.md            # Phase 1 — MenuOption (in-memory only); no schema changes
├── quickstart.md            # Phase 1 — what's new for the user (alias keystrokes, search-then-act flows)
├── contracts/
│   └── cli-grammar.md       # ask_choice contract + delta to apply to 001/contracts/cli-commands.md
├── checklists/
│   └── requirements.md      # spec-quality checklist (already exists, all items pass)
└── tasks.md                 # Phase 2 — produced by /speckit-tasks
```

### Source Code (repository root)

This feature modifies existing files only — no new modules or directories.

```text
src/paper_sorts/cli/
├── prompts.py     # MODIFIED — ask_choice gains per-option alias support; quit_alias= kwarg removed
├── app.py         # MODIFIED — top menu drops the quit_alias="q" call (auto-derives 'q' from "(Q)uit")
├── update.py      # MODIFIED — search-then-update flow on the papers table; --id flag added; table-aware
│                  #            confirmation summary (papers path echoes title + id; bib/authors retain legacy wording)
├── delete.py      # MODIFIED — search-then-delete flow when --id is omitted; --id retained verbatim
└── search.py      # MODIFIED — `_disambiguate` renamed to public `disambiguate`; consumed by update.py + delete.py

tests/
├── unit/
│   └── test_prompts.py      # MODIFIED — alias derivation, override, None opt-out, case-insensitive,
│                            #            collision rejection, digit-only non-regression
└── integration/
    ├── test_update.py       # MODIFIED — search-then-update happy path (papers); --id non-interactive entry;
    │                        #            bib/authors retain raw-id prompt; zero-results case
    └── test_delete.py       # MODIFIED — search-then-delete happy path; --id non-interactive entry;
                             #            zero-results case

specs/001-modernize-stack/contracts/
└── cli-commands.md          # MODIFIED (per spec FR-016) — grammar bullet § "Top-level interactive menu"
                             #   updated for v1.4.0 alias rule; § "Subcommand: update" and § "Subcommand: delete"
                             #   show new search-then-act prompt sequences
```

**Structure Decision**: In-place modification of existing modules under `src/paper_sorts/cli/`. The constitution-mandated layer boundaries are preserved — no module under `cli/` other than `cli/prompts.py` imports `rich.prompt`; no module under `cli/` imports `sqlalchemy`. The promotion of `_disambiguate` to public `disambiguate` does not move it — same module, same dependencies, just a name change so update + delete can import it.

## Complexity Tracking

> No constitution violations require justification under this plan. The v1.4.0 amendment is the change mechanism authorising letter aliases on every menu, and it landed before this plan was drafted. The search-then-act flows do not introduce any new constitutional concern (they reuse existing search and update operations).

No entries.
