# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-04-26 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-modernize-stack/spec.md`

## Summary

Reverse-engineer the existing CLI paper-database tool into a written architecture document, then rebuild its internals on mainstream Python frameworks while preserving every existing feature, prompt, and workflow:

- **ORM**: SQLAlchemy 2.x (sync, typed `Mapped[]` API) with `psycopg` v3 as the driver — replacing hand-written SQL strings and the dual psycopg2/psycopg dependency.
- **Migrations**: Alembic — replacing the runtime `create_tables()` call.
- **CLI**: Typer subcommands + `rich.prompt`/`rich.console` — replacing bespoke `argparse` + `input()` loops while keeping the existing dialog grammar.
- **Configuration**: pydantic-settings v2 with a custom Fernet source — replacing the bespoke `ConfigReader` and adding `.env` / env-var support.
- **Tests**: pytest + pytest-postgresql for ephemeral test databases — replacing bare `unittest` and the developer-local-DB dependency.
- **Lint/format**: ruff (lint + format) — replacing pylint, with optional mypy for static type checking.
- **Logging**: stdlib `logging` with one dict-config — replacing per-class `create_logger` boilerplate.

CLI-only deployment surface (spec FR-017). In-place refactor on this branch. Constitution amended in parallel from v1.1.0 to v1.3.0 (MINOR — five testable predicates redefined to layer/role names; uv replaces Poetry; psycopg v3 replaces psycopg2; Python ≥ 3.11; no principle removed).

## Technical Context

**Language/Version**: Python 3.11+ (raised from 3.10 to gain mainstream-framework support; `requires-python = ">=3.11"`)
**Primary Dependencies**: SQLAlchemy ≥ 2.0, Alembic, Typer, rich, pydantic-settings ≥ 2.0, psycopg[binary] ≥ 3.1, pybtex (retained), pylatexenc (retained), cryptography (retained, for Fernet)
**Storage**: PostgreSQL ≥ 13 (existing user dependency); driver is psycopg v3 (the legacy psycopg2 dependency is removed)
**Testing**: pytest, pytest-postgresql (ephemeral DB), pytest-cov (coverage)
**Target Platform**: Linux/macOS desktop, single-user, offline. CLI only (spec FR-017).
**Project Type**: Single-project CLI tool, `src/` layout (PEP 517/518 mainstream).
**Performance Goals**: No measurable regression vs. current implementation on equivalent operations (constitution Principle IV v1.1.0). Baseline captured by a benchmark fixture *before* any code change; same fixture re-runs against the modernized stack.
**Constraints**: Single user, local Postgres, no async drivers / no caching / no read replicas / no connection pooling beyond SQLAlchemy's default `QueuePool` at default size. Encrypted-config (Fernet) source preserved alongside `.env` and env vars.
**Scale/Scope**: Personal-library-sized dataset (current corpus is the reference). Code target: ≥ 30 % reduction in project-authored Python lines under `paper_sorts/`, replaced by mainstream-library imports rather than feature loss (spec SC-005).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

The plan triggers five bundled amendments to the constitution that the spec already anticipates (FR-016). The amendments are the change mechanism, not violations. They MUST be applied via `/speckit-constitution` as the **first** commit of the implementation phase, raising v1.1.0 → v1.3.0.

| Principle / Section | Current text references | Status under this plan | Amendment scope (v1.3.0) |
|---------------------|-------------------------|-----------------------|---------------------------|
| I. Code Quality | `pylint paper_sorts`; `psycopg2` isolated to `psycopg_db.py`; legacy `add.py` / `search.py` / `get_data.py` frozen | ruff replaces pylint; SQLAlchemy session isolated to `paper_sorts/db/`; legacy modules deleted (FR-012) | Replace tool name `pylint` → `ruff`; replace driver-name rule with persistence-layer rule; remove the "frozen legacy modules" clause (the modules no longer exist after this work). |
| II. Testing Standards | "Tests run via `python -m unittest discover tests`"; integration tests against a real Postgres, no mocking psycopg | pytest replaces unittest; integration tests run against pytest-postgresql ephemeral DB; placeholder `tests/test_user_interaction.py` deleted; seed fixtures co-located with tests | Replace tool name `unittest` → `pytest`; document pytest-postgresql as the canonical ephemeral-DB mechanism; placeholder rule and "no mocking the persistence layer" rule carry forward unchanged. |
| III. UX Consistency | Prompts MUST route through `helpers.get_user_input()` / `helpers.get_user_choice()` | New `paper_sorts/cli/prompts.py` module wraps `rich.prompt` with the same grammar (1-indexed menus, mandatory abort/quit, empty-input re-prompt, dual `1`/`y`/`yes` confirmations) | Replace named-helper references with `paper_sorts.cli.prompts`; preserve grammar rules verbatim. |
| IV. Performance | References to `PsycopgDB`, `search_by_title`, `search_by_author`, `add_data_from_dict`, `load_data_into_db` | Layer-level references to "the persistence layer", "search paths", "bulk import paths". Non-regression criterion (already in v1.1.0) carries forward unchanged. | Replace function-level names with layer / role names. |
| Stack & Constraints (Section 2) | "Language: Python ^3.10, dependencies managed by Poetry"; "Driver is `psycopg2` (binary)" | Python ≥ 3.11; uv replaces Poetry (PEP 621 metadata, `uv.lock`, hatchling build backend); psycopg v3 replaces psycopg2; SQLAlchemy 2.x sits in the persistence layer | Replace Python version line; replace build-tool name; replace driver name. (Reasoning: original R9 deferred uv on the premise that Poetry was already installed; that premise was false — see `research.md` § R9.) |

**Gate result**: PASS (with bundled amendments). No unjustified violations. No Complexity Tracking entries needed. The amendment text is drafted in `research.md` and committed as the first implementation step before any framework-bearing code is added.

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md                  # This file
├── research.md              # Phase 0 — framework choices + amendment text
├── data-model.md            # Phase 1 — domain entities + ORM mappings
├── quickstart.md            # Phase 1 — install + first-run walkthrough
├── contracts/
│   ├── cli-commands.md      # Subcommand surface + interactive grammar
│   └── database-schema.md   # Modernized schema + Alembic migration sketch
├── checklists/
│   └── requirements.md      # spec-quality checklist (already exists)
└── tasks.md                 # Phase 2 — produced by /speckit-tasks
```

### Source Code (repository root)

```text
src/paper_sorts/
├── __init__.py
├── cli/
│   ├── __init__.py
│   ├── app.py              # Typer app + default command (top-level menu)
│   ├── search.py           # `pdbsearch search` + dialog
│   ├── add.py              # `pdbsearch add` + dialog
│   ├── update.py           # `pdbsearch update` + dialog
│   ├── delete.py           # `pdbsearch delete` + dialog
│   ├── importer.py         # `pdbsearch import <tex> <bib>`
│   ├── migrate.py          # `pdbsearch migrate` (alembic upgrade head)
│   └── prompts.py          # rich-backed prompt grammar (replaces helpers.get_user_input)
├── services/
│   ├── __init__.py
│   ├── paper_service.py    # search / add / update / delete domain ops
│   └── import_service.py   # .tex + .bib → domain objects (replaces helpers.get_data + get_bibtex_information)
├── db/
│   ├── __init__.py
│   ├── models.py           # SQLAlchemy ORM: Paper, Author, BibEntry, Authorship
│   ├── repositories.py     # Paper / Author / BibEntry repository classes
│   └── session.py          # engine, sessionmaker, context-managed session
├── config.py               # pydantic-settings: env, .env, Fernet source
└── logging_config.py       # stdlib logging dict-config

migrations/                  # Alembic — at repo root, not under src/
├── env.py
├── script.py.mako
└── versions/
    ├── 001_initial_schema.py
    └── 002_legacy_bibtext_to_bibtex.py    # FR-011: idempotent rename for old data

tests/
├── conftest.py             # pytest-postgresql fixtures, seed data wiring
├── fixtures/
│   ├── seed_papers.py
│   └── sample.bib
├── unit/
│   ├── test_config.py
│   └── test_prompts.py
├── integration/
│   ├── test_search.py
│   ├── test_add.py
│   ├── test_update.py
│   ├── test_delete.py
│   ├── test_import.py
│   └── test_migrations.py
└── benchmarks/
    └── bench_baseline.py   # records baseline timings; rerun after to verify SC-006

docs/
└── architecture.md         # US1 deliverable — reverse-engineering document
```

**Structure Decision**: Single project, `src/` layout. Migrations at repo root (Alembic convention). Per-layer subpackages under `paper_sorts/` enforce the layered architecture (FR-014):

- `cli/` — only layer that imports `typer` / `rich`
- `db/` — only layer that imports `sqlalchemy`
- `services/` — depends on `db/` *interfaces* (the repository classes); no SQLAlchemy types leak across this seam
- `config.py` / `logging_config.py` — depended on by everything; depend on nothing internal

The current modules (`paper_sorts/{add,search,get_data,psycopg_db,database_connector,user_interaction,helpers,config_reader,run}.py`) are all removed in this migration. Their behaviours are preserved by the modules above; the architecture document (US1) records the old names for a finite period to ease review.

## Complexity Tracking

> No constitution violations require justification under this plan. The five bundled v1.3.0 amendments are the change mechanism (FR-016), not deviations.

No entries.
