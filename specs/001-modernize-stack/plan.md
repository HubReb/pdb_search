# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-15 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-modernize-stack/spec.md`

## Summary

Rebuild the bespoke, procedural paper-database CLI on mainstream Python frameworks while
preserving every user-facing behaviour and the existing four-table schema. The new stack is a
src-layout package (`src/paper_sorts/`) with four layers: a Typer CLI (presentation), a
service layer (domain orchestration), a SQLAlchemy 2.x persistence layer over psycopg v3
(repositories + DTOs), and a pydantic-settings configuration layer. Schema changes move to
Alembic migrations; the test suite runs against an ephemeral PostgreSQL via pytest-postgresql;
linting moves to ruff. A one-shot migration command converges either historical column-naming
variant (`bibtex_id` or the legacy `bibtext_id` typo) onto the canonical schema with zero data
loss. Bulk import from `.tex` + `.bib` is preserved with per-paper commits. A reverse-engineered
architecture document captures the legacy behaviour as the acceptance reference.

## Technical Context

**Language/Version**: Python ≥ 3.11 (uv-managed, PEP 621 metadata, hatchling build backend)
**Primary Dependencies**: SQLAlchemy 2.x, psycopg v3 (binary), Alembic, Typer, rich,
pydantic-settings v2, cryptography (Fernet), pybtex, pylatexenc
**Storage**: PostgreSQL only; four tables (`papers`, `bib`, `authors_id`, `authors_papers`)
**Testing**: pytest + pytest-postgresql (ephemeral PG from host `pg_ctl`), pytest-cov for coverage
**Target Platform**: Linux/commodity workstation, single-user offline CLI
**Project Type**: Single project — CLI tool with layered internals
**Performance Goals**: No measurable regression vs. legacy baseline on a personal-library-sized
dataset (interactive search/add/update/delete); bulk import may exceed interactive baseline but
commits per paper
**Constraints**: Offline, single-user, CLI-only; deterministic session close; no connection
pools/caches/async drivers beyond SQLAlchemy defaults; driver/ORM isolated to `db/`
**Scale/Scope**: Personal library (tens to low hundreds of papers); ~2 000 legacy LOC to be
reduced ≥ 30 % under `src/paper_sorts/`

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

The constitution (v1.3.0) was already amended ahead of this work to its target state for
Principles I–IV (ruff, pytest/pytest-postgresql, `cli/prompts.py` prompt routing, layer-level
driver isolation, non-regression performance). This plan upholds all four. One residual
inconsistency remains in the **Development Workflow & Quality Gates** section, which still
references `pylint paper_sorts`, the `unittest` suite, `helpers.get_user_input`, and
`DatabaseConnector.create_tables()` — stale wording carried over from v1.0.0. FR-016 / SC-007
require this be amended via `/speckit-constitution`, not silently violated; that amendment is a
task in the Polish phase (see tasks.md), producing a v1.3.1 PATCH bump.

| Principle | Touched? | Status |
|-----------|----------|--------|
| I. Code Quality (ruff, type hints, docstrings, `db/`-only driver isolation) | Yes | Upheld — new package is fully typed, ruff-clean; only `db/` imports `sqlalchemy`/`psycopg`. |
| II. Testing Standards (pytest, real DB, no mocked session, co-located seeds) | Yes | Upheld — persistence tests run against ephemeral PG; seed dataset co-located at `tests/fixtures/seed_papers.py`; no placeholder failing tests. |
| III. UX Consistency (prompts via `cli/prompts.py`, 1-indexed menus + abort, dual confirmations, plain-language errors) | Yes | Upheld — all prompts route through `cli/prompts.py`; menus 1-indexed with explicit quit/abort; update/delete confirm with numeric+word forms; errors logged, plain message to stdout. |
| IV. Performance (parameterised queries + joins over existing four tables, deterministic session close, no new indexes/tables/pools) | Yes | Upheld — ORM emits parameterised joins; `with_session(...)` closes deterministically; no schema additions beyond the legacy DDL. |

**Gate result: PASS** (no unjustified violations; the workflow-section amendment is tracked, not waived).

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/
│   └── cli-commands.md  # Phase 1 output — CLI command/prompt contract
└── tasks.md             # Phase 2 output (/speckit-tasks)
```

### Source Code (repository root)

```text
pyproject.toml                       # PEP 621 metadata, uv, hatchling, ruff/mypy/pytest config
alembic.ini                          # Alembic config (script_location = migrations)
migrations/
├── env.py                           # Alembic environment (URL from Settings)
├── script.py.mako
└── versions/
    ├── 001_*.py                     # Verbatim port of legacy canonical DDL
    └── 002_*.py                     # Converge legacy bibtext_id variant → canonical

src/paper_sorts/
├── __init__.py
├── config.py                        # pydantic-settings Settings + Fernet INI source
├── logging_config.py                # dictConfig (RichHandler + optional FileHandler)
├── cli/
│   ├── __init__.py
│   ├── app.py                       # Typer app; wires subcommands; no-subcommand menu loop
│   ├── prompts.py                   # ONLY module allowed to import rich.prompt / call input
│   ├── search.py                    # search subcommand + interactive flow
│   ├── add.py                       # add subcommand
│   ├── update.py                    # update subcommand
│   ├── delete.py                    # delete subcommand
│   ├── migrate.py                   # migrate subcommand (Alembic upgrade + legacy converge)
│   └── importer.py                  # import subcommand (bulk .tex+.bib)
├── services/
│   ├── __init__.py
│   ├── paper_service.py             # search/add/update/delete domain ops (DTOs only)
│   └── import_service.py            # extract_papers_from_tex_bib(...) iterator
└── db/
    ├── __init__.py
    ├── models.py                    # 4 ORM models (declarative)
    ├── repositories.py              # PaperRepository/AuthorRepository/BibRepository + DTOs
    └── session.py                   # engine factory + with_session(...) context manager

tests/
├── conftest.py                      # postgresql_proc, ephemeral_db_url, seeded session fixtures
├── fixtures/
│   ├── seed_papers.py               # SEED_PAPERS canonical dataset (co-located)
│   ├── sample.tex / sample.bib      # bulk-import fixture pair
│   └── single.bib                   # single-entry add fixture
├── test_repositories.py             # persistence CRUD + search (real DB)
├── test_paper_service.py            # service orchestration (real DB)
├── test_import_service.py           # tex/bib extraction + bulk import (real DB)
├── test_migration.py                # migration idempotency + legacy-variant converge (real DB)
├── test_config.py                   # settings priority chain + Fernet source (unit)
└── test_prompts.py                  # prompt grammar: empty re-prompt, menu parse, confirm (unit)

docs/
└── architecture.md                  # Reverse-engineered legacy architecture (FR-001)
```

**Structure Decision**: Single-project src-layout. The four constitution layers map to
directories: `cli/` (presentation), `services/` (domain), `db/` (persistence), and
`config.py`/`logging_config.py` (cross-cutting). Driver/ORM imports are physically confined to
`db/`. Subcommands map to legacy top-level menu options (search/add/update/delete) plus the two
admin paths (migrate/import) that are subcommand-only and absent from the four-option menu.

## Complexity Tracking

> No constitution violations require justification. The four-table schema is preserved verbatim
> (no new tables, no new indexes, no FKs added to `authors_papers`), so Principle IV's
> Complexity-Tracking trigger is not hit. The stale Development-Workflow wording is amended via
> `/speckit-constitution` (tracked task), not waived.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| (none) | — | — |
