# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-14 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-modernize-stack/spec.md`

## Summary

Reverse-engineer the legacy flat-layout `paper_sorts/` package (psycopg2/argparse/unittest/poetry, ~2 300 lines) and rebuild it as a modern `src/paper_sorts/` src-layout package using SQLAlchemy 2.x + psycopg v3, Typer CLI, pydantic-settings v2 config, Alembic migrations, ruff linting, and a pytest + pytest-postgresql integration-test suite — all while preserving identical user-facing CLI behaviour and the same personal PostgreSQL database.

## Technical Context

**Language/Version**: Python >= 3.11 (raised from 3.10 per constitution v1.3.0 and FR-015)
**Primary Dependencies**: SQLAlchemy 2.x, Typer, Alembic, pydantic-settings v2, psycopg v3 (binary), pybtex, pylatexenc, ruff, mypy, pytest, pytest-postgresql, rich
**Storage**: PostgreSQL only; four tables: `papers`, `bib`, `authors_id`, `authors_papers`
**Testing**: pytest + pytest-postgresql (ephemeral DB per test session; real SQL, no mocks)
**Target Platform**: Linux / macOS CLI, personal offline use
**Project Type**: CLI tool
**Performance Goals**: No measurable regression vs. the current baseline on personal-library-sized data (constitution Principle IV)
**Constraints**: Offline, single-user; no connection pooling, no async, no web surface; context-managed sessions only
**Scale/Scope**: Personal paper library (~hundreds of entries); 4-table schema preserved

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Touches? | Status | Notes |
|-----------|----------|--------|-------|
| I. Code Quality | YES | PASS | ruff replaces pylint (amended in constitution v1.3.0); isolation rule rewritten to layer-level: only db/ may import sqlalchemy |
| II. Testing Standards | YES | PASS | unittest -> pytest (amended in v1.3.0); pytest-postgresql; no mocking SQLAlchemy session |
| III. UX Consistency | YES | PASS | prompt routing moved to paper_sorts.cli.prompts (amended in v1.3.0); 1-indexed menus, mandatory abort, empty re-prompt, dual confirmations carry forward |
| IV. Performance | YES | PASS | Non-regression criterion; no pooling/async; deterministic session close via context manager |

Constitution v1.3.0 already incorporates all amendments required by FR-016. No further amendments needed.

**Post-Design Re-check**: All four principles upheld. Persistence isolated to db/. CLI prompts routed through cli/prompts.py. Sessions context-managed. Tests are real-DB integration tests.

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/
│   └── cli-commands.md  # Phase 1 output
└── tasks.md             # Phase 2 output (speckit-tasks)
```

### Source Code (repository root)

```text
src/
└── paper_sorts/
    ├── __init__.py
    ├── config.py                  # pydantic-settings Settings model (four-source priority)
    ├── logging_config.py          # single dictConfig setup (RichHandler + optional FileHandler)
    ├── cli/
    │   ├── __init__.py
    │   ├── app.py                 # Typer app, top-level menu, subcommand wiring
    │   ├── search.py              # search subcommand
    │   ├── add.py                 # add subcommand
    │   ├── update.py              # update subcommand
    │   ├── delete.py              # delete subcommand
    │   ├── importer.py            # import subcommand (bulk tex+bib)
    │   └── prompts.py             # ONLY module that may call input()/rich.prompt/typer.prompt
    ├── services/
    │   ├── __init__.py
    │   ├── paper_service.py       # search_by_title, search_by_author, add_paper, update_field, delete_paper
    │   └── import_service.py      # extract_papers_from_tex_bib() -> Iterator[PaperCreate]
    └── db/
        ├── __init__.py
        ├── models.py              # SQLAlchemy 2.x ORM models (4 tables)
        ├── repositories.py        # PaperRepository, AuthorRepository, BibRepository + DTOs
        └── session.py             # with_session() context manager

migrations/
├── alembic.ini
├── env.py
├── script.py.mako
└── versions/
    ├── 001_initial_schema.py      # CREATE IF NOT EXISTS verbatim DDL port
    └── 002_converge_legacy.py     # handle bibtext_id typo column

tests/
├── conftest.py                    # postgresql_proc, ephemeral_db_url fixtures
├── fixtures/
│   └── seed_papers.py             # SEED_PAPERS canonical dataset
├── test_repositories.py           # integration tests: CRUD + search
├── test_migrations.py             # migration smoke test
├── test_import_service.py         # unit/integration tests for import service
└── test_config.py                 # unit tests for config

docs/
└── architecture.md                # US1 deliverable

pyproject.toml                     # PEP 621 + hatchling + uv; ruff + mypy config
uv.lock
```

**Structure Decision**: Single project, src-layout. Legacy `paper_sorts/` flat layout is removed once each module's functionality is covered by the modern equivalent (FR-012). Migrations live at repo root under `migrations/`.

## Complexity Tracking

| Item | Why Needed | Simpler Alternative Rejected Because |
|------|------------|--------------------------------------|
| Two Alembic revisions | Legacy has `bibtext_id` typo (get_data.py, add.py) vs `bibtex_id` (database_connector.py); migration must handle both (FR-011 edge case) | Single revision cannot idempotently detect and rename a column that may or may not exist |
| `with_session()` helper | Deterministic commit-on-success / rollback-on-exception without repeating try/except across every repository method (constitution Principle IV) | Bare Session() calls require each caller to manage commit/rollback; leaks connections on exceptions |
| `extract_papers_from_tex_bib` as Iterator | Bulk import pipeline: tex parse -> bib parse -> yield DTO. Iterator enables per-paper commit (constitution Principle IV) | Returning a list buffers the entire dataset and prevents per-paper commit semantics |
