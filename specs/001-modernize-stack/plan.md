# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-14 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-modernize-stack/spec.md`

## Summary

Replace the legacy flat-layout `paper_sorts/` package (poetry, psycopg2, argparse, unittest, pylint, hand-written SQL) with a modern `src/paper_sorts/` src-layout package using uv, SQLAlchemy 2.x + psycopg v3, Typer CLI, Alembic migrations, pydantic-settings config, ruff linting, and pytest + pytest-postgresql. All user-facing behaviour (search by title/author, add, update, delete, bulk import) is preserved. An architecture document is produced first to serve as the acceptance reference. Constitution v1.3.0 amendments are pre-applied.

## Technical Context

**Language/Version**: Python ≥ 3.11  
**Primary Dependencies**: SQLAlchemy 2.x, psycopg v3 (binary), Typer, Alembic, pydantic-settings v2, pybtex, pylatexenc, ruff, mypy, pytest, pytest-postgresql  
**Storage**: PostgreSQL (local, single-user)  
**Testing**: pytest + pytest-postgresql (ephemeral DB via host pg_ctl)  
**Target Platform**: Linux CLI, offline  
**Project Type**: CLI tool  
**Performance Goals**: No measurable regression vs. current implementation on interactive operations (constitution Principle IV)  
**Constraints**: Personal-library-sized dataset; no connection pooling beyond SQLAlchemy default; context-managed sessions; no async; CLI only  
**Scale/Scope**: Single user, ~hundreds of papers

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Touches? | Status |
|-----------|----------|--------|
| I. Code Quality — ruff, type hints, docstrings, persistence-layer isolation, doc-currency gate | YES | PASS — constitution v1.3.0 already amended pylint→ruff; driver isolation mapped to `db/` layer |
| II. Testing Standards — pytest, pytest-postgresql, real DB, per-layer 80% coverage gate | YES | PASS — constitution v1.3.0 already amended unittest→pytest; no mocking of SQLAlchemy session |
| III. UX Consistency — prompts via `cli/prompts.py`, 1-indexed menus, abort options, confirmations | YES | PASS — constitution v1.3.0 already amended prompt-routing reference |
| IV. Performance — non-regression vs. baseline; benchmark harness gate | YES | PASS — baseline benchmark harness planned under `tests/benchmarks/` (R14) |

No constitution violations. No waivers required.

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md              # This file
├── research.md          # Phase 0 output (already complete)
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
    ├── cli/
    │   ├── __init__.py
    │   ├── app.py          # Typer app wiring + top-level menu
    │   ├── search.py       # search subcommands
    │   ├── add.py          # add subcommand
    │   ├── update.py       # update subcommand
    │   ├── delete.py       # delete subcommand
    │   ├── importer.py     # import subcommand
    │   ├── migrate.py      # migrate subcommand
    │   └── prompts.py      # SOLE importer of rich.prompt/typer.prompt
    ├── services/
    │   ├── __init__.py
    │   ├── paper_service.py   # domain operations (no SQL, no I/O)
    │   └── import_service.py  # bulk import extraction
    ├── db/
    │   ├── __init__.py
    │   ├── models.py       # SQLAlchemy DeclarativeBase ORM models
    │   ├── repositories.py # PaperRepository, AuthorRepository, BibRepository + DTOs
    │   └── session.py      # with_session() context manager
    ├── config.py           # pydantic-settings Settings (4-source chain)
    └── logging_config.py   # dictConfig setup

migrations/
├── env.py
├── script.py.mako
└── versions/
    ├── 001_initial_schema.py
    └── 002_handle_legacy_bibtext_id.py

tests/
├── conftest.py              # postgresql_proc, ephemeral_db_url fixtures
├── fixtures/
│   └── seed_papers.py       # SEED_PAPERS dataset
├── test_repositories.py     # persistence-layer integration tests
├── test_services.py         # service-layer unit tests
├── test_cli.py              # CLI layer tests via CliRunner
├── test_config.py           # config unit tests
├── test_migrations.py       # Alembic migration tests
├── test_doc_currency.py     # doc-currency gate (constitution I)
└── benchmarks/
    ├── bench_baseline.py    # wall-clock baseline benchmark
    └── baseline.json        # recorded baseline

docs/
└── architecture.md          # US1 deliverable (reverse-engineered doc)

pyproject.toml               # PEP 621, hatchling, uv deps
uv.lock
alembic.ini
```

**Structure Decision**: Single src-layout project. Four-layer architecture: `cli/` → `services/` → `db/` → `config.py`. Legacy `paper_sorts/` flat layout is removed in task T025 after all functionality is ported.

## Phase 0: Research Summary

Research is complete in `research.md`. Key decisions:
- ORM: SQLAlchemy 2.x + psycopg v3 (R1)
- CLI: Typer (R2)
- Config: pydantic-settings v2 with FernetIniSettingsSource (R3)
- Migrations: Alembic (R4)
- Tests: pytest + pytest-postgresql (R5)
- Lint: ruff (R6)
- BibTeX: pybtex + pylatexenc (R7)
- Build: uv + hatchling (R8)
- Architecture: four-layer src-layout (R9)
- Constitution amendments: pre-applied in v1.3.0 (R10)
- Canonical schema: four tables, no extra FKs on authors_papers (R11)
- Legacy survey: 9 modules, ~2323 LOC to remove (R12)
- Benchmark gate: tests/benchmarks/bench_baseline.py (R14)
- Doc-currency gate: tests/test_doc_currency.py (R15)

## Phase 1: Design Artifacts

Produced in this planning phase:
- `data-model.md` — entity definitions, ORM field mapping
- `contracts/cli-commands.md` — CLI subcommand grammar and prompts contract
- `quickstart.md` — user-facing install/run instructions

## Complexity Tracking

> No constitution violations — table left intentionally minimal.

| Deviation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| FernetIniSettingsSource custom pydantic source | Preserve backward-compat with existing encrypted config files | Dropping encrypted config would break existing installations |
| Alembic Revision 002 (bibtext_id rename guard) | Legacy modules used wrong column name; personal DB may be in either state | A single migration without the guard would fail on databases created by legacy add.py |
