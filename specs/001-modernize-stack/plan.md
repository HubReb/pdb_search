# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-15 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-modernize-stack/spec.md`

## Summary

Reverse-engineer the existing flat-layout `paper_sorts/` Python package, document it, then rebuild it on mainstream libraries (SQLAlchemy 2.x ORM, Typer CLI, pydantic-settings config, Alembic migrations, pytest + pytest-postgresql test suite, ruff linter) — preserving all user-facing behaviour while removing bespoke procedural glue code. The src-layout `src/paper_sorts/` package replaces the old flat layout entirely; legacy modules are deleted once their functionality is covered.

## Technical Context

**Language/Version**: Python ≥ 3.11 (uv-managed)
**Primary Dependencies**: SQLAlchemy 2.x, Typer, pydantic-settings v2, Alembic, psycopg v3 (binary), pybtex, pylatexenc, cryptography, rich, pytest, pytest-postgresql, ruff, mypy
**Storage**: PostgreSQL only; four-table schema (papers, bib, authors_id, authors_papers)
**Testing**: pytest + pytest-postgresql (ephemeral DB per session); ruff + mypy quality gates
**Target Platform**: Linux (personal workstation, offline use)
**Project Type**: CLI tool (single-user, personal library manager)
**Performance Goals**: No measurable regression vs. current baseline on interactive operations (search, add, update, delete) on personal-library-sized dataset
**Constraints**: Per-paper commit on bulk import; deterministic session close (context-managed `with Session()`)
**Scale/Scope**: Single user, personal dataset (~hundreds of papers); no multi-user, no network exposure

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Touch? | Status | Notes |
|-----------|--------|--------|-------|
| I. Code Quality | YES | PASS with amendments | ruff replaces pylint; ORM layer isolation enforced; doc-currency gate for README/CLAUDE.md |
| II. Testing Standards | YES | PASS with amendments | pytest replaces unittest; pytest-postgresql for ephemeral DB; no mocking SQLAlchemy/psycopg; per-layer coverage gate |
| III. UX Consistency | YES | PASS | prompts routed through `cli/prompts.py`; 1-indexed menus; abort option required; confirmation for destructive ops |
| IV. Performance | YES | PASS with benchmark harness | baseline benchmark must exist and execute under `tests/benchmarks/`; no connection pooling beyond SA default |

**Amendments required (FR-016)**: Constitution is already at v1.3.0-b2-hardened reflecting all required amendments. No silent deviations remain.

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/
│   └── cli-commands.md  # CLI subcommand grammar
└── tasks.md             # Phase 2 output (speckit-tasks)
```

### Source Code (repository root)

```text
src/
└── paper_sorts/
    ├── __init__.py
    ├── cli/
    │   ├── __init__.py
    │   ├── app.py          # Typer app wiring; top-level menu when no subcommand
    │   ├── search.py       # search subcommand
    │   ├── add.py          # add subcommand
    │   ├── update.py       # update subcommand
    │   ├── delete.py       # delete subcommand
    │   ├── migrate.py      # migrate subcommand (admin, not in menu)
    │   ├── importer.py     # import subcommand (admin, not in menu)
    │   └── prompts.py      # ALL user-facing prompt/input helpers (sole rich.prompt importer)
    ├── db/
    │   ├── __init__.py
    │   ├── models.py       # SQLAlchemy 2.x ORM models (declarative)
    │   ├── session.py      # with_session() context manager
    │   └── repositories.py # PaperRepository, AuthorRepository, BibRepository + DTOs
    ├── services/
    │   ├── __init__.py
    │   ├── paper_service.py   # search_by_title, search_by_author, add_paper, update_field, delete_paper
    │   └── import_service.py  # extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]
    ├── config.py           # pydantic-settings Settings (CLI args > env > .env > Fernet INI)
    └── logging_config.py   # single dictConfig call (RichHandler + optional FileHandler)

migrations/
├── env.py
├── script.py.mako
└── versions/
    ├── 001_initial_schema.py       # verbatim port of current DDL (bibtex_id column)
    └── 002_converge_legacy_bibtext_id.py  # handles bibtext_id typo variant

tests/
├── conftest.py                    # postgresql_proc, ephemeral_db_url fixtures
├── fixtures/
│   └── seed_papers.py             # SEED_PAPERS canonical dataset
├── test_repositories.py           # persistence layer (real DB, no mocking)
├── test_paper_service.py          # service layer
├── test_config.py                 # config unit tests
├── test_cli.py                    # CLI layer via Typer CliRunner
└── benchmarks/
    ├── bench_baseline.py          # benchmark harness (Principle IV gate)
    └── baseline.json              # recorded baseline results

docs/
└── architecture.md               # reverse-engineered legacy architecture (FR-001)
```

**Structure Decision**: src-layout single project with explicit layer separation (cli/, db/, services/). Tests in `tests/` with per-layer test files and a benchmark harness.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Benchmark harness (Principle IV gate) | Constitution requires an executing baseline benchmark | "no regression" cannot be claimed vacuously; measurement needed |

## Implementation Phases

### Phase T0: Architecture Documentation
- Reverse-engineer legacy code and produce `docs/architecture.md`
- Document: purpose, user journeys, data model, control flow, config, install/run, known limitations

### Phase T1: Project Scaffolding
- Convert pyproject.toml from Poetry to uv/hatchling (Python ≥ 3.11)
- Create `src/paper_sorts/` src-layout skeleton
- Set up ruff, mypy, pytest configuration in pyproject.toml
- Create `migrations/` Alembic environment

### Phase T2: Config & Logging
- Implement `src/paper_sorts/config.py` (pydantic-settings, four-source priority)
- Implement `src/paper_sorts/logging_config.py` (RichHandler + optional FileHandler)

### Phase T3: DB Layer
- Implement `src/paper_sorts/db/models.py` (SQLAlchemy 2.x ORM)
- Implement `src/paper_sorts/db/session.py` (with_session context manager)
- Implement `src/paper_sorts/db/repositories.py` (PaperRepository, AuthorRepository, BibRepository + DTOs)
- Create Alembic revision 001 (verbatim port of DDL)
- Create Alembic revision 002 (handle bibtext_id → bibtex_id migration)

### Phase T4: Services Layer
- Implement `src/paper_sorts/services/paper_service.py`
- Implement `src/paper_sorts/services/import_service.py`

### Phase T5: CLI Layer
- Implement `src/paper_sorts/cli/prompts.py` (all prompts route through here)
- Implement `src/paper_sorts/cli/search.py`, `add.py`, `update.py`, `delete.py`
- Implement `src/paper_sorts/cli/migrate.py`, `importer.py`
- Implement `src/paper_sorts/cli/app.py` (Typer app, top-level menu)

### Phase T6: Tests
- Implement `tests/conftest.py` with ephemeral DB fixtures
- Implement `tests/fixtures/seed_papers.py`
- Implement `tests/test_repositories.py` (real DB integration tests)
- Implement `tests/test_paper_service.py`
- Implement `tests/test_config.py`
- Implement `tests/test_cli.py` (Typer CliRunner)
- Implement `tests/benchmarks/bench_baseline.py`

### Phase T7: Legacy Removal & Doc Cleanup
- Delete legacy `paper_sorts/` flat layout
- Update README.md (no legacy tokens)
- Update CLAUDE.md (no legacy tokens, point to new commands)
