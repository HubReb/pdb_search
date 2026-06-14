# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-15 | **Spec**: [spec.md](spec.md)  
**Input**: Feature specification from `specs/001-modernize-stack/spec.md`

## Summary

Rebuild `paper_sorts` on mainstream Python libraries (SQLAlchemy 2.x ORM, Typer CLI, Alembic
migrations, pydantic-settings config, pytest + pytest-postgresql test suite, ruff linter) while
preserving 100% of existing CLI behaviour and the four-table PostgreSQL schema. Legacy procedural
modules (`add.py`, `search.py`, `get_data.py`, `psycopg_db.py`, `user_interaction.py`,
`database_connector.py`, `config_reader.py`, `helpers.py`) are deleted once replaced. The
project moves to a `src/paper_sorts/` src-layout with a clean three-layer architecture
(cli → services → db), managed via uv and a PEP 621 pyproject.toml.

## Technical Context

**Language/Version**: Python >= 3.11  
**Primary Dependencies**: SQLAlchemy 2.x, psycopg v3, Typer, Alembic, pydantic-settings v2, pybtex, pylatexenc, cryptography, rich  
**Storage**: PostgreSQL (single local instance, personal-library scale)  
**Testing**: pytest, pytest-postgresql (host pg_ctl at `/usr/bin/pg_ctl`), pytest-cov  
**Target Platform**: Linux CLI (personal offline tool)  
**Project Type**: CLI application  
**Performance Goals**: No measurable regression vs. current implementation (measured on seeded DB)  
**Constraints**: Offline-only, single-user, no connection pool beyond SQLAlchemy default, sessions closed deterministically, no async drivers  
**Scale/Scope**: Personal library (~100–500 papers)

## Constitution Check

| Principle | Touched | Status | Notes |
|-----------|---------|--------|-------|
| I. Code Quality | Yes | PASS | ruff replaces pylint (already in v1.3.0); SQLAlchemy isolated to `db/`; type hints + docstrings required on all public APIs; doc-currency gate enforced by test |
| II. Testing Standards | Yes | PASS | pytest + pytest-postgresql (ephemeral DB); no mocking of SQLAlchemy session/repos; per-layer 80% coverage gate; no placeholder failing tests |
| III. UX Consistency | Yes | PASS | All prompts route through `cli/prompts.py`; 1-indexed menus; every menu has abort/quit; confirmations accept numeric + word forms |
| IV. Performance | Yes | PASS | Non-regression vs. baseline measured by `tests/benchmarks/bench_baseline.py`; no async drivers, no connection pooling beyond default; per-paper commit in bulk import |

**Gate result**: PASS — no constitution violations. No waivers required.

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

### Source Code Layout (target)

```text
src/
└── paper_sorts/
    ├── __init__.py
    ├── config.py              # pydantic-settings Settings + Fernet source
    ├── logging_config.py      # dictConfig helper
    ├── cli/
    │   ├── __init__.py
    │   ├── app.py             # Typer root + interactive menu
    │   ├── search.py          # 'search' subcommand
    │   ├── add.py             # 'add' subcommand
    │   ├── update.py          # 'update' subcommand
    │   ├── delete.py          # 'delete' subcommand
    │   ├── importer.py        # 'import' subcommand
    │   ├── migrate.py         # 'migrate' subcommand
    │   └── prompts.py         # ALL user-facing prompts (Principle III)
    ├── services/
    │   ├── __init__.py
    │   ├── paper_service.py   # search_by_title, search_by_author, add_paper, update_field, delete_paper
    │   └── import_service.py  # extract_papers_from_tex_bib()
    └── db/
        ├── __init__.py
        ├── models.py          # SQLAlchemy ORM models
        ├── repositories.py    # PaperRepository, AuthorRepository, BibRepository + DTOs
        └── session.py         # with_session(), engine factory

tests/
├── conftest.py            # postgresql_proc, ephemeral_db_url, session fixtures
├── fixtures/
│   └── seed_papers.py     # SEED_PAPERS canonical dataset
├── test_repositories.py   # persistence layer integration tests
├── test_services.py       # service layer integration tests
├── test_cli.py            # CLI layer tests via Typer CliRunner
├── test_config.py         # config unit tests
├── test_migrations.py     # Alembic migration tests
└── benchmarks/
    ├── bench_baseline.py   # wall-clock benchmark (MUST execute, not skipped)
    └── baseline.json       # recorded baseline

migrations/
├── env.py
├── script.py.mako
└── versions/
    ├── 001_initial_schema.py    # canonical four-table schema
    └── 002_fix_bibtext_typo.py  # legacy bibtext_id → bibtex_id

docs/
└── architecture.md        # reverse-engineered legacy architecture doc (US1)
```

## Complexity Tracking

> No constitution violations — this section is for record only.

| Decision | Justification |
|----------|---------------|
| Two Alembic revisions | Revision 001 = canonical schema; Revision 002 = legacy typo variant fix. Cannot collapse because idempotency requires checking column existence |
| `migrate` + `import` subcommand-only | Admin/scripted operations; not part of the four-option interactive UX |
| `src/` layout | PEP 621 + uv standard; avoids import-without-install confusion |

## Implementation Phases

### Phase T0: Architecture Document

- Write `docs/architecture.md` covering legacy stack (US1 / FR-001)

### Phase T1: Project Scaffolding

- Replace `pyproject.toml` (Poetry → uv / PEP 621 / hatchling)
- Create `src/paper_sorts/` package skeleton
- Wire Alembic (`migrations/`, `alembic.ini`)
- Configure ruff + mypy in `pyproject.toml`

### Phase T2: Persistence Layer (`db/`)

- `db/models.py` — ORM models
- `db/session.py` — `with_session()`, engine factory
- `db/repositories.py` — PaperRepository, AuthorRepository, BibRepository + DTOs

### Phase T3: Alembic Migrations

- Revision 001 — initial canonical schema
- Revision 002 — legacy `bibtext_id` → `bibtex_id` fix

### Phase T4: Service Layer (`services/`)

- `services/paper_service.py`
- `services/import_service.py`

### Phase T5: CLI Layer (`cli/`)

- `cli/prompts.py` — all prompts
- `cli/app.py` — Typer root + interactive menu
- Individual subcommand modules

### Phase T6: Configuration & Logging

- `config.py` — pydantic-settings + Fernet source
- `logging_config.py` — dictConfig

### Phase T7: Test Suite

- `tests/conftest.py` + fixtures
- `tests/test_repositories.py`
- `tests/test_services.py`
- `tests/test_cli.py`
- `tests/test_config.py`
- `tests/test_migrations.py`
- `tests/benchmarks/bench_baseline.py`

### Phase T8: Remove Legacy Code

- Delete `paper_sorts/` flat layout
- Update `CLAUDE.md` and `README.md` (doc-currency gate)

### Phase T9: Green Build

- Pass `ruff check`, `mypy src`, `pytest` with all layers ≥ 80% coverage
