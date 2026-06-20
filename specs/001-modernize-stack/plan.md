# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-20 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-modernize-stack/spec.md`

## Summary

Rebuild `paper_sorts` on mainstream Python libraries (SQLAlchemy 2.x ORM,
Typer CLI, pydantic-settings config, Alembic migrations, pytest with
pytest-postgresql, ruff linter) in a src-layout package, while preserving
the existing four-table schema, all CLI flows, and the encrypted-config
workflow. Legacy flat-layout modules are removed once their functionality
is covered. The constitution is already at v1.3.0 — no further amendments needed.

---

## Technical Context

**Language/Version**: Python >= 3.11  
**Primary Dependencies**: SQLAlchemy 2.x, Typer, pydantic-settings 2, Alembic, psycopg v3 (binary), pybtex, pylatexenc, rich, cryptography  
**Storage**: PostgreSQL (local, personal-library-sized dataset)  
**Testing**: pytest + pytest-postgresql (ephemeral PG from host pg_ctl)  
**Target Platform**: Linux (local workstation)  
**Project Type**: CLI tool  
**Performance Goals**: No measurable regression vs. legacy baseline on personal-library-sized dataset  
**Constraints**: Offline, single-user, no connection pooling, no async, deterministic session lifecycle  
**Scale/Scope**: ~hundreds of papers, single user

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Touches? | Status | Notes |
|-----------|----------|--------|-------|
| I. Code Quality (ruff, type hints, docstrings, SQLAlchemy isolated to `db/`) | YES | PASS | src-layout enforces isolation; ruff in pyproject.toml |
| II. Testing Standards (pytest, pytest-postgresql, no mocking SQLAlchemy session) | YES | PASS | ephemeral DB via pytest-postgresql; real integration tests |
| III. UX Consistency (prompts via `cli/prompts.py`, 1-indexed menus, abort options, confirmations) | YES | PASS | All prompts routed through `paper_sorts.cli.prompts` |
| IV. Performance (no regression, deterministic session lifecycle, no pooling/async) | YES | PASS | `with Session(...)` context management; no pooling added |

Constitution already at v1.3.0 — all amendments are in place. No waivers needed.

**Post-design re-check**: All four principles upheld by the design (research.md R01–R12).

---

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
    ├── config.py                # pydantic-settings Settings
    ├── logging_config.py        # dictConfig + RichHandler
    ├── cli/
    │   ├── __init__.py
    │   ├── app.py               # Typer app wiring + interactive menu
    │   ├── search.py            # search subcommand
    │   ├── add.py               # add subcommand
    │   ├── update.py            # update subcommand
    │   ├── delete.py            # delete subcommand
    │   ├── migrate.py           # migrate subcommand (admin)
    │   ├── importer.py          # import subcommand (admin)
    │   └── prompts.py           # ALL user-facing I/O (Principle III gate)
    ├── services/
    │   ├── __init__.py
    │   ├── paper_service.py     # search_by_title, search_by_author, add_paper, update_field, delete_paper
    │   └── import_service.py    # extract_papers_from_tex_bib() → Iterator[PaperCreate]
    └── db/
        ├── __init__.py
        ├── models.py            # SQLAlchemy ORM models (4 tables)
        ├── repositories.py      # PaperRepository, AuthorRepository, BibRepository + DTOs
        └── session.py           # with_session(), engine factory

migrations/
├── env.py
├── script.py.mako
└── versions/
    ├── 001_initial_schema.py    # verbatim DDL port
    └── 002_fix_bibtext_typo.py  # bibtext_id → bibtex_id rename

tests/
├── conftest.py                  # postgresql_proc, ephemeral_db_url session fixtures
├── fixtures/
│   └── seed_papers.py           # SEED_PAPERS list of PaperCreate
├── test_repositories.py         # integration tests for PaperRepository
├── test_paper_service.py        # integration tests for paper_service
├── test_import_service.py       # integration tests for import_service
├── test_config.py               # unit tests for config.py
└── test_prompts.py              # unit tests for cli/prompts.py

docs/
└── architecture.md              # US1 deliverable (already written)

pyproject.toml                   # uv + hatchling, replaces pyproject.toml (poetry)
```

**Legacy files to remove** (FR-012): `paper_sorts/` flat directory (all .py files
therein) once the modernized stack is complete and all tests pass.

---

## Complexity Tracking

No constitution violations. No entries required.

---

## Implementation Phases

### Phase T01–T005: Project skeleton

- T001: Replace `pyproject.toml` (poetry → hatchling/uv); add all deps; `uv sync`
- T002: Create `src/paper_sorts/` package skeleton (all `__init__.py` files)
- T003: Initialize Alembic (`alembic init migrations`); configure `env.py`
- T004: Write Revision 001 migration (verbatim DDL from `create_tables()`)
- T005: Write Revision 002 migration (bibtext_id → bibtex_id typo fix)

### Phase T006–T012: Persistence layer (`db/`)

- T006: Write `db/models.py` (4 SQLAlchemy 2.x declarative models)
- T007: Write `db/session.py` (`with_session`, engine factory from `Settings`)
- T008: Write Pydantic DTOs in `db/repositories.py` (`PaperSummary`, `PaperCreate`)
- T009: Write `PaperRepository` in `db/repositories.py`
- T010: Write `AuthorRepository` in `db/repositories.py`
- T011: Write `BibRepository` in `db/repositories.py`
- T012: Integration tests for repositories (`tests/test_repositories.py`)

### Phase T013–T017: Service layer (`services/`)

- T013: Write `services/paper_service.py` (`search_by_title`, `search_by_author`, `add_paper`, `update_field`, `delete_paper`)
- T014: Write `services/import_service.py` (`extract_papers_from_tex_bib`)
- T015: Integration tests for paper_service (`tests/test_paper_service.py`)
- T016: Integration tests for import_service (`tests/test_import_service.py`)

### Phase T017–T022: Config + Logging

- T017: Write `config.py` (pydantic-settings, 4-source priority, FernetSettingsSource)
- T018: Write `logging_config.py` (dictConfig + RichHandler)
- T019: Unit tests for config.py (`tests/test_config.py`)

### Phase T020–T028: CLI layer (`cli/`)

- T020: Write `cli/prompts.py` (all prompt helpers: `ask_str`, `ask_choice`, `ask_confirm`)
- T021: Write `cli/app.py` (Typer app, interactive menu, callback for no-subcommand)
- T022: Write `cli/search.py` subcommand
- T023: Write `cli/add.py` subcommand
- T024: Write `cli/update.py` subcommand
- T025: Write `cli/delete.py` subcommand
- T026: Write `cli/migrate.py` subcommand
- T027: Write `cli/importer.py` subcommand
- T028: Unit tests for prompts (`tests/test_prompts.py`)

### Phase T029–T032: Test infrastructure + seed data

- T029: Write `tests/conftest.py` (pytest-postgresql fixtures, schema via Alembic)
- T030: Write `tests/fixtures/seed_papers.py` (SEED_PAPERS with legacy test rows)
- T031: Verify `uv run pytest` passes (all integration + unit tests green)
- T032: Verify `uv run ruff check src tests` and `uv run mypy src` pass

### Phase T033: Legacy removal

- T033: Remove `paper_sorts/` flat-layout directory (FR-012) once T031 green

### Phase T034: Architecture doc (US1)

- T034: Finalize `docs/architecture.md` (already written at plan time)
