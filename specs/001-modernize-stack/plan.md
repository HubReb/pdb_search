# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-20 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-modernize-stack/spec.md`

## Summary

Rebuild the paper-sorts CLI on a modern Python stack (SQLAlchemy 2.x / psycopg v3 / Typer / pydantic-settings / pytest / ruff) while preserving the identical user-facing feature set: search by title, search by author, add, update, delete, bulk import. The legacy flat-layout procedural modules (`psycopg_db.py`, `database_connector.py`, `user_interaction.py`, `helpers.py`, `add.py`, `search.py`, `get_data.py`, `config_reader.py`, `run.py`) are replaced by a `src/paper_sorts/` src-layout package with a four-layer architecture (cli → services → db → config). An Alembic migration handles both historical schema variants (`bibtex_id` and `bibtext_id` typo). Dependency management moves from Poetry to uv + PEP 621 `pyproject.toml`. Tests migrate from hand-written unittest to pytest with ephemeral PostgreSQL via pytest-postgresql.

## Technical Context

**Language/Version**: Python >= 3.11  
**Primary Dependencies**: SQLAlchemy 2.x, psycopg v3 (binary), Typer, pydantic-settings v2, Alembic, pybtex, pylatexenc, pytest, pytest-postgresql, ruff, mypy  
**Storage**: PostgreSQL (local, single-user). Four tables: papers, bib, authors_id, authors_papers.  
**Testing**: pytest + pytest-postgresql (ephemeral DB per session; no mocking of SQLAlchemy session or driver)  
**Target Platform**: Linux CLI, offline, single-user  
**Project Type**: CLI application  
**Performance Goals**: No measurable regression versus current implementation on interactive operations (Principle IV)  
**Constraints**: No connection pooling beyond SQLAlchemy default; no async drivers; deterministic session close  
**Scale/Scope**: Personal library, ~hundreds of papers

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Code Quality — ruff, type hints, docstrings, sqlalchemy isolated to db/ | COMPLIANT | ruff replaces pylint per FR-010/FR-016 amendment already in constitution v1.3.0 |
| II. Testing Standards — pytest, pytest-postgresql, no mock of session/repo | COMPLIANT | Explicitly required by FR-008/FR-009 and already amended in constitution v1.3.0 |
| III. UX Consistency — prompts via cli/prompts.py, 1-indexed menus, confirm destructive | COMPLIANT | New module cli/prompts.py is the single prompt-routing point per constitution III |
| IV. Performance — non-regression on interactive ops, per-paper commit in bulk import | COMPLIANT | Context-managed sessions satisfy determinism requirement |

No waivers required. Constitution v1.3.0 already incorporates all amendments needed for this feature (driver isolation → layer-level, pylint → ruff, unittest → pytest, psycopg2 → psycopg v3, prompt routing to cli/prompts.py).

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output (/speckit-plan command)
├── data-model.md        # Phase 1 output (/speckit-plan command)
├── quickstart.md        # Phase 1 output (/speckit-plan command)
├── contracts/           # Phase 1 output (/speckit-plan command)
└── tasks.md             # Phase 2 output (/speckit-tasks command)
```

### Source Code (repository root)

```text
src/
└── paper_sorts/
    ├── __init__.py
    ├── cli/
    │   ├── __init__.py
    │   ├── app.py           # Typer app wiring, top-level menu
    │   ├── search.py        # search subcommand
    │   ├── add.py           # add subcommand
    │   ├── update.py        # update subcommand
    │   ├── delete.py        # delete subcommand
    │   ├── importer.py      # import subcommand (bulk tex/bib)
    │   ├── migrate.py       # migrate subcommand (admin)
    │   └── prompts.py       # ONLY module that may import rich.prompt / typer.prompt
    ├── services/
    │   ├── __init__.py
    │   ├── paper_service.py    # search_by_title, search_by_author, add_paper, update_field, delete_paper
    │   └── import_service.py   # extract_papers_from_tex_bib -> Iterator[PaperCreate]
    ├── db/
    │   ├── __init__.py
    │   ├── models.py        # SQLAlchemy ORM models (4 tables)
    │   ├── repositories.py  # PaperRepository, AuthorRepository, BibRepository + DTOs
    │   └── session.py       # with_session() context manager
    ├── config.py            # pydantic-settings Settings (4-source priority)
    └── logging_config.py    # logging.config.dictConfig setup

migrations/
├── env.py
├── script.py.mako
└── versions/
    ├── 001_initial_schema.py   # verbatim port of legacy DDL
    └── 002_converge_schema.py  # handle bibtext_id typo column

tests/
├── conftest.py                  # postgresql_proc, ephemeral_db_url fixtures
├── fixtures/
│   └── seed_papers.py           # SEED_PAPERS canonical dataset
├── test_repositories.py
├── test_services.py
├── test_config.py
├── test_cli.py
└── benchmarks/
    ├── bench_baseline.py        # @pytest.mark.skip until T046 modernizes it
    └── baseline.json
```

**Structure Decision**: Single src-layout project. All SQLAlchemy imports confined to `src/paper_sorts/db/`. No subpackages at the top level.

## Phase Details

### Phase 1: Foundation (T001–T010)

**Scope**: pyproject.toml (uv, PEP 621), src-layout package skeleton, config.py, logging_config.py, ORM models + session, initial Alembic migration (verbatim DDL), migration revision 002 (schema convergence).

**Key decisions**:
- `hatchling` build backend; `uv.lock` for reproducibility
- pydantic-settings `Settings` with four sources: CLI > env (`PDBSEARCH_*`) > `.env` > Fernet INI
- SQLAlchemy `DeclarativeBase`, `mapped_column`, 2.x style
- Alembic `env.py` uses `Settings.database_url` from `paper_sorts.config`
- `with_session()`: `contextlib.contextmanager` yielding `Session`, commit on clean exit, rollback on exception

### Phase 2: Persistence Layer (T011–T018)

**Scope**: PaperRepository, AuthorRepository, BibRepository, pydantic DTOs (`PaperSummary`, `PaperCreate`), repository tests.

**Key decisions**:
- Repository methods take a `Session` argument (caller owns session lifecycle)
- DTOs are pure pydantic models, no ORM type leaks into services/cli
- `search_by_title` / `search_by_author` do a JOIN across 4 tables — no denormalisation
- `PaperCreate` carries title, authors (list[str]), bibtex_key, summary, bibtex_text

### Phase 3: Service Layer (T019–T024)

**Scope**: `paper_service.py` (all five domain operations), `import_service.py` (tex/bib extractor), service tests.

**Key decisions**:
- Services receive `with_session` as a dependency; never import SQLAlchemy
- `update_field` uses `match`/`case` over `Literal[...]` table arg + `assert_never`
- `import_service.extract_papers_from_tex_bib` is a generator, yields `PaperCreate` objects
- Per-paper commit in bulk import (constitution IV)

### Phase 4: CLI Layer (T025–T031)

**Scope**: Typer app with subcommands, top-level four-option menu, prompts.py.

**Key decisions**:
- `pdbsearch` with no args drops into interactive four-option menu (search / add / update / delete)
- `pdbsearch search`, `pdbsearch add`, `pdbsearch update`, `pdbsearch delete` are also direct subcommands
- `pdbsearch import` and `pdbsearch migrate` are subcommand-only (admin/scripted)
- `cli/prompts.py` is the single prompt-routing point; all `input()` / `rich.prompt` calls live here
- 1-indexed menus with explicit quit option everywhere; confirmation accepts `y`/`n` and `1`/`2`

### Phase 5: Tests + Cleanup (T032–T040)

**Scope**: Ephemeral DB fixtures, seed dataset, test suite, benchmark skip stub, remove legacy modules, docs/architecture.md.

**Key decisions**:
- `pytest-postgresql` with `postgresql_proc` fixture; host `pg_ctl` at `/usr/bin/pg_ctl`
- Seed dataset: at least 3 papers, multiple authors per paper, one author shared across papers
- Legacy flat-layout modules deleted once functionality verified in new stack
- `docs/architecture.md` written as per US1 / FR-001

## Complexity Tracking

> No constitution violations requiring justification.

| Item | Decision | Rationale |
|------|----------|-----------|
| Two Alembic revisions | 001 = verbatim DDL; 002 = convergence | Single revision cannot be both "verbatim port" and "handle typo variant" — splitting keeps history readable |
| No FKs on authors_papers | Preserved from legacy DDL | Schema-preservation contract; adding FK would be a breaking change per CLAUDE.md |
| Benchmark skip | `@pytest.mark.skip` pending T046 | No modern baseline exists; running against legacy baseline would be misleading |
