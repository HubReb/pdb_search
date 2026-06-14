# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-15 | **Spec**: [spec.md](spec.md)  
**Input**: Feature specification from `specs/001-modernize-stack/spec.md`

## Summary

Rebuild `paper_sorts` from a flat-layout Poetry/psycopg2/argparse/unittest codebase to a modern src-layout package using uv/hatchling, SQLAlchemy 2.x + psycopg v3, Typer CLI, Alembic migrations, pydantic-settings config, ruff linting, and a pytest suite with ephemeral PostgreSQL. All existing CLI behaviour (search, add, update, delete, import) is preserved. Legacy procedural modules (`add.py`, `search.py`, `get_data.py`) are removed once replaced. Architecture is documented first; modernization is validated against it.

## Technical Context

**Language/Version**: Python ≥ 3.11  
**Primary Dependencies**: SQLAlchemy 2.x, psycopg v3 (binary), Typer, Alembic, pydantic-settings v2, pybtex, pylatexenc, cryptography, rich, ruff, mypy, pytest, pytest-postgresql, pytest-benchmark  
**Storage**: PostgreSQL (local); four existing tables (papers, bib, authors_id, authors_papers)  
**Testing**: pytest + pytest-postgresql (ephemeral cluster via host `pg_ctl` at `/usr/bin/pg_ctl`)  
**Target Platform**: Linux/macOS CLI; single-user local  
**Project Type**: CLI application  
**Performance Goals**: No measurable regression vs. current implementation (per constitution Principle IV); benchmark must execute and record results  
**Constraints**: Offline, single-user. No connection pools beyond SQLAlchemy default. No async. Sessions closed deterministically.  
**Scale/Scope**: Personal library (~hundreds of papers)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Code Quality | REQUIRES AMENDMENT | Amend: psycopg2 isolation → SQLAlchemy/db/ isolation; pylint → ruff. Amendment already in constitution v1.3.0-b2-hardened. |
| II. Testing Standards | REQUIRES AMENDMENT | Amend: unittest → pytest; pytest-postgresql named. Amendment already in constitution v1.3.0-b2-hardened. |
| III. UX Consistency | REQUIRES AMENDMENT | Amend: prompt routing from `helpers.get_user_input` to `paper_sorts.cli.prompts`. Amendment already in constitution v1.3.0-b2-hardened. |
| IV. Performance Requirements | SATISFIABLE | Benchmark harness required (G2 gate). Must not be permanently skipped. Baseline recorded before legacy removal. |

**Post-design re-check**: All four principles satisfied by design decisions in research.md. Constitution amendments already ratified (v1.3.0-b2-hardened). Complexity Tracking below covers G2 waiver.

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/
│   └── cli-commands.md  # CLI subcommand contract
└── tasks.md             # Phase 2 output (speckit-tasks)
```

### Source Code (repository root)

```text
src/
└── paper_sorts/
    ├── __init__.py
    ├── config.py                # pydantic-settings Settings model
    ├── logging_config.py        # dictConfig setup; called once at startup
    ├── cli/
    │   ├── __init__.py
    │   ├── app.py               # Typer app + top-level menu when no subcommand
    │   ├── search_cmd.py        # `pdbsearch search` subcommand
    │   ├── add_cmd.py           # `pdbsearch add` subcommand
    │   ├── update_cmd.py        # `pdbsearch update` subcommand
    │   ├── delete_cmd.py        # `pdbsearch delete` subcommand
    │   ├── import_cmd.py        # `pdbsearch import` subcommand
    │   ├── migrate_cmd.py       # `pdbsearch migrate` subcommand
    │   └── prompts.py           # All bare input() calls; the ONLY place importing rich.prompt
    ├── db/
    │   ├── __init__.py
    │   ├── models.py            # SQLAlchemy ORM models (Paper, BibEntry, Author, AuthorPaper)
    │   ├── repositories.py      # PaperRepository, AuthorRepository, BibRepository + DTOs
    │   └── session.py           # with_session() context manager; engine factory
    └── services/
        ├── __init__.py
        ├── paper_service.py     # search_by_title, search_by_author, add_paper, update_field, delete_paper
        └── import_service.py    # extract_papers_from_tex_bib() -> Iterator[PaperCreate]

migrations/
├── env.py
├── script.py.mako
└── versions/
    ├── 001_initial_schema.py    # Verbatim DDL port
    └── 002_converge_bibtext_typo.py  # Rename bibtext_id → bibtex_id if present

tests/
├── conftest.py                  # postgresql_proc, ephemeral_db_url fixtures
├── fixtures/
│   └── seed_papers.py          # SEED_PAPERS canonical dataset
├── test_repositories.py         # Persistence layer: CRUD + search + migration
├── test_services.py             # Service layer: domain logic
├── test_cli.py                  # CLI layer: Typer CliRunner for all subcommands
├── test_config.py               # Config layer: env vars, .env, missing key
└── benchmarks/
    ├── conftest.py              # Benchmark-specific fixtures
    └── bench_baseline.py        # pytest-benchmark: search, add, update, delete
```

**Structure Decision**: src-layout single project. Legacy `paper_sorts/` flat layout is removed once all tasks passing.

## Complexity Tracking

| Item | Why Needed | Simpler Alternative Rejected Because |
|------|------------|-------------------------------------|
| G2: Benchmark harness (tests/benchmarks/) | Constitution Principle IV baseline-benchmark gate requires executing benchmark; "no measurable regression" is vacuously true without it | Skipping benchmark would violate a merge-blocking gate |
| Alembic rev 002 (bibtext_id typo handling) | Two historical schema variants exist; migration must be idempotent across both | Documenting-only without migration would break existing personal databases on the old schema |
| Per-layer coverage gate (G1) | Constitution Principle II requires each of four layers to hit ≥80% independently | An aggregate figure could mask an untested CLI layer; Typer CliRunner allows testing interactive CLI without bare `input()` |
