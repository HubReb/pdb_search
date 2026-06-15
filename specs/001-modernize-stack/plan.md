# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-15 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-modernize-stack/spec.md`

## Summary

Rebuild the bespoke `paper_sorts` CLI on mainstream Python libraries with no
observable change to user-facing behaviour. The legacy flat-layout package
(hand-written `psycopg2` SQL strings, `argparse` dialog loop, `unittest`
suite, encrypted-INI-only config, runtime `create_tables()`) is replaced by a
`src/paper_sorts/` package layered into `cli/` (Typer), `services/` (pure
orchestration), `db/` (SQLAlchemy 2.x repositories + pydantic DTOs), plus
`config.py` (pydantic-settings, four-source priority chain) and
`logging_config.py` (stdlib `dictConfig`). Schema lives in declarative ORM
models managed by Alembic; a `migrate` command converges both historical
column-name variants (`bibtex_id` and the legacy `bibtext_id` typo) onto the
canonical schema with zero data loss, idempotently. Tests run against an
ephemeral PostgreSQL spun up by `pytest-postgresql` — no developer-local DB.
The four-table schema, its relationships, and the exact CLI prompt grammar are
preserved as contracts.

## Technical Context

**Language/Version**: Python ≥ 3.11 (raised from the legacy 3.10 minimum; constitution Stack & Constraints)
**Primary Dependencies**: SQLAlchemy 2.x (ORM, isolated to `db/`), psycopg v3 binary (driver), Alembic (migrations), Typer (CLI), pydantic-settings v2 (config), pydantic v2 (DTOs), pybtex (BibTeX parse), pylatexenc (LaTeX→text for bulk import), cryptography/Fernet (encrypted-config source), rich (Typer dep; menus/tables)
**Storage**: PostgreSQL only; driver `psycopg` v3. Four tables: `papers`, `bib`, `authors_id`, `authors_papers`.
**Testing**: `pytest` with `pytest-postgresql` (ephemeral PG off host `pg_ctl`), `pytest-cov` for coverage. No mocking of the SQLAlchemy session, repositories, or driver in persistence tests (Principle II).
**Target Platform**: Linux/macOS CLI, single-user, offline.
**Project Type**: Single-project src-layout CLI application.
**Performance Goals**: No measurable regression vs. the legacy baseline on a personal-library-sized dataset (Principle IV / SC-006). No absolute latency bound.
**Constraints**: CLI-only (FR-017); persistence-layer driver isolation (Principle I); deterministic session close, no pools/caches/async (Principle IV); schema-preservation — no new NOT NULL outside PKs, no FK on `authors_papers`, no new indexes beyond the original PKs/uniques.
**Scale/Scope**: ~dozens–hundreds of papers; ~2 000 legacy LOC to be reduced ≥30% (SC-005).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

The constitution (v1.3.0) is already largely written against the modern stack.
This feature touches all four principles:

| Principle | Touched? | Status |
|-----------|----------|--------|
| I. Code Quality | Yes | UPHELD. `src/paper_sorts/` passes `ruff check`/`ruff format --check`; full type hints + docstrings; `sqlalchemy`/driver imports confined to `db/`. |
| II. Testing Standards | Yes | UPHELD. Tests under `tests/`, `pytest` discovery; persistence tests on ephemeral PostgreSQL via `pytest-postgresql`, no session/repo/driver mocking; seed fixture co-located; no always-failing placeholder. |
| III. UX Consistency | Yes | UPHELD. All prompts route through `cli/prompts.py`; 1-indexed menus with explicit abort/quit; dual-form confirmations; plain-language errors on stdout, technical detail to logs. |
| IV. Performance | Yes | UPHELD. Search via parameterised joins over the existing four-table schema; sessions context-managed; bulk import commits per-paper; no new tables/indexes/denormalisation; no pools/caches/async. |

**Required amendment (FR-016 / SC-007)**: The constitution's *Core Principles*
and *Stack & Constraints* sections already reference the modern stack (ruff,
pytest, pytest-postgresql, `cli/prompts`, SQLAlchemy-session isolation,
pydantic-settings). However the **Development Workflow & Quality Gates**
section still mandates `pylint paper_sorts`, the `unittest` suite, and
`DatabaseConnector.create_tables()` schema updates — direct conflicts with
FR-010, FR-009, and FR-005. Per FR-016 these MUST be amended via
`/speckit-constitution`, not silently violated. The amendment (PATCH-level —
aligning a stale workflow section with already-ratified principles) is task
**T001** and lands before the conflicting code. See research.md R10.

No principle requires a waiver. **Complexity Tracking is empty.**

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── cli-commands.md
│   └── repositories.md
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created here)
```

### Source Code (repository root)

```text
src/paper_sorts/
├── __init__.py
├── config.py                 # pydantic-settings Settings, four-source chain
├── logging_config.py         # stdlib dictConfig (RichHandler + optional FileHandler)
├── cli/
│   ├── __init__.py
│   ├── app.py                # Typer app; wires subcommands; no-subcommand → top menu
│   ├── prompts.py            # ONLY module allowed to import rich.prompt
│   ├── search.py
│   ├── add.py
│   ├── update.py
│   ├── delete.py
│   ├── importer.py
│   └── migrate.py
├── services/
│   ├── __init__.py
│   ├── paper_service.py      # search/add/update/delete orchestration over DTOs
│   └── import_service.py     # extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]
└── db/
    ├── __init__.py
    ├── models.py             # four declarative ORM models
    ├── session.py            # with_session(...) context manager
    └── repositories.py       # PaperRepository / AuthorRepository / BibRepository + DTOs

migrations/
├── env.py
├── script.py.mako
└── versions/
    ├── 001_initial_schema.py  # verbatim port of legacy DDL
    └── 002_converge_legacy.py # rename bibtext_id → bibtex_id, etc. (idempotent)

tests/
├── conftest.py               # postgresql_proc, ephemeral_db_url, seeded session
├── fixtures/
│   └── seed_papers.py        # SEED_PAPERS canonical dataset (+ .tex/.bib pair)
├── test_repositories.py
├── test_paper_service.py
├── test_import_service.py
├── test_migration.py
├── test_config.py
├── test_prompts.py
└── test_cli.py

docs/
└── architecture.md           # FR-001 reverse-engineering document

pyproject.toml                # PEP 621, hatchling backend, uv-managed
alembic.ini
```

**Structure Decision**: Single-project src-layout. The three-layer split
(`cli/` presentation → `services/` domain → `db/` persistence) plus `config.py`
satisfies FR-014's layered-architecture requirement and Principle I's
driver-isolation rule (only `db/` imports `sqlalchemy`/`psycopg`). The legacy
flat `paper_sorts/` package is deleted in the same change set (FR-012) once the
modern stack covers its functionality.

## Complexity Tracking

> No constitution violations. Section intentionally empty.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| — | — | — |
