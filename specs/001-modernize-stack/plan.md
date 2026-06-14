# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-14 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-modernize-stack/spec.md`

## Summary

Reverse-engineer and document the legacy `paper_sorts/` flat-layout codebase (psycopg2, argparse, unittest,
bespoke logging), then rebuild it on mainstream libraries: SQLAlchemy 2.x + Alembic (persistence),
Typer (CLI), pydantic-settings v2 (config), ruff (lint), pytest + pytest-postgresql (tests), uv/hatchling
(packaging). The modern code lives in `src/paper_sorts/` (src-layout); the legacy flat-layout modules are
removed once all functionality is covered. Same four-table PostgreSQL schema, same user-facing flows.

## Technical Context

**Language/Version**: Python ≥ 3.11 (raised from 3.10 — psycopg v3 binary + pydantic-settings v2 require it)
**Primary Dependencies**: SQLAlchemy 2.x, Alembic, Typer, pydantic-settings v2, psycopg[binary] v3,
pybtex, pylatexenc, cryptography, ruff, mypy, pytest, pytest-postgresql, pytest-cov
**Storage**: PostgreSQL only — four tables (papers, bib, authors_id, authors_papers). No schema drift allowed.
**Testing**: pytest + pytest-postgresql (ephemeral DB per session). Real DB; no mocking SQLAlchemy or psycopg.
**Target Platform**: Linux, local machine, single-user. No network exposure.
**Project Type**: CLI tool (personal-use, offline)
**Performance Goals**: Non-regression vs. current implementation on personal-library-sized dataset.
Baseline benchmark harness must exist and execute (constitution Principle IV gate).
**Constraints**: No connection pooling beyond SQLAlchemy defaults. No async. No new tables/indexes. Sessions
must be context-managed (deterministic close). No credentials in logs or repo.
**Scale/Scope**: Personal library (~hundreds of papers). Four tables. Five interactive CLI operations.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Code Quality (ruff, type hints, doc-currency gate) | ✅ UPHELD | ruff replaces pylint (amendment already in constitution v1.3.0). Doc-currency gate: README/CLAUDE.md must not mention Poetry/psycopg2/UserInteraction/PsycopgDB after legacy removal. |
| II. Testing Standards (pytest, real DB, per-layer coverage gate) | ✅ UPHELD | pytest + pytest-postgresql for ephemeral DB. Per-layer 80% coverage gate is mechanical and merge-blocking (constitution v1.3.0-b2-hardened G1). |
| III. UX Consistency (prompts via cli/prompts.py, numbered menus, abort option, destructive confirmations) | ✅ UPHELD | All prompts routed through `paper_sorts.cli.prompts`. 1-indexed menus with explicit quit option. Update/delete require confirmation. |
| IV. Performance (non-regression, baseline benchmark must execute) | ✅ UPHELD | Benchmark harness under tests/benchmarks/. Must NOT be permanently skipped (constitution G2). Sessions context-managed; no pools. |

**Required amendments already incorporated in constitution v1.3.0**:
- psycopg2 → psycopg v3 driver isolation rule (Principle I)
- pylint → ruff (Principle I)
- unittest → pytest (Principle II)
- prompt routing: helpers.get_user_input → paper_sorts.cli.prompts (Principle III)

No new violations. No waivers needed.

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   └── cli-commands.md  # CLI subcommand contract
└── tasks.md             # Phase 2 output (/speckit-tasks)
```

### Source Code (repository root)

```text
src/
└── paper_sorts/
    ├── __init__.py
    ├── config.py           # pydantic-settings Settings model
    ├── logging_config.py   # dictConfig, called once at startup
    ├── cli/
    │   ├── __init__.py
    │   ├── app.py          # Typer app, wires subcommands, top-level menu
    │   ├── prompts.py      # ALL user-facing prompts live here (constitution III)
    │   ├── search.py       # search subcommand
    │   ├── add.py          # add subcommand
    │   ├── update.py       # update subcommand
    │   ├── delete.py       # delete subcommand
    │   ├── importer.py     # import subcommand (bulk import)
    │   └── migrate.py      # migrate subcommand (Alembic upgrade)
    ├── services/
    │   ├── __init__.py
    │   ├── paper_service.py   # search_by_title, search_by_author, add_paper, update_field, delete_paper
    │   └── import_service.py  # extract_papers_from_tex_bib -> Iterator[PaperCreate]
    └── db/
        ├── __init__.py
        ├── models.py          # SQLAlchemy ORM models (Paper, Author, Bib, AuthorPaper)
        ├── repositories.py    # PaperRepository, AuthorRepository, BibRepository + DTOs
        └── session.py         # with_session(engine) context manager

migrations/
├── env.py
├── script.py.mako
└── versions/
    └── 001_initial_schema.py  # Verbatim port of current DDL

tests/
├── conftest.py                # postgresql_proc, ephemeral_db_url, seeded_session fixtures
├── fixtures/
│   └── seed_papers.py         # SEED_PAPERS constant (canonical dataset)
├── test_repositories.py       # persistence layer integration tests
├── test_services.py           # service layer tests (use real ephemeral DB)
├── test_cli.py                # CLI layer tests via Typer CliRunner
├── test_config.py             # config unit tests
├── test_migration.py          # Alembic migration tests
└── benchmarks/
    ├── bench_baseline.py      # timing harness for interactive operations
    └── baseline.json          # recorded baseline

pyproject.toml    # uv/hatchling, ruff, mypy, pytest config
uv.lock
docs/
└── architecture.md            # US1 deliverable — reverse-engineered legacy architecture

README.md         # updated: uv commands, modern stack, no legacy tokens
CLAUDE.md         # updated: modern stack references
```

**Structure Decision**: Single src-layout package under `src/paper_sorts/`. Three-layer architecture
(cli → services → db) with strict import direction (no sqlalchemy in services or cli).

## Complexity Tracking

> No constitution violations requiring justification.

| Item | Decision | Rationale |
|------|----------|-----------|
| Benchmark gate (Principle IV) | `tests/benchmarks/bench_baseline.py` must NOT be permanently `@pytest.mark.skip`'d | Constitution v1.3.0-b2-hardened G2: absence of executing benchmark is a violation. The harness records baseline.json on first run and asserts non-regression on subsequent runs. |
| Per-layer coverage gate (Principle II) | Each of four layers must independently hit 80% line coverage | Constitution G1: aggregate 80% that hides a 0%-covered layer is a violation. CLI layer satisfiable via Typer CliRunner end-to-end tests. |
| Doc-currency gate (Principle I) | README.md and CLAUDE.md must not contain: `Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB` | Constitution G3: enforced by a test (test_doc_currency.py) that fails if any forbidden token is found. |
| Legacy schema variants | Migration 001 must handle both `bibtex_id` and `bibtext_id` (sic) column names | Edge case from spec: users may have either schema variant. Migration detects and renames the typo column. |
| No FK on authors_papers | Schema preservation rule — do not add FKs not in original DDL | authors_papers has no DDL-level FKs in the original; schema preservation contract forbids adding them. |
