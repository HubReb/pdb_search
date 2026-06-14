# Research: Modernize the Stack

**Feature**: 001-modernize-stack  
**Date**: 2026-06-15

## R1 — CLI Framework Choice

**Decision**: Typer (built on Click)

**Rationale**: Typer produces Typer-style subcommand apps with minimal boilerplate, native type-hint
integration, and automatic `--help` generation. The spec requires subcommands (`search`, `add`,
`update`, `delete`, `import`, `migrate`) which map cleanly to Typer commands grouped in a Typer
app. Typer 0.12+ supports `typer.main.get_command()` for testing via Click's CliRunner.

**Alternatives considered**:
- Click directly: more boilerplate, same testing story
- argparse (current): bespoke `match/case` loop replaces framework; no `--help` generation
- Cement: heavier, less recognised

## R2 — ORM / Database Toolkit Choice

**Decision**: SQLAlchemy 2.x (declarative ORM) with psycopg v3 driver

**Rationale**: SQLAlchemy 2.x is the dominant Python ORM. Its `select()` builder produces
parameterised queries, supports joins, and the `Session` context manager satisfies the
constitution's "closed deterministically" requirement. The constitution's Stack & Constraints
section explicitly names SQLAlchemy 2.x + psycopg v3.

**Alternatives considered**:
- Tortoise-ORM: async-first; async drivers are explicitly out of scope per constitution Principle IV
- SQLModel: thin SQLAlchemy wrapper; extra dependency with no benefit here
- Raw psycopg v3: loses parameterised query builder; violates spirit of FR-004

## R3 — Migration Tool Choice

**Decision**: Alembic

**Rationale**: Alembic is the de facto migration tool for SQLAlchemy projects. It generates
versioned, reversible migration scripts under `migrations/versions/` and applies them via
`alembic upgrade head`. The migration command (`pdbsearch migrate`) simply calls Alembic
programmatically.

**Alternatives considered**:
- Flyway: JVM-based, not Python-native
- yoyo-migrations: less integration with SQLAlchemy ORM models

## R4 — Settings / Configuration Library

**Decision**: pydantic-settings v2 with a custom Fernet source

**Rationale**: pydantic-settings v2 natively chains env vars, `.env` files, and custom sources.
The Fernet-encrypted INI source is preserved by writing a `BaseSettings` source class. Priority
order (CLI flags > env > `.env` > Fernet INI) is achieved via source ordering.
Environment variable prefix: `PDBSEARCH_`.

**Alternatives considered**:
- dynaconf: flexible but heavier; less Pydantic-native
- python-decouple: no custom source support

## R5 — Test Framework

**Decision**: pytest + pytest-postgresql

**Rationale**: pytest is the spec-mandated framework (FR-009). pytest-postgresql spins up an
ephemeral PostgreSQL per test session using the host's `pg_ctl` at `/usr/bin/pg_ctl`. This
satisfies US3 and SC-003 (no personal database required). The constitution Principle II forbids
mocking the SQLAlchemy session or repositories.

## R6 — Linter / Formatter

**Decision**: ruff

**Rationale**: ruff is the fastest Python linter/formatter, with built-in support for flake8,
isort, and pyupgrade rule sets. Constitution Principle I (already v1.3.0) names ruff explicitly,
superseding the old pylint reference.

## R7 — Build / Package Manager

**Decision**: uv with hatchling build backend, PEP 621 pyproject.toml

**Rationale**: Constitution Stack & Constraints names uv explicitly. pyproject.toml with
`[project]` table and `[tool.hatch.build]` is the PEP 621 layout. `uv sync --all-extras`
installs runtime + dev extras.

## R8 — Schema & Legacy Migration Strategy

**Decision**: Two Alembic migrations

- Revision 001: canonical schema as it was in the original `DatabaseConnector` code (column
  `bibtex_id`, four tables, no DDL FKs on authors_papers). This is the verbatim port.
- Revision 002: handles the legacy `bibtext_id` typo variant — if the column exists, renames it
  to `bibtex_id`.

**Rationale**: The spec's edge case list includes both schema variants. The migration must be
idempotent (Alembic's version table ensures this). The `pdbsearch migrate` subcommand calls
`alembic upgrade head`.

**Schema preservation contract** (from CLAUDE.md):
- No NOT NULL outside PKs
- No DDL FKs on `authors_papers`
- No extra indexes beyond original PKs

## R9 — Architecture: Package Layout

**Decision**: `src/paper_sorts/` src-layout with sub-packages:

```
src/paper_sorts/
├── __init__.py
├── config.py           ← pydantic-settings Settings
├── logging_config.py   ← dictConfig helper, called once from cli/app.py
├── cli/
│   ├── app.py          ← Typer root app + top-level menu
│   ├── search.py       ← 'search' subcommand
│   ├── add.py          ← 'add' subcommand
│   ├── update.py       ← 'update' subcommand
│   ├── delete.py       ← 'delete' subcommand
│   ├── importer.py     ← 'import' subcommand
│   ├── migrate.py      ← 'migrate' subcommand
│   └── prompts.py      ← ALL user-facing prompts live here (Principle III)
├── services/
│   ├── paper_service.py    ← domain ops (search, add, update, delete)
│   └── import_service.py   ← bulk import logic
└── db/
    ├── models.py        ← SQLAlchemy ORM models
    ├── repositories.py  ← PaperRepository, AuthorRepository, BibRepository + DTOs
    └── session.py       ← with_session() context manager
```

Layering rule: `db/` only imports sqlalchemy; `services/` depends on DTOs from `db/`; `cli/`
calls service functions.

## R10 — Constitution Amendments Required (FR-016)

The constitution v1.3.0-b2-hardened already contains the amendments needed:
- Principle I: driver isolation rewritten to SQLAlchemy layer (not psycopg2 per-class)
- Principle II: pytest + pytest-postgresql (not unittest)
- Principle III: prompts route through `paper_sorts.cli.prompts` (not `helpers.get_user_input`)
- Principle IV: non-regression vs baseline (not fabricated 1s bound)

No further constitution amendments are required for this feature — the constitution was
pre-amended for this modernization.

## R11 — Bulk Import Approach

**Decision**: `import_service.extract_papers_from_tex_bib(tex_file, bib_file)` returns an
`Iterator[PaperCreate]`. The CLI calls it and commits each paper individually via the service
(per-paper commit, per Principle IV).

The legacy `get_data.py` parsing logic is ported into `import_service.py` using the same
`pylatexenc` + `pybtex` libraries.

## R12 — Benchmark Harness

**Decision**: `tests/benchmarks/` directory with `bench_baseline.py` using pytest-benchmark
or a simple wall-clock timer, recording results to `tests/benchmarks/baseline.json`.

The constitution (Principle IV gate) requires the benchmark to execute successfully — not be
permanently skipped. The benchmark tests search-by-title, search-by-author, add, update, delete
against a seeded ephemeral DB.

## R13 — Seed Data / Fixture Strategy

**Decision**: A canonical seed dataset at `tests/fixtures/seed_papers.py` (or `.json`) seeded by
a pytest session fixture. All integration tests reference this dataset explicitly.

## R14 — Entry Point

**Decision**: `pdbsearch` console script (pyproject.toml `[project.scripts]`), pointing to
`paper_sorts.cli.app:main`. When invoked with no subcommand it drops into the four-option
top-level interactive menu (`search`, `add`, `update`, `(Q)uit`). When invoked with a subcommand
it runs that subcommand directly (delete, import, migrate are subcommand-only).
