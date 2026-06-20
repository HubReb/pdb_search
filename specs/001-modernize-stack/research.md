# Research: Modernize the Stack

**Feature**: 001-modernize-stack  
**Date**: 2026-06-20  
**Status**: Complete

---

## R01 — ORM Choice: SQLAlchemy 2.x

**Decision**: SQLAlchemy 2.x (Core + ORM, declarative models)

**Rationale**: SQLAlchemy is the most widely adopted Python ORM. Its 2.x API uses
typed `Mapped[...]` annotations with `mapped_column(...)`, which satisfies the
constitution's type-hint requirement naturally. The `with Session(...)` context
manager closes connections deterministically as required by Principle IV. It
isolates all SQL to `db/` per Principle I. It supports psycopg v3 via the
`postgresql+psycopg` dialect.

**Alternatives Considered**:
- **Django ORM**: heavyweight, requires Django project structure — out of scope.
- **Tortoise ORM**: async-only, excluded by Principle IV (no async drivers).
- **Peewee**: lighter but less mainstream recognition.

---

## R02 — CLI Framework: Typer

**Decision**: Typer (built on Click) for the subcommand CLI surface

**Rationale**: Typer is the idiomatic modern Python CLI framework; it derives
argument parsers from Python type annotations and is recognizable to any 2026
Python developer. It maps cleanly onto the four user-facing operations
(search / add / update / delete) as subcommands, plus a top-level interactive
menu when invoked with no subcommand. Satisfies FR-006.

**Alternatives Considered**:
- **argparse** (current): bespoke dialog loop, no subcommand typing.
- **Click** directly: Typer wraps Click with better type integration.
- **Textual / Rich TUI**: FR-017 prohibits TUI surfaces beyond the CLI.

---

## R03 — Configuration: pydantic-settings v2

**Decision**: `pydantic-settings` v2 with a custom `FernetSettingsSource`

**Rationale**: pydantic-settings v2 is the standard configuration library for
pydantic-based projects. It supports env vars, `.env` files, and custom sources
out of the box. A custom `FernetSettingsSource` preserves the existing
encrypted-INI workflow while adding `.env` / env-var support. Priority order:
CLI flags > `PDBSEARCH_*` env vars > `.env` file > Fernet-encrypted INI.
Satisfies FR-007.

**Implementation Note**: The `Settings` model uses `PDBSEARCH_` as the env
prefix. `database_url` is the primary connection parameter; `log_level` is a
secondary setting. The Fernet source is tried last and is skipped with a clear
error message when the key file is missing.

---

## R04 — Migration Tool: Alembic

**Decision**: Alembic with SQLAlchemy metadata autogenerate

**Rationale**: Alembic is the canonical migration companion for SQLAlchemy.
Versioned migration scripts in `migrations/versions/` give the idempotency
guaranteed by FR-011. Revision 001 is the verbatim DDL port (preserving the
exact schema contract). A separate revision handles the `bibtext_id` →
`bibtex_id` typo-rename for the legacy variant schema.

---

## R05 — Test Framework: pytest + pytest-postgresql

**Decision**: pytest with pytest-postgresql for ephemeral DB fixtures

**Rationale**: pytest is the standard Python test framework (FR-009). 
`pytest-postgresql` spins up an isolated PostgreSQL instance per test session
using the host's `pg_ctl` at `/usr/bin/pg_ctl`. No personal database required.
A `conftest.py` session fixture creates the schema via Alembic and seeds from
`tests/fixtures/seed_papers.py`. Satisfies FR-008, SC-003, and Principle II.

**No mocking**: persistence-layer tests run against the real DB per Principle II.

---

## R06 — Linter/Formatter: ruff

**Decision**: ruff (check + format)

**Rationale**: ruff is the fastest, most widely adopted Python linter/formatter
in 2026, replacing flake8, isort, and black in a single tool. It is already
referenced in constitution v1.3.0 (Principle I). Satisfies FR-010.

---

## R07 — Schema Analysis: Dual-Column Legacy Variant

**Decision**: Revision 001 codifies the `bibtex_id` variant; Revision 002
handles the `bibtext_id` (sic) typo variant present in `get_data.py` / `add.py`

**Rationale**: The legacy codebase has two column-name variants:
- `get_data.py` / `add.py` / `search.py`: use `bibtext_id` (typo, older)
- `database_connector.py` / `config_reader.py` / `run.py`: use `bibtex_id` (correct, newer)

The canonical modernized schema uses `bibtex_id` (correct). Alembic migration
Revision 002 detects and renames `bibtext_id` → `bibtex_id` if the typo column
exists, making the migration idempotent on both DB variants. Satisfies FR-011,
US4, and SC-004.

---

## R08 — BibTeX Parsing: pybtex retained

**Decision**: Retain pybtex for BibTeX parsing, with pylatexenc for LaTeX text

**Rationale**: The spec permits switching BibTeX libraries if functionally
equivalent. pybtex already handles the required operations (parse file, extract
authors/title, serialize to BibTeX string). Replacing it introduces risk with
no observable benefit. pylatexenc is needed for the LaTeX→text conversion in
the bulk-import TEX parser.

---

## R09 — Package Layout: src-layout with uv

**Decision**: `src/paper_sorts/` src-layout; `pyproject.toml` with hatchling
backend and uv for dependency/venv management.

**Rationale**: The src-layout prevents accidental imports from the repo root,
is the PEP 517/518 best practice, and is required by the constitution's Stack &
Constraints section. uv replaces Poetry as the package manager per constitution
v1.3.0. The legacy flat-layout `paper_sorts/` is removed once its functionality
is covered (FR-012). The entry point is `pdbsearch` (script in pyproject.toml).

**pyproject.toml key fields**:
```toml
[project]
name = "paper-sorts"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "sqlalchemy>=2.0",
  "alembic>=1.13",
  "typer>=0.12",
  "pydantic-settings>=2.0",
  "psycopg[binary]>=3.1",
  "pybtex>=0.24",
  "pylatexenc>=2.10",
  "cryptography>=41",
  "rich>=13",
]

[project.scripts]
pdbsearch = "paper_sorts.cli.app:app"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

---

## R10 — Constitution Amendment (FR-016)

**Decision**: Constitution already amended to v1.3.0 before this plan was drafted.

**Rationale**: The constitution currently at v1.3.0 already reflects all
necessary amendments: ruff replaces pylint (Principle I), pytest replaces
unittest (Principle II), SQLAlchemy session replaces psycopg isolation
(Principle I), `paper_sorts.cli.prompts` replaces `helpers.get_user_input`
(Principle III), performance is non-regression-based (Principle IV). No further
amendments are required.

---

## R11 — Module Architecture

The modernized package under `src/paper_sorts/`:

```
src/paper_sorts/
├── __init__.py
├── config.py                # pydantic-settings Settings model
├── logging_config.py        # dictConfig setup, RichHandler
├── cli/
│   ├── __init__.py
│   ├── app.py               # Typer app wiring + top-level menu
│   ├── search.py            # search subcommand
│   ├── add.py               # add subcommand
│   ├── update.py            # update subcommand
│   ├── delete.py            # delete subcommand
│   ├── migrate.py           # migrate subcommand (admin)
│   ├── importer.py          # import subcommand (admin)
│   └── prompts.py           # ALL user-facing input/output (Principle III gate)
├── services/
│   ├── __init__.py
│   ├── paper_service.py     # domain operations
│   └── import_service.py    # bulk import logic
└── db/
    ├── __init__.py
    ├── models.py            # SQLAlchemy ORM models
    ├── repositories.py      # PaperRepository, AuthorRepository, BibRepository + DTOs
    └── session.py           # with_session(), engine factory
```

---

## R12 — Logging

**Decision**: stdlib `logging.config.dictConfig` with `rich.logging.RichHandler`
for stdout; optional `FileHandler` configurable via `log_level` setting.

**Rationale**: RichHandler provides the modern console output format; FileHandler
is kept for debug sessions. Per-class log files are not preserved verbatim —
a single configurable `log_level` setting controls verbosity. Satisfies FR-013.

---

## R13 — Delete Subcommand

**Decision**: `pdbsearch delete` is a subcommand. The interactive menu includes
it as option 4, with quit as option 5.

The legacy `UserInteraction` does not have an explicit `delete` method in the
top-level `interact` loop — but the spec (FR-002, US2-AC4) requires delete.
The `DatabaseConnector.delete_paper_entry_from_database` method exists. The
modernized CLI exposes it.

---

## R14 — Seed Dataset for Tests

**Decision**: `tests/fixtures/seed_papers.py` exports `SEED_PAPERS` — a list of
`PaperCreate` DTOs covering:
- papers with multiple authors (to test join)
- papers sharing a title (disambiguation test)
- papers with LaTeX accents in BibTeX (round-trip test)

The exact rows from the legacy test suit are included (the "Direct speech-to-
speech translation" paper, the "Large-scale Self-" paper).
