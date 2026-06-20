# Research: Modernize the Stack

## R1 — ORM Choice

**Decision**: SQLAlchemy 2.x with psycopg v3 (binary wheel)  
**Rationale**: SQLAlchemy is the dominant Python ORM, actively maintained, and specifically supports psycopg v3 as its recommended async/sync driver for PostgreSQL. The 2.x API (`mapped_column`, `DeclarativeBase`, typed ORM) integrates naturally with mypy. psycopg v3 is the successor to psycopg2, already referenced in constitution v1.3.0.  
**Alternatives considered**: SQLModel (wraps SQLAlchemy, less stable), Tortoise ORM (async-only, out of scope), raw psycopg (loses ORM query composition).

## R2 — Migration Tool

**Decision**: Alembic (SQLAlchemy's official migration tool)  
**Rationale**: Native integration with SQLAlchemy models; versioned, reversible migrations; `alembic upgrade head` is idempotent when run repeatedly; widely adopted in Python projects.  
**Alternatives considered**: yoyo-migrations (no SQLAlchemy integration), Flyway (JVM-based).

## R3 — CLI Framework

**Decision**: Typer  
**Rationale**: Typer is built on Click, generates help text automatically from type hints, and supports both subcommand dispatch and an interactive fallback (invoking the app with no args drops to a custom menu). Widely recognised; no bespoke glue required for argument parsing.  
**Alternatives considered**: Click (lower-level, more boilerplate), argparse (already in use — too bespoke), rich-click (just styling, not a replacement).

## R4 — Settings Library

**Decision**: pydantic-settings v2  
**Rationale**: Supports environment variables (`PDBSEARCH_*`), `.env` files, and custom sources (used to implement the Fernet-encrypted INI source). Type-safe, validated at startup. Already referenced in constitution v1.3.0.  
**Alternatives considered**: dynaconf (no pydantic integration), python-decouple (no custom sources).

## R5 — Linter

**Decision**: ruff  
**Rationale**: ruff replaces pylint per FR-010 and is already named in constitution v1.3.0. Faster than pylint, covers flake8 + isort + pyupgrade rules.  
**Alternatives considered**: pylint (replaced per constitution amendment), flake8 + isort (ruff subsumes both).

## R6 — Test Framework

**Decision**: pytest + pytest-postgresql  
**Rationale**: pytest is the modern standard; pytest-postgresql spins an ephemeral PostgreSQL cluster per test session using the host `pg_ctl` at `/usr/bin/pg_ctl`. No Docker needed, no shared state. Already required by constitution v1.3.0.  
**Alternatives considered**: unittest (being replaced per FR-009), testcontainers (Docker dependency).

## R7 — Schema: Two Alembic Revisions

**Decision**: Revision 001 = verbatim port of legacy `create_tables()` DDL; Revision 002 = converge legacy variants.  
**Rationale**: The spec calls for idempotent migration that handles both historical schema variants (`bibtex_id` column in `database_connector.py` and the `bibtext_id` typo column in legacy `add.py`/`get_data.py`/`search.py`). Two revisions keeps the audit trail clean.

## R8 — Prompt Routing

**Decision**: All prompts routed through `src/paper_sorts/cli/prompts.py`. No bare `input()` outside this module.  
**Rationale**: Constitution Principle III explicitly names `paper_sorts.cli.prompts` as the single routing point. This enables future testability of prompts (mock the module, not scattered input() calls).

## R9 — Build Backend

**Decision**: hatchling + uv  
**Rationale**: Constitution v1.3.0 Stack & Constraints: "uv (PEP 621 metadata, uv.lock for reproducibility, hatchling build backend)". hatchling is the default backend for projects scaffolded by uv/hatch and handles src-layout natively.

## R10 — Constitution Amendment Status

**Decision**: All required amendments already incorporated in constitution v1.3.0 (ratified 2026-04-26, amended 2026-04-27).  
**Summary of amendments**:
- Principle I: driver-isolation rule → layer-level (db/ only); pylint → ruff  
- Principle II: unittest → pytest; pytest-postgresql named; "no mock session/repo"  
- Principle III: prompt routing → `paper_sorts.cli.prompts`  
- Principle IV: absolute latency bounds removed; non-regression vs. baseline  
- Stack & Constraints: Python ^3.10 → ≥3.11; Poetry → uv; psycopg2 → psycopg v3  

No further amendments required for implementation.

## R11 — Legacy Schema (Reference)

The four tables created by `DatabaseConnector.create_tables()`:

```sql
CREATE TABLE IF NOT EXISTS authors_papers (
  id SERIAL PRIMARY KEY,
  author_id INT,
  paper_id INT
);

CREATE TABLE IF NOT EXISTS authors_id (
  id SERIAL PRIMARY KEY,
  author TEXT
);

CREATE TABLE IF NOT EXISTS bib (
  bibtex_id TEXT PRIMARY KEY,
  bibtex TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS papers (
  id SERIAL PRIMARY KEY,
  title TEXT,
  contents TEXT,
  bibtex_id TEXT,
  CONSTRAINT fk_bibtex_id FOREIGN KEY(bibtex_id) REFERENCES bib(bibtex_id)
);
```

Legacy `add.py`/`get_data.py`/`search.py` used `bibtext_id` (typo, missing 'a') — Revision 002 handles this.

## R12 — BibTeX Parsing

**Decision**: Continue using pybtex for BibTeX parsing.  
**Rationale**: pybtex is already a dependency; handles round-tripping LaTeX escapes, which the spec explicitly requires. Switching library is unnecessary risk.

## R13 — Dependency Injection Pattern

**Decision**: Services receive the `with_session` factory as a parameter (default to the production `with_session` from `db.session`).  
**Rationale**: Makes tests straightforward — pass the ephemeral DB's session factory. No global state. No mock required — the ephemeral DB is a real PostgreSQL instance.
