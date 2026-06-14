# Research: Modernize the Stack

**Feature**: 001-modernize-stack | **Date**: 2026-06-15

## R1 — ORM Choice: SQLAlchemy 2.x

**Decision**: SQLAlchemy 2.x (declarative ORM with `mapped_column` / `Mapped` type annotations)

**Rationale**: SQLAlchemy 2.x is the de-facto Python ORM; it supports psycopg v3 natively via its `psycopg` dialect, offers full async and sync modes, and provides typed mapped columns for cleaner type-hint integration. The `with Session(engine) as session:` pattern satisfies the constitution's deterministic session-close requirement without any extra glue.

**Alternatives considered**:
- Tortoise ORM — async only, adds async complexity the constitution explicitly forbids
- Peewee — simpler API but far smaller ecosystem; psycopg v3 support is less mature
- raw psycopg v3 — loses ORM benefits; would keep bespoke SQL strings

## R2 — CLI Framework: Typer

**Decision**: Typer (built on top of Click)

**Rationale**: Typer is the mainstream choice for Python CLI tools in 2026 — it infers argument types from Python type hints, produces auto-generated `--help`, integrates with rich for styled output, and supports both subcommands and a fallback interactive menu (via `typer.main.get_command()` called programmatically). The `CliRunner` from Click (used by Typer) enables hermetic CLI testing without spawning subprocesses.

**Alternatives considered**:
- argparse — stdlib but requires manual help text, no rich integration, no subcommand introspection
- Click alone — Typer wraps Click; using Typer directly reduces boilerplate
- Cement — heavyweight framework with DI container; unnecessary for a personal CLI tool

## R3 — Settings Library: pydantic-settings v2

**Decision**: pydantic-settings v2 (`BaseSettings`)

**Rationale**: Pydantic-settings v2 directly supports the four-source priority chain required by the spec (CLI args > env vars > .env file > custom source). A custom `FernetConfigSource` can be implemented as a `PydanticBaseSettingsSource` that decrypts the legacy INI file, slotting into the priority chain without touching the other sources. Env var prefix (`PDBSEARCH_*`) is trivially configured.

**Alternatives considered**:
- Dynaconf — heavy; more suited to multi-environment application config
- python-decouple — no priority chain ordering; Fernet source is non-trivial to plug in
- environ-config — attrs-based; less idiomatic with SQLAlchemy/Typer stack

## R4 — Migration Tool: Alembic

**Decision**: Alembic (SQLAlchemy's official migration tool)

**Rationale**: Alembic is the canonical companion to SQLAlchemy. Revision 001 ports the verbatim DDL (bibtex_id column). Revision 002 handles the `bibtext_id` typo variant present in the legacy `add.py`/`search.py`/`get_data.py` modules. Alembic's `op.execute()` allows raw SQL for the column rename where the ORM abstraction is insufficient.

**Alternatives considered**:
- Flyway — JVM; wrong language
- Yoyo — simpler but less SQLAlchemy integration; no auto-generation
- Manual SQL scripts — FR-005 requires a mainstream migration tool, not manual scripts

## R5 — Test Framework: pytest + pytest-postgresql

**Decision**: pytest with pytest-postgresql plugin; host `pg_ctl` at `/usr/bin/pg_ctl`

**Rationale**: pytest-postgresql spins up a real ephemeral PostgreSQL instance per test session using the host `pg_ctl`, satisfying the constitution's "no mocking the SQLAlchemy session or psycopg" requirement. Each test that needs the DB receives an `ephemeral_db_url` fixture and applies Alembic migrations before seeding. pytest's fixture system integrates cleanly.

**Alternatives considered**:
- pytest-docker-postgresql — requires Docker daemon; not available in all dev environments
- SQLite in-memory — different SQL dialect; masks PostgreSQL-specific behaviour
- testcontainers — Docker-dependent

## R6 — Linter: ruff

**Decision**: ruff (already mandated by constitution v1.3.0-b2-hardened)

**Rationale**: ruff replaces pylint (as mandated by FR-016 and the constitution amendment). It is 10–100x faster, implements the same rule set, and is configurable via `pyproject.toml`. Default select includes E, F, I (imports). `ruff format` replaces black.

**Alternatives considered**:
- pylint — replaced per FR-016; still installed as legacy dep, will be removed
- flake8 — unmaintained relative to ruff; no auto-fix

## R7 — BibTeX Parser: pybtex (unchanged)

**Decision**: Keep pybtex (already in use)

**Rationale**: pybtex is the only actively maintained pure-Python BibTeX parser. The import service wraps it; switching would not reduce complexity. The spec explicitly permits keeping it.

## R8 — Build Backend & Package Manager: uv + hatchling

**Decision**: uv (package manager) + hatchling (build backend); PEP 621 `[project]` metadata

**Rationale**: The constitution v1.3.0 mandates uv. Poetry's pyproject.toml format (`[tool.poetry]`) is incompatible with PEP 621; migration to `[project]` metadata with `hatchling` as build backend is the standard path. `uv sync --all-extras` installs all runtime + dev deps. The entry point is declared as `pdbsearch = "paper_sorts.cli.app:entry_point"`.

**Alternatives considered**:
- setuptools — works but hatchling is more modern and has better uv integration
- flit — simpler but less flexible for scripts entry points

## R9 — Coverage Gate: per-layer 80% (constitution Principle II)

**Decision**: `pytest-cov` with `--cov=src/paper_sorts` and per-module coverage reports

**Rationale**: The constitution requires per-layer 80% coverage independently (db/, services/, cli/, config.py). `pytest-cov` with `--cov-report=term-missing` and explicit `--cov=src/paper_sorts/<layer>` invocations satisfies this. The CLI layer can be covered via Typer's `CliRunner`.

## R10 — Constitution Amendments (FR-016)

The constitution was pre-amended to v1.3.0-b2-hardened covering all required changes:
- Principle I: pylint → ruff; driver isolation to `db/` layer
- Principle II: unittest → pytest; pytest-postgresql; per-layer coverage gate; no mocking SA session
- Principle III: prompt routing → `paper_sorts.cli.prompts`
- Principle IV: baseline-benchmark gate (executing, not skipped)
- Stack & Constraints: Python ≥ 3.11; uv; psycopg v3; pydantic-settings

## R11 — Legacy Schema Variants

Two column name variants exist:
- `bibtex_id` — used by the OO stack (`DatabaseConnector`, `PsycopgDB`)
- `bibtext_id` (sic — typo) — used by procedural modules (`add.py`, `get_data.py`, `search.py`)

Alembic revision 002 must detect which variant is present and perform the rename only when `bibtext_id` exists (idempotency requirement).

## R12 — Rollback Semantics

The legacy `DatabaseConnector.add_entry_to_db` manually rolls back on partial failure using SQL DELETE statements. SQLAlchemy's `Session` context manager automatically rolls back on exception, which is cleaner and more reliable. The `with_session()` helper in `db/session.py` wraps this pattern.

## R13 — Benchmark Harness Design

The constitution's Principle IV baseline-benchmark gate requires:
1. A harness at `tests/benchmarks/bench_baseline.py` that actually executes (not permanently skipped)
2. Measures wall-clock time for: search_by_title, search_by_author, add_paper, update_field, delete_paper
3. Records results to `tests/benchmarks/baseline.json`

The harness will use `timeit` against the seeded ephemeral DB; the benchmark is run separately from the main test suite via `pytest tests/benchmarks/` or a dedicated make target. It MUST execute successfully when invoked.
