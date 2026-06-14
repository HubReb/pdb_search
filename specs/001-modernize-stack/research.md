# Research: Modernize the Stack

**Feature**: 001-modernize-stack | **Date**: 2026-06-14

## R1 — ORM Choice: SQLAlchemy 2.x

**Decision**: SQLAlchemy 2.x (Core + ORM, declarative models)
**Rationale**: The constitution (v1.3.0) explicitly names SQLAlchemy as the ORM. It is the dominant Python ORM in 2026, supports psycopg v3 as a dialect, provides parameterised queries natively, handles joins over the four-table schema naturally, and its `Session` context-manager (`with Session(engine) as session`) satisfies the constitution's deterministic-close requirement.
**Alternatives considered**: Tortoise-ORM (async only — excluded by constitution); Peewee (smaller ecosystem, no async option needed anyway, but SQLAlchemy is the more recognisable choice); raw psycopg (removed the ORM benefit; spec FR-004 requires a mainstream ORM).

## R2 — CLI Framework: Typer

**Decision**: Typer (built on Click)
**Rationale**: FR-006 requires a mainstream CLI framework replacing argparse. Typer is type-hint-first, widely adopted in 2026, produces subcommand CLIs trivially, and integrates with rich for coloured output. Its `CliRunner` (from Click's testing utilities) enables unit-testable CLI paths — satisfying Principle II's per-layer coverage gate for cli/.
**Alternatives considered**: Click (lower-level; Typer is Click with type annotation sugar — strictly better ergonomics for this use case); argparse (legacy, already present — the modernization purpose is to replace it).

## R3 — Settings: pydantic-settings v2

**Decision**: pydantic-settings v2 (`BaseSettings`)
**Rationale**: FR-007 and constitution Stack & Constraints name pydantic-settings. It provides env-var + `.env` + custom-source support in a single class. We add a custom `FernetIniSettingsSource` that decrypts the legacy config file on demand, keeping backward compatibility with existing `../../database.crypt` + `../../key` pairs.
**Alternatives considered**: dynaconf (heavier, not named in constitution); python-dotenv alone (no type coercion or env-var layering).

## R4 — Migration Tool: Alembic

**Decision**: Alembic (driven by SQLAlchemy metadata)
**Rationale**: FR-005 requires versioned, reversible migrations. Alembic is the canonical migration tool for SQLAlchemy projects, widely used, and supports both `upgrade` and `downgrade`. Revision 001 is a verbatim port of `DatabaseConnector.create_tables()` DDL; revision 002+ handle the `bibtext_id` → `bibtex_id` column fix for legacy databases.
**Alternatives considered**: Flyway (Java-centric); Yoyo (less ecosystem support); hand-rolled scripts (not mainstream).

## R5 — Linting/Formatting: ruff

**Decision**: ruff (lint + format)
**Rationale**: FR-010 and constitution Principle I name ruff. It is the dominant Python linter/formatter in 2026, replaces pylint + black + isort in a single tool, and runs substantially faster. `ruff check` enforces code quality; `ruff format --check` enforces formatting.
**Alternatives considered**: pylint (legacy — present in current stack; the spec explicitly replaces it); black + flake8 (ruff supersedes both).

## R6 — Test Framework: pytest + pytest-postgresql

**Decision**: pytest with pytest-postgresql plugin
**Rationale**: FR-008, FR-009, and constitution Principle II name pytest and pytest-postgresql. pytest-postgresql spins up an ephemeral PostgreSQL process from the host `pg_ctl` binary per test session — no Docker dependency, no manual DB setup. The `postgresql_proc` and `postgresql` fixtures are composed into an `ephemeral_db_url` fixture that builds the SQLAlchemy engine for integration tests.
**Alternatives considered**: testcontainers (Docker dependency — heavyweight for an offline CLI tool); in-memory SQLite (different SQL dialect; constitution forbids mocking the persistence layer).

## R7 — Build Backend: hatchling + uv

**Decision**: hatchling build backend; uv for dependency management
**Rationale**: Constitution Stack & Constraints names uv and hatchling. PEP 621 `[project]` metadata with `uv.lock` for reproducibility. `[project.scripts] pdbsearch = "paper_sorts.cli.app:main"` provides the CLI entry point without a separate wrapper script.
**Alternatives considered**: Poetry (legacy — present in current stack; the spec replaces it); setuptools (still works but hatchling is lighter and more PEP-621-native).

## R8 — BibTeX Parsing: pybtex

**Decision**: Retain pybtex
**Rationale**: pybtex is already a dependency, handles LaTeX accents/escapes correctly, and the spec assumption states "BibTeX parsing remains the responsibility of a dedicated library." No functional reason to switch. pylatexenc is retained for `.tex` file parsing in the import service.
**Alternatives considered**: bibtexparser (alternative; no strong advantage; switching cost for zero gain).

## R9 — Type Checking: mypy (strict on src/)

**Decision**: mypy with `strict = true` on `src/`
**Rationale**: Constitution Principle I requires full type hints. Strict mypy catches missing annotations, Any propagation, and return-type errors. Test files use a more lenient profile (no strict on `tests/`).
**Alternatives considered**: pyright (equally capable; mypy is more established in CI integration for this project type).

## R10 — Constitution Amendment

**Decision**: No amendment required. Constitution was pre-amended to v1.3.0 which already names ruff, pytest, pytest-postgresql, SQLAlchemy, pydantic-settings, and uv.
**Rationale**: The SYNC IMPACT REPORT in constitution.md records the 1.1.0 → 1.3.0 bump that aligned all five testable predicates with the modernization targets. The five amendment groups are: (I) pylint→ruff, layer-level isolation; (II) unittest→pytest, pytest-postgresql named; (III) prompt routing moved to cli/prompts; (IV) function-level→layer-level performance references; (Stack) Poetry→uv, psycopg2→psycopg v3, Python ≥3.11.
**Alternatives considered**: Amendment via `/speckit-constitution` — not needed since v1.3.0 is already in place.

## R11 — Ephemeral DB Fixture Design

**Decision**: `postgresql_proc` fixture (scope=session) → `ephemeral_db_url` fixture → `engine` → `Session` passed to repos. Alembic `upgrade head` run once per session on the ephemeral DB.
**Rationale**: Running migrations on the test DB ensures the migration scripts themselves are tested (T031 requirement). Seeding via `SEED_PAPERS` constant in `tests/fixtures/seed_papers.py` satisfies constitution Principle II's "co-located seed data" requirement.
**Alternatives considered**: Factory fixtures per test (slower); shared global state (flaky).

## R12 — Legacy Column-Name Handling

**Decision**: Alembic migration 002 detects `bibtext_id` (sic) vs `bibtex_id` at runtime and renames if necessary, making upgrade idempotent.
**Rationale**: FR-011 requires handling both historical schemas. The legacy `add.py`/`search.py`/`get_data.py` modules used `bibtext_id` (typo); the OO stack used `bibtex_id`. A conditional migration (inspect columns, rename if old name present, skip if already correct) satisfies the idempotency requirement.
**Alternatives considered**: Detect-and-fail with user instructions (bad UX); two separate upgrade paths (complex; Alembic's `op.get_bind().execute(...)` plus Inspector is simpler).

## R13 — Logging Design

**Decision**: Single `logging_config.py` calling `logging.config.dictConfig` once at startup. RichHandler to stdout (INFO+). Optional FileHandler configured via `PDBSEARCH_LOG_FILE` env var. No per-class log files.
**Rationale**: FR-013 requires a mainstream logging approach. Per-class log files (`db_connector.log`, `interaction.log`) are a legacy artifact — the spec explicitly allows their removal. A single dictConfig call avoids handler duplication and integrates cleanly with pytest's log-capturing.
**Alternatives considered**: structlog (adds dependency; overkill for a personal CLI); per-class FileHandlers (explicitly removed by FR-013).
