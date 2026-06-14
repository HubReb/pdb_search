# Research: Modernize the Stack

**Feature**: 001-modernize-stack  
**Date**: 2026-06-15  
**Status**: Final (all NEEDS CLARIFICATION resolved)

---

## R01 — CLI Framework Choice

**Decision**: Typer  
**Rationale**: Typer builds on Click (stable, widely used) and adds first-class Python type-hint integration, automatic `--help` generation, and subcommand wiring that maps 1-to-1 with the existing menu options (search, add, update, delete, import). Rich integration is built-in for better terminal output. Zero config discovery of subcommands.  
**Alternatives considered**:
- `Click` directly — more boilerplate; Typer is a superset.
- `argparse` — already in use and is what we're replacing; less ergonomic for subcommands.
- `docopt` — string-based, not type-hint-friendly, lower adoption in 2026.

---

## R02 — ORM / Database Toolkit Choice

**Decision**: SQLAlchemy 2.x (Core + ORM) with `psycopg` v3 driver  
**Rationale**: Constitution Stack & Constraints mandate SQLAlchemy 2.x on psycopg v3. SQLAlchemy 2.x uses parameterised queries exclusively, supports joins, and offers `Session` context managers for deterministic connection lifecycle. psycopg v3 is already listed as a dependency in the current pyproject.toml.  
**Alternatives considered**: Direct psycopg v3 SQL strings (stays bespoke); asyncpg (async, constitution prohibits async drivers); tortoise-orm (async-only).

---

## R03 — Migration Tool Choice

**Decision**: Alembic  
**Rationale**: Official SQLAlchemy migration partner; supports up/down migrations; autogenerate from ORM models; idempotent `alembic upgrade head`. Revision 001 ports the current DDL verbatim; future revisions converge legacy schema variants (bibtext_id typo).  
**Alternatives considered**: Flyway (JVM), Liquibase (JVM) — not Python-native. Yoyo-migrations — less adoption, no SQLAlchemy integration.

---

## R04 — Settings / Config Library

**Decision**: pydantic-settings v2  
**Rationale**: Constitution names `paper_sorts.config` (pydantic-settings v2) explicitly. Supports four priority sources: CLI flags > env vars (`PDBSEARCH_*`) > `.env` file > Fernet-encrypted INI (custom `BaseSettings` source). All credential fields use `SecretStr` so they never appear in logs or repr.  
**Alternatives considered**: `dynaconf` (heavier), `python-decouple` (no custom source protocol), bare `os.environ` (no validation).

---

## R05 — Test Runner and Ephemeral DB

**Decision**: pytest + pytest-postgresql  
**Rationale**: Constitution Principle II mandates pytest and pytest-postgresql. `pytest-postgresql` provisions an ephemeral PostgreSQL cluster per test session using the host's `pg_ctl` binary (at `/usr/bin/pg_ctl`). Fixtures in `tests/conftest.py` expose `postgresql_proc` and an `ephemeral_db_url` URL string that Alembic and SQLAlchemy can consume.  
**Alternatives considered**: testcontainers (Docker required, not guaranteed in all CI); pytest-docker (same); in-memory SQLite (not PostgreSQL, misses schema quirks).

---

## R06 — Linting Tool

**Decision**: ruff  
**Rationale**: Constitution Principle I mandates ruff. Replaces pylint. Ruff covers flake8, isort, and basic pyflakes checks in one tool; order-of-magnitude faster. Config in `pyproject.toml` under `[tool.ruff]`.  
**Alternatives considered**: pylint (being replaced per constitution), flake8 (subset of ruff), black (formatting only).

---

## R07 — BibTeX Parsing

**Decision**: pybtex (retained)  
**Rationale**: Already a dependency; spec assumption explicitly allows retaining it. Handles LaTeX accent round-trips correctly. No functional gap versus alternatives.  
**Alternatives considered**: bibtexparser v2 (not yet stable enough in 2026 ecosystem); hand-rolled (violates "mainstream library" principle).

---

## R08 — Package Management

**Decision**: uv with PEP 621 `pyproject.toml` and `hatchling` build backend  
**Rationale**: Constitution Stack & Constraints mandates uv + hatchling. Replaces Poetry. `uv sync --all-extras` installs all runtime and dev dependencies reproducibly via `uv.lock`.  
**Alternatives considered**: Poetry (being replaced), pip + requirements.txt (no lock file, no build backend).

---

## R09 — Logging

**Decision**: stdlib `logging` with `logging.config.dictConfig`, single call from `cli/app.py` at startup; RichHandler to stdout, optional FileHandler  
**Rationale**: Single logging configuration at process startup avoids per-class log files. Constitution Principle III requires failure paths to log AND show a plain-language message; stdlib logging satisfies this cleanly without added dependencies.  
**Alternatives considered**: `loguru` (extra dependency, non-standard); per-class file logging (the current approach, being replaced per spec assumption).

---

## R10 — Constitution Amendment

**Decision**: Amend constitution from v1.0.0 to v1.3.0 as part of this feature (already ratified in `.specify/memory/constitution.md`)  
**Rationale**: FR-016 requires amendments for: psycopg2 isolation → SQLAlchemy/db/ isolation; pylint → ruff; unittest → pytest; `helpers.get_user_input` → `paper_sorts.cli.prompts`. Stack & Constraints section must reflect uv/hatchling, psycopg v3, Python ≥ 3.11.  
**Note**: Constitution at `v1.3.0-b2-hardened` in this experiment worktree already captures all these amendments plus three additional mechanical gates (G1 per-layer coverage, G2 baseline benchmark, G3 doc-currency).

---

## R11 — Architecture (Current / Pre-Modernization)

The legacy flat-layout `paper_sorts/` has these modules:

| Module | Role |
|--------|------|
| `run.py` | Entry point; argparse; wires `ConfigReader` + `DatabaseConnector` + `UserInteraction` |
| `user_interaction.py` | `UserInteraction` class — all CLI prompts and menus (bare `input()`) |
| `database_connector.py` | `DatabaseConnector` — domain orchestration; no SQL |
| `psycopg_db.py` | `PsycopgDB` — raw psycopg2 SQL strings; connection open/close per call |
| `config_reader.py` | `ConfigReader(ConfigParser)` — reads Fernet-encrypted INI |
| `helpers.py` | Shared helpers: `get_user_input`, `get_user_choice`, `pretty_print_results`, `cast`, `create_logger`, `get_data` (tex parse) |
| `add.py` | `add_entry_to_db` — standalone psycopg add path (uses `bibtext_id` typo column) |
| `search.py` | `search_by_title`, `search_by_author` — standalone psycopg search path |
| `get_data.py` | `get_data`, `get_bibtex_information`, `load_data_into_db` — bulk import from .tex/.bib |

**Schema** (four tables, no DDL foreign keys on `authors_papers`):
```sql
papers(id SERIAL PK, title TEXT, contents TEXT, bibtex_id TEXT → bib.bibtex_id)
bib(bibtex_id TEXT PK, bibtex TEXT UNIQUE)
authors_id(id SERIAL PK, author TEXT)
authors_papers(id SERIAL PK, author_id INT, paper_id INT)  -- no FK constraints
```

**Known schema variant**: legacy `add.py`/`get_data.py` use column `bibtext_id` (typo); `DatabaseConnector` uses `bibtex_id`. Migration must handle both.

**Connection lifecycle**: per-operation `connect()`/`close()` pairs; no pooling; rollback on error. `create_tables()` at startup creates schema.

---

## R12 — Performance Baseline

**Decision**: Record baseline with `pytest-benchmark` in a dedicated `tests/benchmarks/` harness before removing legacy code; carry baseline.json forward.  
**Rationale**: Constitution Principle IV (baseline-benchmark gate G2) mandates an executing benchmark that records results for search-by-title, search-by-author, add, update, delete. The benchmark must NOT be permanently skipped.  
**Implementation**: `tests/benchmarks/bench_baseline.py` using `pytest-benchmark`; mark with `@pytest.mark.bench`; run via `uv run pytest tests/benchmarks/ --benchmark-autosave`.

---

## R13 — Src Layout and Entry Point

**Decision**: `src/paper_sorts/` layout; entry point `pdbsearch` via `[project.scripts]` in `pyproject.toml`  
**Rationale**: Prevents accidental imports of source without install; idiomatic for packages distributed via uv/pip. `pdbsearch` as the script name matches CLAUDE.md documentation.
