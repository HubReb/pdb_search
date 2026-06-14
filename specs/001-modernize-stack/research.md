# Research: Modernize the Stack

**Feature**: 001-modernize-stack  
**Date**: 2026-04-26  
**Status**: Complete

---

## R1: ORM / Database Toolkit

**Decision**: SQLAlchemy 2.x with declarative ORM models  
**Rationale**: SQLAlchemy 2.x is the dominant Python ORM in 2026, actively maintained, and mandated by the constitution Stack & Constraints section. Its declarative model definition, session context manager (`with Session(...)`), and type-safe query API (via `select()`) directly address the spec requirement for parameterised queries, joins, and transactions. The `with_session` wrapper pattern (commit on success, rollback on exception) replaces the scattered `con.commit()` / `con.rollback()` calls in `psycopg_db.py`.  
**Alternatives considered**:
- `peewee`: Simpler API, but less mainstream in enterprise/larger Python projects; no compelling advantage over SQLAlchemy for this schema size.
- Raw `psycopg` v3 with a thin wrapper: Would satisfy parameterised queries but not the "mainstream ORM" requirement in FR-004.

---

## R2: CLI Framework

**Decision**: Typer (built on Click)  
**Rationale**: Typer is the mainstream Python CLI framework choice for 2026 Python projects, especially where subcommands and type-annotated argument parsing are wanted. It maps cleanly to the existing top-level menu (search / add / update / delete / import / migrate) as subcommands. Its `CliRunner` enables unit-testable CLI paths without mocking stdin. The constitution Principle III already references `paper_sorts.cli.prompts` as the single prompt-routing module, which Typer supports cleanly.  
**Alternatives considered**:
- Click directly: Typer is a thin type-annotated wrapper over Click — using Click would require more boilerplate.
- `argparse`: Legacy approach, already in use — the spec explicitly requires replacing it (FR-006).
- `rich-click`: Adds Rich formatting to Click but no structural advantage over Typer here.

---

## R3: Configuration / Settings

**Decision**: pydantic-settings v2 with a custom `FernetConfigSource`  
**Rationale**: pydantic-settings v2 provides the four-source priority chain mandated by the constitution (CLI args > env vars > `.env` > Fernet-encrypted INI). A custom `PydanticBaseSettingsSource` subclass reads the encrypted INI file and feeds its values as the lowest-priority source. Environment variables use the `PDBSEARCH_` prefix. This keeps the encrypted-config workflow while adding plain `.env` and env-var support (FR-007).  
**Alternatives considered**:
- `dynaconf`: More complex, YAML/TOML sources not needed here.
- Plain `configparser` + custom priority chain: Would not satisfy the "mainstream settings library" requirement.

---

## R4: Test Runner & Ephemeral DB

**Decision**: pytest + pytest-postgresql  
**Rationale**: pytest is mandated by constitution Principle II. `pytest-postgresql` spins up an ephemeral PostgreSQL process per test session from the host's `pg_ctl` binary (at `/usr/bin/pg_ctl`), fulfilling FR-008 (no developer-local DB required). Session-scoped fixtures seed the schema via Alembic `upgrade("head")` against the ephemeral URL, then seed rows from a fixture file. Integration tests for the persistence layer run against real SQL; no mocking of the SQLAlchemy session is permitted.  
**Alternatives considered**:
- `pytest-docker`: Would need Docker, not guaranteed on all dev machines.
- `sqlalchemy` in-memory SQLite: Would not test PostgreSQL-specific behaviour (serial sequences, FK syntax).

---

## R5: Linting and Formatting

**Decision**: ruff (check + format)  
**Rationale**: ruff is mandated by constitution Principle I (amended from pylint at v1.3.0). It is the fastest Python linter in 2026 and handles both linting (`ruff check`) and formatting (`ruff format --check`). The FR-010 requirement to amend the constitution if switching away from pylint has already been satisfied at constitution v1.3.0.  
**Alternatives considered**:
- pylint: Was the legacy tool; constitution already amended.
- flake8 + black: Predecessor combination to ruff; no advantage.

---

## R6: Schema Migration Tool

**Decision**: Alembic with auto-generated migration scripts  
**Rationale**: Alembic is the canonical migration tool for SQLAlchemy projects (FR-005). Versioned, reversible migration files under `migrations/versions/` replace the runtime `create_tables()` call. Revision 001 is the verbatim port of the current DDL (the canonical `bibtex_id` schema). Additional revisions handle the legacy `bibtext_id` (sic) typo column used by `add.py`/`get_data.py`, ensuring US4 (one-shot migration of existing personal databases) is satisfied.  
**Alternatives considered**:
- Flyway: JVM dependency, not appropriate for a pure Python project.
- Manual SQL scripts: Not versioned or reversible.

---

## R7: Build System / Package Manager

**Decision**: uv with PEP 621 `pyproject.toml` (hatchling build backend)  
**Rationale**: The constitution Stack & Constraints section mandates uv. The project switches from `poetry` (in the legacy `pyproject.toml`) to a PEP 621 `[project]` section managed by uv. `uv.lock` ensures reproducible installs. The entry point is declared as `pdbsearch = "paper_sorts.cli.app:app"`.  
**Alternatives considered**:
- Keep poetry: Constitution explicitly mandates uv.

---

## R8: Src-Layout Package

**Decision**: `src/paper_sorts/` (src-layout)  
**Rationale**: The src-layout prevents accidental imports of the un-installed package. The structure mirrors the layered architecture required by FR-014: `cli/`, `services/`, `db/`, `config.py`, `logging_config.py`.  
**Alternatives considered**:
- Keep flat `paper_sorts/` layout: Would not reflect the layered architecture, and legacy procedural modules must be removed (FR-012) anyway.

---

## R9: BibTeX Parsing

**Decision**: Keep `pybtex` (same as legacy)  
**Rationale**: pybtex handles LaTeX accents/escapes in BibTeX entries (edge case in spec) and already works correctly in the legacy codebase. The import service will wrap it in a pure Python function `extract_papers_from_tex_bib(tex: str, bib: str) -> Iterator[PaperCreate]` with no I/O or UI concerns.  
**Alternatives considered**:
- `bibtexparser`: Alternative library; no functional advantage for this use case.

---

## R10: Constitution Amendments Required (FR-016)

The following constitution amendments were applied at v1.3.0 (already ratified):

1. **Principle I** — driver-isolation rule rewritten from `psycopg2` → layer-level (`src/paper_sorts/db/` only imports SQLAlchemy/driver); `pylint` → `ruff`.
2. **Principle II** — `unittest` → `pytest`; `pytest-postgresql` named; "no mocking persistence layer" rephrased to reference SQLAlchemy session.
3. **Principle III** — prompt-routing reference moved from `helpers.get_user_input` → `paper_sorts.cli.prompts`.
4. **Stack & Constraints** — Python ^3.10 → ≥ 3.11; Poetry → uv; psycopg2 → psycopg v3; `ConfigReader` → `paper_sorts.config`.
5. **Performance** — absolute latency numbers replaced with non-regression-vs-baseline criterion.

All amendments are visible in `.specify/memory/constitution.md` SYNC IMPACT REPORT section.

---

## R11: Schema Variant Handling (Edge Case)

The legacy codebase has **two column naming variants**:
- `bibtex_id` (canonical — used by `database_connector.py`, `psycopg_db.py`)
- `bibtext_id` (sic, typo — used by `add.py` and `get_data.py`)

**Decision**: Alembic Revision 001 establishes the canonical `bibtex_id` schema. Revision 002 (or a conditional migration) detects and renames `bibtext_id` → `bibtex_id` if the typo column is present (idempotent with `IF EXISTS`). The `migrate` subcommand runs both, satisfying US4 SC-004.

---

## R12: Logging

**Decision**: Single `logging.config.dictConfig` call in `cli/app.py`; RichHandler to stdout (INFO+), optional FileHandler via `--log-level` flag  
**Rationale**: FR-013 requires a mainstream logging approach; per-class log files (legacy) are not required to be preserved verbatim. The constitution Principle III requires failure paths to log via configured stdlib logger and surface plain-language messages to user.

---

## R13: Benchmark / Performance Baseline

**Decision**: `tests/benchmarks/bench_baseline.py` using `pytest-benchmark` or simple `time.perf_counter` harness, seeded with `SEED_PAPERS` fixture, records search/add/update/delete wall-clock times to `tests/benchmarks/baseline.json`.  
**Rationale**: Constitution Principle IV (v1.3.0-b2-hardened) adds Gate G2 requiring a baseline benchmark that MUST execute (not be permanently skipped). The harness records baseline on first run and asserts no regression > 2× on subsequent runs.

---

## R14: Per-Layer Coverage Gate

**Decision**: `pytest --cov=src/paper_sorts --cov-report=term-missing` with `pytest-cov`; per-layer coverage checks enforced in `tests/test_coverage_gate.py` or via `pyproject.toml` `[tool.coverage.report]` fail-under settings.  
**Rationale**: Constitution Principle II (v1.3.0-b2-hardened) Gate G1 requires each of the four layers to independently reach ≥ 80% line coverage.

---

## R15: Doc-Currency Gate

**Decision**: `tests/test_doc_currency.py` performs a case-sensitive search of `README.md` and `CLAUDE.md` for the four forbidden tokens: `Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB`. Any match is a test failure.  
**Rationale**: Constitution Principle I (v1.3.0-b2-hardened) Gate G3 makes this a merge-blocking mechanical check.
