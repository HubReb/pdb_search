# Research: Modernize the Stack

**Feature**: 001-modernize-stack | **Date**: 2026-06-14

## R1 — ORM: SQLAlchemy 2.x

**Decision**: SQLAlchemy 2.x (declarative ORM + Core for complex queries)
**Rationale**: Most widely used Python ORM. SQLAlchemy 2.x has a clean `Session` context-manager API
(`with Session(engine) as session: ...`) that satisfies the constitution's "deterministic close" rule.
Parameterised queries are the default. Mature Alembic integration for migrations.
**Alternatives considered**:
- SQLModel: Built on SQLAlchemy, too new, less battle-tested migration story.
- Tortoise ORM: Async-only, prohibited by constitution.
- Raw psycopg v3: Eliminates ORM abstraction entirely; FR-004 requires a mainstream ORM.

## R2 — Migrations: Alembic

**Decision**: Alembic with autogenerate disabled (manual migration scripts)
**Rationale**: Standard companion to SQLAlchemy. Versioned, reversible migrations satisfy FR-005.
Manual migration scripts (not autogenerate) give us explicit control over the schema-preservation
contract and the legacy `bibtext_id` → `bibtex_id` rename handling.
**Alternatives considered**: Flyway (Java ecosystem, not Python-native), yoyo-migrations (less adopted).

## R3 — CLI Framework: Typer

**Decision**: Typer with subcommands; `app.py` drops into an interactive four-option menu when called with no subcommand
**Rationale**: Typer is the mainstream Python CLI framework built on Click. Clean subcommand syntax
matching FR-006. `CliRunner` in tests satisfies the per-layer 80% CLI coverage gate (constitution G1).
**Alternatives considered**:
- Click directly: More boilerplate. Typer is a thin, ergonomic wrapper.
- argparse: Legacy; not "mainstream framework" per spec context.

## R4 — Configuration: pydantic-settings v2

**Decision**: pydantic-settings v2 `BaseSettings` with four-source priority (CLI args > PDBSEARCH_* env > .env > Fernet-encrypted INI)
**Rationale**: pydantic-settings v2 is the canonical pydantic companion for settings management. Custom
`SettingsSource` for Fernet-encrypted INI preserves the existing workflow. `.env` and env vars are free.
**Alternatives considered**: dynaconf (heavier), python-decouple (no pydantic validation).

## R5 — Linting: ruff

**Decision**: ruff for both check and format
**Rationale**: Already mandated by constitution v1.3.0 (amended from pylint). Extremely fast.
Replaces pylint + black in a single tool.

## R6 — Testing: pytest + pytest-postgresql

**Decision**: pytest with pytest-postgresql for ephemeral DB
**Rationale**: pytest-postgresql spins up a real PostgreSQL instance per session using the host's
`pg_ctl` (at `/usr/bin/pg_ctl`). No Docker dependency. Constitution II explicitly names this tool.
Real DB required — mocking SQLAlchemy session is forbidden.

## R7 — BibTeX Parsing: pybtex

**Decision**: retain pybtex (already a dependency)
**Rationale**: Functionally sufficient; already handles the legacy BibTeX entries. Spec assumption
says switching library is permitted but not required. No benefit from switching.

## R8 — Type Checking: mypy

**Decision**: mypy in strict mode on `src/`
**Rationale**: Constitution Principle I requires full type hints. mypy strict mode enforces this
mechanically at CI time.

## R9 — Packaging: uv + hatchling

**Decision**: Replace Poetry with uv (PEP 621 pyproject.toml, hatchling build backend, uv.lock)
**Rationale**: Already mandated by constitution v1.3.0 Stack & Constraints section. uv is significantly
faster than Poetry. `uv sync --all-extras` is the single install command.

## R10 — Legacy Schema Variants

**Decision**: Migration revision 001 detects `bibtext_id` (typo, legacy) and renames it to `bibtex_id`.
Both tables (bib and papers) must be checked. Migration is idempotent (uses `IF EXISTS`).
**Rationale**: The spec edge cases require both historical schema variants to survive migration.
The legacy `get_data.py` / `add.py` / `search.py` modules use `bibtext_id` (sic);
the `DatabaseConnector` / `PsycopgDB` stack uses `bibtex_id`. A real personal database could be in either state.
**Alembic upgrade path**:
1. Check if `bib.bibtext_id` exists → rename to `bibtex_id`
2. Check if `papers.bibtext_id` exists → rename to `bibtex_id`
3. Apply canonical schema (NOT NULL, unique constraints) only if not already present
4. All steps wrapped in a transaction; failure leaves DB in pre-migration state

## R11 — Architecture Document (US1)

**Decision**: `docs/architecture.md` — single Markdown file covering all six areas from FR-001
**Rationale**: Markdown is universally readable. Contents:
- Purpose and scope
- User journeys (all five flows: search by title, search by author, add, update, delete, bulk import)
- Data model (four tables, column types, FK relationship, known schema variants)
- Control flow (CLI dialog → UserInteraction → DatabaseConnector → PsycopgDB → PostgreSQL)
- Configuration approach (Fernet-encrypted INI + argparse)
- Install/run instructions
- Known limitations and quirks (identical author names, schema dual variants, developer-local DB dependency in tests)

## R12 — Benchmark Harness (Constitution G2)

**Decision**: `tests/benchmarks/bench_baseline.py` with `baseline.json`. Must execute (NOT permanently skipped).
**Rationale**: Constitution v1.3.0-b2-hardened Principle IV G2: "absence of an executing baseline
benchmark… is itself a violation." The harness:
1. On first run with `--record-baseline` flag: times each interactive operation on seeded data, writes baseline.json
2. On normal pytest run: reads baseline.json, times operations, asserts no measurable regression
   (defined as: no operation exceeds 2× baseline wall-clock time — generous bound for CI variance)
The benchmark is NOT hidden behind `@pytest.mark.skip`. It is behind a `pytest.mark.benchmark` mark
so it can be excluded from the default run with `-m "not benchmark"`, but the CI step that validates
the gate runs it explicitly.

## R13 — Doc-Currency Gate (Constitution G3)

**Decision**: `tests/test_doc_currency.py` — reads README.md and CLAUDE.md, asserts none of the
forbidden tokens (`Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB`) appear.
**Rationale**: Constitution v1.3.0-b2-hardened Principle I G3: "The check is a case-sensitive search
for these tokens over those two files; any match is a build-failing defect."
This test runs as part of the default pytest suite — not behind a mark.

## R14 — Per-Layer Coverage Gate (Constitution G1)

**Decision**: pytest-cov configured with per-source-dir reporting. CI asserts each of the four layers
hits ≥ 80% line coverage independently.
**Rationale**: Constitution II G1: "A whole-repository coverage figure that is met while any single
layer sits below 80% does NOT satisfy this principle."
Coverage config in pyproject.toml `[tool.pytest.ini_options]`: `--cov=src/paper_sorts` with
`--cov-report=term-missing`. A `tests/test_coverage_gate.py` reads the coverage data and asserts per-layer
thresholds, OR the coverage minimum is set per-directory using `[tool.coverage.report]` with
`exclude_lines` and `fail_under` applied to each layer separately.

## Summary of Framework Choices

| Concern | Old | New |
|---------|-----|-----|
| ORM | raw psycopg2 SQL strings | SQLAlchemy 2.x |
| Migrations | `create_tables()` at runtime | Alembic versioned migrations |
| CLI | argparse + bespoke dialog loop | Typer subcommands |
| Config | hand-rolled Fernet INI reader | pydantic-settings v2 + custom Fernet source |
| Linting | pylint | ruff |
| Type checking | none | mypy strict |
| Tests | unittest (live DB) | pytest + pytest-postgresql (ephemeral DB) |
| Packaging | Poetry | uv + hatchling |
| DB driver | psycopg2 | psycopg v3 (binary) |
