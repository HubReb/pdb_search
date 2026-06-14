# Research: Modernize the Stack

**Feature**: 001-modernize-stack  
**Date**: 2026-06-14  
**Branch**: rep/001-A1

## R1 — ORM Choice

**Decision**: SQLAlchemy 2.x (with `psycopg` v3 as driver)  
**Rationale**: Widest adoption in Python ecosystem; SQLAlchemy 2.x has mature async and sync APIs. The constitution mandates psycopg v3 (binary). SQLAlchemy 2.x's `DeclarativeBase`, typed columns, and ORM session context-managers align with the layered architecture required. Repository pattern over ORM is idiomatic.  
**Alternatives considered**: Django ORM (too heavy, requires WSGI project), Tortoise ORM (async-only), raw psycopg v3 (too bespoke).

## R2 — CLI Framework

**Decision**: Typer (built on Click)  
**Rationale**: Typer offers automatic `--help`, subcommand routing, and `CliRunner` for test-time invocation without spawning subprocess. Minimal boilerplate. Constitution requires a "mainstream CLI framework" to replace argparse + manual dialog. Typer satisfies FR-006.  
**Alternatives considered**: Click (good, but Typer adds type-hint-driven ergonomics); argparse (current — staying is not modernization).

## R3 — Settings / Config

**Decision**: pydantic-settings v2  
**Rationale**: Supports environment variables (`PDBSEARCH_*`), `.env` file, and custom sources — we will implement a `FernetIniSettingsSource` for the encrypted legacy config. Four-source priority chain (CLI > env > .env > encrypted file) maps cleanly onto pydantic-settings `model_config` with `env_prefix`. FR-007 satisfied.  
**Alternatives considered**: `dynaconf` (overpowered), plain `python-dotenv` (no Fernet), `hydra` (too heavy).

## R4 — Migration Tool

**Decision**: Alembic  
**Rationale**: Standard SQLAlchemy companion; generates reversible versioned migrations; `alembic upgrade head` is idiomatic. The existing schema (`papers`, `bib`, `authors_id`, `authors_papers`) maps cleanly onto ORM models; Revision 001 will be a verbatim port of the `CREATE TABLE` DDL from `get_data.py`. Revision 002 handles the `bibtext_id` → `bibtex_id` column rename.  
**Alternatives considered**: Flyway (JVM), Liquibase (JVM), raw SQL files (no tooling).

## R5 — Test Framework + Ephemeral DB

**Decision**: pytest + pytest-postgresql  
**Rationale**: pytest is the constitution's mandated replacement for unittest. pytest-postgresql uses the host `pg_ctl` to spin up a throwaway cluster per session; no containers required. Seed data will live in `tests/fixtures/seed_papers.py`. Constitution Principle II explicitly names pytest-postgresql.  
**Alternatives considered**: testcontainers (requires Docker daemon), pytest-docker (same), homegrown temp-DB script.

## R6 — Linting / Formatting

**Decision**: ruff  
**Rationale**: Constitution v1.3.0 explicitly changed "pylint → ruff" (Principle I). ruff subsumes isort + pyflakes + many pylint checks. Format with `ruff format`. No separate black needed.  
**Alternatives considered**: pylint (superseded by constitution amendment), black+flake8 (more tools to configure).

## R7 — BibTeX Parsing

**Decision**: Keep pybtex for file-based BibTeX parsing; use pylatexenc for LaTeX-to-text conversion.  
**Rationale**: Both are already dependencies and work correctly. FR spec says switching is permitted if functionally equivalent — but there is no reason to switch here.  
**Alternatives considered**: bibtexparser (newer, but different API; not worth the friction).

## R8 — Build / Packaging

**Decision**: uv + pyproject.toml (PEP 621) + hatchling build backend  
**Rationale**: Constitution Stack & Constraints mandates uv. `[project.scripts]` entry `pdbsearch = "paper_sorts.cli.app:main"` provides the CLI entry point. `[tool.uv]` dev-dependency group for pytest, ruff, mypy.  
**Legacy**: Current `pyproject.toml` uses poetry-core backend; we will replace with hatchling. `paper_sorts/` flat layout → `src/paper_sorts/` src-layout (required for hatchling by convention).

## R9 — Architecture Layers

**Decision**: Four-layer src-layout:
1. `src/paper_sorts/cli/` — Typer subcommands + `prompts.py` (sole importer of rich.prompt / typer.prompt)
2. `src/paper_sorts/services/` — domain logic, no SQL, no I/O
3. `src/paper_sorts/db/` — SQLAlchemy models, repositories, session; sole importer of `sqlalchemy` and `psycopg`
4. `src/paper_sorts/config.py` — pydantic-settings Settings model

**Rationale**: Satisfies FR-014, Principle I layering rule, and the constitution's driver-isolation requirement.

## R10 — Constitution Amendments Required

FR-016 mandates explicit amendments for constitution text that conflicts with modernization. Amendments already applied in constitution v1.3.0:
- Principle I: driver-isolation rule rewritten to layer-level (`db/` not `PsycopgDB`); pylint → ruff
- Principle II: unittest → pytest; pytest-postgresql named; "no mocking psycopg" → "no mocking SQLAlchemy session"
- Principle III: prompt-routing reference moved from `helpers.get_user_input` to `paper_sorts.cli.prompts`
- Principle IV: function-level performance references replaced with layer-level; non-regression criterion vs. current baseline

All four constitution principles are accounted for. No new amendment needed.

## R11 — Database Schema (Canonical)

From analysis of `get_data.py` DDL and `database_connector.py`:
```sql
CREATE TABLE bib (bibtex_id TEXT PRIMARY KEY, bibtex TEXT UNIQUE);
CREATE TABLE papers (id SERIAL PRIMARY KEY, title TEXT, contents TEXT, bibtex_id TEXT,
    CONSTRAINT fk_bibtex_id FOREIGN KEY(bibtex_id) REFERENCES bib(bibtex_id));
CREATE TABLE authors_id (id SERIAL PRIMARY KEY, author TEXT);
CREATE TABLE authors_papers (id SERIAL PRIMARY KEY, author_id INT, paper_id INT);
```

Note: Legacy modules `add.py` and `search.py` used the typo column `bibtext_id` (swapped `x` and `t`). `database_connector.py` normalised to `bibtex_id`. Alembic Revision 001 ports the canonical `bibtex_id` DDL. Revision 002 guards against legacy `bibtext_id` column with an idempotent rename via `op.alter_column`.

## R12 — Legacy Module Survey

| Module | LOC (approx) | Fate |
|--------|-------------|------|
| `paper_sorts/run.py` | 44 | Remove (replaced by `cli/app.py`) |
| `paper_sorts/user_interaction.py` | 185 | Remove (replaced by `cli/` subcommands) |
| `paper_sorts/database_connector.py` | ~250 | Remove (replaced by `services/` + `db/`) |
| `paper_sorts/psycopg_db.py` | ~145 | Remove (replaced by SQLAlchemy session) |
| `paper_sorts/helpers.py` | ~120 | Remove (split into `cli/prompts.py` + `services/`) |
| `paper_sorts/config_reader.py` | 45 | Remove (replaced by `config.py`) |
| `paper_sorts/add.py` | ~100 | Remove (functionality → `services/paper_service.py` + `cli/`) |
| `paper_sorts/search.py` | ~130 | Remove (functionality → `services/paper_service.py` + `cli/`) |
| `paper_sorts/get_data.py` | ~165 | Remove (functionality → `services/import_service.py`) |

Total legacy project-authored lines: ~1 184. Expected SC-005 target: ~830 lines max in `src/paper_sorts/` (excluding tests + migrations).

## R13 — Edge Cases from Spec

- `bibtex` column in `bib` table must have a `UNIQUE` constraint (currently in DDL; preserve it)
- `authors_papers` has no DDL foreign keys (preserve: schema-preservation contract)
- BibTeX LaTeX escapes must round-trip through pybtex without corruption
- Empty input re-prompt: must remain in modernized `cli/prompts.py`
- Ctrl+C mid-dialog: SQLAlchemy's context-managed session ensures rollback on exception exit
- Multiple papers same title: disambiguation prompt in `cli/search.py` subcommand
- Duplicate author names: treated as the same author (current behaviour; documented limitation)

## R14 — Benchmark Harness (Constitution IV Gate)

Constitution v1.3.0-b2-hardened adds a merge-blocking gate: a baseline benchmark harness MUST exist and execute. Plan: `tests/benchmarks/bench_baseline.py` records wall-clock timings for search-by-title, search-by-author, add, update, delete on seeded data, writes JSON to `tests/benchmarks/baseline.json`. The bench suite runs under `pytest -m benchmark` (separate mark so default `pytest` doesn't run it, but it remains executable and documented).

## R15 — Doc-Currency Gate (Constitution I Gate)

Constitution v1.3.0-b2-hardened: `README.md` and `CLAUDE.md` MUST NOT contain the tokens `Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB` after FR-012 (legacy modules removed). A test in `tests/test_doc_currency.py` checks this mechanically.
