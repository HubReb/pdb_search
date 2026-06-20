# Research: Modernize the Stack (001-modernize-stack)

**Feature Branch**: `001-modernize-stack`
**Date**: 2026-06-20
**Status**: Complete

---

## R01 — ORM & Database Toolkit

**Decision**: SQLAlchemy 2.x (declarative ORM) + psycopg v3 (binary) driver

**Rationale**:
- Industry standard for Python ORM; parameterised queries, joins, and transactions are first-class.
- SQLAlchemy 2.x uses `Session` as a context manager (`with Session(engine) as s:`), which satisfies the constitution's "sessions closed deterministically" requirement.
- `psycopg` v3 (binary) is already listed in the constitution's Stack & Constraints.
- The persistence-layer isolation rule maps cleanly: only `src/paper_sorts/db/` imports `sqlalchemy` or `psycopg`.

**Alternatives considered**:
- `peewee`: smaller, but limited async/migration tooling and less community adoption.
- Raw `psycopg` only (no ORM): preserves hand-written SQL, which is exactly the problem to remove.

---

## R02 — Migration Tool

**Decision**: Alembic (SQLAlchemy's official migration companion)

**Rationale**:
- Alembic is the de-facto standard for SQLAlchemy projects.
- Supports reversible migrations (`upgrade` / `downgrade`), version chaining, and idempotent `--sql` mode.
- Revision 001 will be the verbatim port of the existing `create_tables()` DDL (canonical schema: `bibtex_id` column name).
- Revision 002 will be a convergence migration that handles the `bibtext_id` (typo) legacy variant by detecting the column name at runtime and renaming if needed.

**Alternatives considered**:
- `yoyo-migrations`: lighter but not SQLAlchemy-integrated.
- Manual `CREATE TABLE IF NOT EXISTS`: already in use and is the problem to replace.

---

## R03 — CLI Framework

**Decision**: Typer (Click-based)

**Rationale**:
- Subcommands (`search`, `add`, `update`, `delete`, `import`, `migrate`) map to Typer commands cleanly.
- Type hints drive argument parsing with no boilerplate.
- `typer.prompt` is available but the constitution (Principle III) requires all prompt calls to route through `paper_sorts.cli.prompts` — Typer's prompt is allowed only inside that module.
- Invoked with no subcommand → drops into the interactive four-option top-level menu (preserve legacy UX).

**Alternatives considered**:
- `argparse` (current): bespoke, verbose, no subcommand auto-help.
- `click` directly: Typer wraps Click; using Typer gives type-hint ergonomics.

---

## R04 — Settings / Configuration

**Decision**: pydantic-settings v2 with a custom `FernetIniSettingsSource`

**Rationale**:
- `pydantic-settings` supports env vars, `.env` files, and custom sources in a single `BaseSettings` class.
- Priority order (highest first): CLI flags → `PDBSEARCH_*` env vars → `.env` → Fernet-encrypted INI.
- The custom source reads the encrypted `.crypt` file + key file and injects values; the user can continue to use their existing `database.crypt` / `key` files unchanged.
- Plaintext credentials and keys are never logged (the `Settings` model uses `SecretStr` for the password field).

**Alternatives considered**:
- `dynaconf`: more features but heavier; pydantic-settings is already in the stack for data validation.
- Bare `ConfigParser`: what exists now; does not support env vars or `.env`.

---

## R05 — Test Runner & Ephemeral DB

**Decision**: pytest + pytest-postgresql (host `pg_ctl`)

**Rationale**:
- `pytest-postgresql` spins up a per-session PostgreSQL instance via the host's `pg_ctl` binary (`/usr/bin/pg_ctl` on this machine).
- No Docker required; works on a fresh checkout as long as PostgreSQL is installed.
- Session fixtures in `tests/conftest.py` provide `ephemeral_db_url` (a SQLAlchemy URL) that tests use.
- Seed data lives in `tests/fixtures/seed_papers.py` (a `SEED_PAPERS` list of `PaperCreate` dicts), co-located with tests per constitution Principle II.
- Integration tests for the persistence layer run against this real DB — no mocking the session or repositories.

**Alternatives considered**:
- `pytest-docker`: requires Docker daemon; heavier setup.
- In-memory SQLite: does not exercise PostgreSQL-specific SQL; constitution forbids mocking persistence.

---

## R06 — Linter / Formatter

**Decision**: ruff (check + format)

**Rationale**:
- Constitution v1.3.0 already specifies ruff; this feature makes the switch concrete.
- Replaces pylint; `ruff check` and `ruff format --check` are the quality gates.
- mypy (strict on `src/`) is added for static type checking per constitution Principle I.

**Alternatives considered**:
- pylint (current): slower, harder to configure, being replaced by ruff across the ecosystem.

---

## R07 — BibTeX Parsing

**Decision**: Keep pybtex

**Rationale**:
- pybtex is already a dependency; it handles LaTeX accents/escapes and BibTeX `to_string("bibtex")` round-trips correctly.
- The import service uses `parse_file` + `Entry.to_string("bibtex")` — this is preserved verbatim in `import_service.py`.

**Alternatives considered**:
- `bibtexparser` v2: newer, but would require re-testing all edge cases (LaTeX escapes, author name parsing).

---

## R08 — Build Backend

**Decision**: hatchling + uv (PEP 621 `[project]` metadata)

**Rationale**:
- Constitution Stack & Constraints already mandates uv + hatchling.
- `pyproject.toml` replaces the legacy `poetry`-style metadata.
- `uv.lock` pins all dependencies for reproducibility.
- Entry point: `pdbsearch = "paper_sorts.cli.app:app"` (Typer app object).

---

## R09 — Schema & Migration Analysis

**Two historical schema variants detected**:

| Column | `database_connector.py` (newer) | `get_data.py` / `add.py` / `search.py` (older) |
|--------|----------------------------------|------------------------------------------------|
| papers FK  | `bibtex_id` | `bibtext_id` (typo) |
| bib PK | `bibtex_id` | `bibtext_id` (typo) |
| bib bibtex col | `bibtex` (UNIQUE) | `bibtext` (no UNIQUE) |

**Canonical (modernized) schema**: `bibtex_id` everywhere, `bibtex` column in `bib` with UNIQUE constraint.

**Migration strategy**:
- Revision 001: `CREATE TABLE IF NOT EXISTS` for all four tables using canonical names.  
- Revision 002: Convergence — detect `bibtext_id` (typo) columns; if present, rename to `bibtex_id` and rename `bibtext` → `bibtex` in the `bib` table. Idempotent (checks column existence before renaming).

**Schema preservation contract** (from CLAUDE.md):
- Do NOT add NOT NULL constraints outside primary keys.
- Do NOT add FKs to `authors_papers`.
- Do NOT add indexes beyond existing primary keys.

---

## R10 — Constitution Amendments (FR-016)

The following amendments were ratified as v1.3.0 (already applied to `.specify/memory/constitution.md`):

| Amendment | Old text | New text |
|-----------|----------|---------|
| I. Code Quality — isolation rule | `psycopg2` isolated to `PsycopgDB` | `sqlalchemy`/`psycopg` isolated to `src/paper_sorts/db/` |
| I. Code Quality — linter | pylint | ruff |
| II. Testing Standards — framework | unittest | pytest + pytest-postgresql |
| III. UX Consistency — prompt routing | `helpers.get_user_input` | `paper_sorts.cli.prompts` |
| IV. Performance — references | function-level (`PsycopgDB`, etc.) | layer-level; non-regression criterion |

All amendments are already encoded in constitution v1.3.0 — no further amendment is required as part of implementation.

---

## R11 — Logging

**Decision**: stdlib `logging` with `RichHandler` (stdout) + optional `FileHandler`; configured once in `cli/app.py` via `logging.config.dictConfig`.

**Rationale**:
- Constitution Principle III requires plain-language errors on stdout and technical detail in logs.
- `RichHandler` from `rich.logging` provides readable stdout output without coupling the whole codebase to rich.
- Per-class log files (`db_connector.log`, `interaction.log`) are not required verbatim; a single configurable-level logger is sufficient.
- Log level is controlled via `--log-level` CLI flag / `PDBSEARCH_LOG_LEVEL` env var.

---

## R12 — Src Layout & Package Structure

**Decision**: `src/paper_sorts/` with subpackages `cli/`, `services/`, `db/`

**Rationale**:
- Modern Python packaging practice; avoids accidental import of the working-tree package in tests.
- Matches the architecture described in CLAUDE.md.
- Legacy flat layout `paper_sorts/` is removed once all functionality is covered (FR-012).

**Subpackage responsibilities**:
- `cli/`: Typer commands + `prompts.py` (the only place allowed to call `input()` / `Prompt.ask`).
- `services/`: Domain logic — `paper_service.py`, `import_service.py`. No SQL, no rich, no I/O.
- `db/`: `models.py` (ORM), `repositories.py` (PaperRepository, AuthorRepository, BibRepository), `session.py` (`with_session` context manager). **Only place that imports sqlalchemy or psycopg.**
- `config.py`: pydantic-settings `Settings`.
- `logging_config.py`: single `dictConfig` call.
