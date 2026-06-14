# Quickstart: Modernize the Stack

**Feature**: 001-modernize-stack | **Date**: 2026-06-14

## What Changed

The paper-sorts CLI has been rebuilt on mainstream Python libraries. The user-facing
behaviour is identical: same prompts, same database, same operations.

### Under the hood

| Concern | Before | After |
|---------|--------|-------|
| Package manager | Poetry | uv |
| Python minimum | 3.10 | 3.11 |
| DB driver | psycopg2 | psycopg v3 (binary) |
| ORM / queries | hand-written SQL strings | SQLAlchemy 2.x |
| Schema migrations | `create_tables()` at runtime | Alembic versioned migrations |
| CLI framework | argparse + bespoke loop | Typer subcommands |
| Configuration | argparse + Fernet INI reader | pydantic-settings v2 |
| Linting | pylint | ruff |
| Tests | unittest + live DB | pytest + ephemeral PostgreSQL |

## Install

Requires Python ≥ 3.11 and PostgreSQL on the host (for `pg_ctl`).

```bash
git clone <repo-url>
cd paper-sorts
uv sync --all-extras
```

## Run

```bash
# Interactive menu (same as before):
uv run pdbsearch

# Or use subcommands directly:
uv run pdbsearch search
uv run pdbsearch add
uv run pdbsearch update
uv run pdbsearch delete
uv run pdbsearch import --tex literature.tex --bib refs.bib
uv run pdbsearch migrate      # apply DB migrations (run once on first use)
```

## Configuration

Configuration is resolved in priority order (highest first):

1. **CLI flags**: `--database-url`, `--log-level`, `--config`, `--key`
2. **Environment variables**: `PDBSEARCH_DB_HOST`, `PDBSEARCH_DB_PORT`, `PDBSEARCH_DB_NAME`,
   `PDBSEARCH_DB_USER`, `PDBSEARCH_DB_PASSWORD`
3. **`.env` file** in the working directory
4. **Fernet-encrypted INI file** (same format as before):
   ```bash
   uv run pdbsearch --config ../../database.crypt --key ../../key
   ```

## Migrating an Existing Database

If you have an existing personal database, run the migration command once:

```bash
uv run pdbsearch migrate --database-url "postgresql://user:pass@localhost/mydb"
```

This is idempotent — safe to run multiple times. Both legacy schema variants
(`bibtex_id` and `bibtext_id`) are handled automatically.

## Tests

```bash
# Run the full suite (requires pg_ctl on PATH):
uv run pytest

# Run without benchmark tests:
uv run pytest -m "not benchmark"

# Run with coverage:
uv run pytest --cov=src/paper_sorts --cov-report=term-missing
```

The test suite spins up its own ephemeral PostgreSQL instance. No personal database or
config files required.

## Linting / Type Checking

```bash
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy src
```
