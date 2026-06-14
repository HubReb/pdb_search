# Quickstart: Modernized Stack

**Feature**: 001-modernize-stack | **Date**: 2026-06-14

## What Changed

The internals were rebuilt on mainstream Python libraries. **Your data is unchanged; your usage patterns are unchanged.**

| Before | After |
|--------|-------|
| `poetry install` | `uv sync --all-extras` |
| `python -m paper_sorts.run` | `uv run pdbsearch` |
| `python paper_sorts/get_data.py ...` | `uv run pdbsearch import --tex FILE --bib FILE` |
| `pylint paper_sorts` | `uv run ruff check src tests` |
| `python -m unittest discover` | `uv run pytest` |
| Schema created at runtime by `create_tables()` | Schema managed by Alembic migrations |

## Install (Fresh Machine)

```bash
git clone <repo>
cd pdb_search
uv sync --all-extras
```

No external database or credentials needed to run the test suite.

## Run

```bash
# Interactive mode (top-level menu):
uv run pdbsearch

# Direct subcommands:
uv run pdbsearch search
uv run pdbsearch add
uv run pdbsearch update
uv run pdbsearch delete
uv run pdbsearch import --tex literature_overview.tex --bib bib.bib

# Admin operations (not in interactive menu):
uv run pdbsearch migrate
```

## Configuration

Priority order (highest first):

1. CLI flags: `--database-url`, `--log-level`, `--config`, `--key`
2. Environment variables: `PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`, etc.
3. `.env` file in the working directory
4. Fernet-encrypted INI file (default: `../../database.crypt` + `../../key`)

The encrypted config workflow from the original version is fully preserved.

**Example `.env`**:
```
PDBSEARCH_DATABASE_URL=postgresql+psycopg://user:pass@localhost/papers
PDBSEARCH_LOG_LEVEL=INFO
```

## Migrate Existing Personal Database

If you have an existing personal database, run the migration once:

```bash
uv run pdbsearch migrate
```

The migration is idempotent — safe to rerun. It handles both historical schema variants (`bibtex_id` and the legacy `bibtext_id` typo column).

## Run Tests

```bash
uv run pytest                    # all tests
uv run pytest -v                 # verbose
uv run pytest --tb=short         # shorter tracebacks
```

Tests spin up an ephemeral PostgreSQL instance automatically using `pg_ctl` from the host.

## Quality Gates

```bash
uv run ruff check src tests      # lint
uv run ruff format --check src tests  # format check
uv run mypy src                  # type check
uv run pytest                    # tests
```

All four must pass before merge.
