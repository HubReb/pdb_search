# Quickstart: Modernized paper_sorts

**Feature**: 001-modernize-stack | **Date**: 2026-06-14

## What Changed

The internal stack is replaced. User-visible behaviour is identical:

| Before | After |
|--------|-------|
| `poetry install` | `uv sync --all-extras` |
| `poetry run python paper_sorts/run.py -c ... -k ...` | `uv run pdbsearch [--database-url ...]` |
| `poetry run pylint paper_sorts` | `uv run ruff check src tests` |
| `poetry run python -m unittest discover tests` | `uv run pytest` |

## Install

```bash
uv sync --all-extras
```

Requires Python ≥ 3.11 and PostgreSQL (for the live DB; tests use an ephemeral instance).

## Run the CLI

```bash
# Interactive mode (four-option menu):
uv run pdbsearch

# Or with explicit database URL:
uv run pdbsearch --database-url postgresql+psycopg://user:pass@localhost/mydb

# Or with encrypted config (legacy workflow preserved):
uv run pdbsearch --config ../../database.crypt --key ../../key
```

## Subcommands

```bash
uv run pdbsearch search     # search by title or author
uv run pdbsearch add        # add a new paper
uv run pdbsearch update     # update a field on an existing paper
uv run pdbsearch delete     # delete a paper
uv run pdbsearch import --tex lit.tex --bib refs.bib   # bulk import
uv run pdbsearch migrate    # upgrade personal DB schema to current revision
```

## Migrate an Existing Database

If you have a personal database from the previous version:

```bash
uv run pdbsearch --database-url postgresql+psycopg://... migrate
```

This runs `alembic upgrade head` against your database. Safe to run multiple times (idempotent). Works on both historical column variants (`bibtex_id` and `bibtext_id`).

## Configuration Priority

Highest to lowest:

1. CLI flags (`--database-url`, `--log-level`, etc.)
2. Environment variables (`PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`, …)
3. `.env` file in current directory
4. Fernet-encrypted INI (`--config` + `--key`)

## Run Tests

```bash
uv run pytest                    # full suite (ephemeral PostgreSQL via pytest-postgresql)
uv run ruff check src tests      # lint
uv run mypy src                  # type-check
```

No external database required — `pytest-postgresql` spins one up from your system `pg_ctl`.
