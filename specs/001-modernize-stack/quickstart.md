# Quickstart: Modernized pdbsearch

**Feature Branch**: `001-modernize-stack`
**Date**: 2026-06-20

---

## Install

```bash
# Requires Python ≥ 3.11 and uv
uv sync --all-extras
```

---

## Run

```bash
# Interactive menu (no subcommand):
uv run pdbsearch

# Subcommand usage:
uv run pdbsearch search
uv run pdbsearch add
uv run pdbsearch update
uv run pdbsearch delete
uv run pdbsearch import --tex literature_overview.tex --bib refs.bib
uv run pdbsearch migrate
```

---

## Configuration

Priority (highest first):

1. **CLI flags**: `--database-url`, `--log-level`, `--config`, `--key`
2. **Environment variables**: `PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`
3. **`.env` file** in the working directory
4. **Fernet-encrypted INI** (existing `database.crypt` + `key`):
   ```bash
   uv run pdbsearch --config ../../database.crypt --key ../../key
   ```

Minimum required: `database_url` (e.g. `postgresql+psycopg://user:pass@localhost/papers`).

---

## Migrate existing database

```bash
# From the repo root:
uv run pdbsearch migrate
```

Handles both legacy schema variants (`bibtex_id` and `bibtext_id` typo). Idempotent.

---

## Run tests

```bash
uv run pytest                # full suite (ephemeral PG, no personal DB required)
uv run pytest -x             # stop on first failure
uv run pytest --cov=src      # with coverage
```

---

## Lint / type-check

```bash
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy src
```

---

## Bulk import

```bash
uv run pdbsearch import --tex literature_overview.tex --bib refs.bib
```

Already-imported entries are skipped (idempotent by BibTeX key). Partial failures leave prior entries intact.
