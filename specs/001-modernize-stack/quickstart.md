# Quickstart: Modernized paper_sorts

**Feature**: 001-modernize-stack  
**Date**: 2026-06-15

---

## Installation

```bash
# Install with uv (Python ≥ 3.11 required)
git clone <repo>
cd paper_sorts
uv sync --all-extras
```

---

## Configuration

The tool reads connection settings from four sources in priority order (highest first):

1. **CLI flags**: `--database-url postgresql://...` or `--config`/`--key` pair
2. **Environment variables**: `PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`
3. **`.env` file** in the current directory
4. **Fernet-encrypted INI** (default: `../../database.crypt`, key: `../../key`)

Example `.env`:
```
PDBSEARCH_DATABASE_URL=postgresql://user:pass@localhost:5432/papers
```

---

## Running the CLI

```bash
# Interactive mode (four-option menu)
uv run pdbsearch

# Direct subcommands
uv run pdbsearch search --by title --query "speech translation"
uv run pdbsearch add
uv run pdbsearch update --id Wang2021LargeScaleSA
uv run pdbsearch delete --id Wang2021LargeScaleSA
uv run pdbsearch import literature.tex references.bib
uv run pdbsearch migrate
```

---

## Migrating an Existing Database

If you have a personal database from the old version:

```bash
uv run pdbsearch migrate
```

This runs Alembic migrations idempotently. Handles both schema variants:
- `bibtex_id` column (current `DatabaseConnector`)
- `bibtext_id` column (typo, used by old `add.py` / `get_data.py`)

---

## Running the Test Suite

No personal database or credentials needed:

```bash
uv run pytest          # full suite (ephemeral PostgreSQL)
uv run ruff check .    # lint
uv run mypy src        # type-check
```

---

## Running the Benchmark

```bash
uv run pytest tests/benchmarks/ --benchmark-autosave
```

Saves results to `.benchmarks/` for comparison across runs.
