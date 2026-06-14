# Quickstart: Modernized paper_sorts

## Install

```bash
# Requires Python >= 3.11 and uv
uv sync --all-extras
```

## Configure

Priority order (highest first): CLI flags → environment variables → `.env` file → Fernet-encrypted INI.

**Simplest approach — environment variable:**
```bash
export PDBSEARCH_DATABASE_URL="postgresql+psycopg://user:pass@localhost/mydb"
```

**Using a `.env` file:**
```
PDBSEARCH_DATABASE_URL=postgresql+psycopg://user:pass@localhost/mydb
PDBSEARCH_LOG_LEVEL=INFO
```

**Using the legacy encrypted config** (backward compatible):
```bash
pdbsearch --config ../../database.crypt --key ../../key
```

## First-time setup (fresh database)

```bash
# Apply all schema migrations
pdbsearch migrate

# (Optional) bulk import from your LaTeX literature overview
pdbsearch import --tex literature_overview.tex --bib bib.bib
```

## Migrate an existing personal database

```bash
# Works with both historical schemas (bibtex_id and bibtext_id columns)
pdbsearch migrate
```

The migration is idempotent — safe to run multiple times.

## Run

```bash
# Interactive mode (four-option menu)
uv run pdbsearch

# Direct subcommand
uv run pdbsearch search
uv run pdbsearch add
uv run pdbsearch update
uv run pdbsearch delete
```

## Test

```bash
# Full suite — no personal database needed
uv run pytest

# With coverage
uv run pytest --cov=src/paper_sorts --cov-report=term-missing
```

## Development commands

```bash
uv run ruff check src tests    # lint
uv run ruff format src tests   # format
uv run mypy src                # type-check
```

## What changed from the legacy stack

| Before | After |
|--------|-------|
| `poetry` | `uv` (PEP 621 pyproject.toml) |
| `psycopg2` | `psycopg` v3 (via SQLAlchemy 2.x) |
| Hand-written SQL strings | SQLAlchemy ORM + query builder |
| `argparse` + manual dialog loop | Typer subcommands |
| `unittest` with live personal DB | pytest + ephemeral DB via pytest-postgresql |
| `pylint` | ruff |
| Bespoke config file reader | pydantic-settings v2 |
| `create_tables()` at runtime | Alembic versioned migrations |
| Flat `paper_sorts/` layout | `src/paper_sorts/` src-layout |
