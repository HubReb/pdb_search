# Quickstart: Modernized pdbsearch

**Feature**: 001-modernize-stack | **Date**: 2026-06-15

## Installation

```bash
# Clone (or pull) the repository
git clone <repo>
cd pdb_search

# Install all runtime and dev dependencies (Python >= 3.11 required)
uv sync --all-extras
```

## Configuration

Choose one of the following configuration approaches (highest priority first):

### 1. CLI flags (highest priority)
```bash
uv run pdbsearch --database-url "postgresql+psycopg://user:password@localhost/papers"
```

### 2. Environment variables
```bash
export PDBSEARCH_DATABASE_URL="postgresql+psycopg://user:password@localhost/papers"
uv run pdbsearch
```

### 3. .env file
Create a `.env` file in the project root:
```
PDBSEARCH_DATABASE_URL=postgresql+psycopg://user:password@localhost/papers
PDBSEARCH_LOG_LEVEL=INFO
```

### 4. Fernet-encrypted INI (legacy — lowest priority)
```bash
uv run pdbsearch --config /path/to/database.crypt --key /path/to/key
```
The encrypted INI must have a `[postgresql]` section with `dbname`, `user`, `password` (and optionally `host`, `port`).

## First-time database setup

Apply migrations to create (or upgrade) the schema:
```bash
uv run pdbsearch migrate
```

For an existing database with the legacy `bibtext_id` typo column, the migration handles the rename automatically.

## Running the CLI

### Interactive mode (no subcommand)
```bash
uv run pdbsearch
```
Presents a four-option menu: Search / Add / Update / Quit.

### Direct subcommands
```bash
uv run pdbsearch search          # interactive search
uv run pdbsearch add             # add a new entry
uv run pdbsearch update          # update an existing entry
uv run pdbsearch delete          # delete an entry
```

### Bulk import from .tex + .bib
```bash
uv run pdbsearch import --tex literature_overview.tex --bib references.bib
```

## Development

```bash
uv run ruff check src tests       # lint
uv run ruff format --check src    # format check
uv run mypy src                   # type-check (strict on src/)
uv run pytest                     # run the full test suite (ephemeral PG via pytest-postgresql)
uv run pytest tests/benchmarks/   # run the performance baseline benchmark
```

No live database or credentials are required for the test suite. `pytest-postgresql` spins up an ephemeral PostgreSQL instance automatically using the host `pg_ctl`.

## Key differences from the legacy stack

| Before | After |
|--------|-------|
| `poetry install` | `uv sync --all-extras` |
| `poetry run python paper_sorts/run.py -c <cfg> -k <key>` | `uv run pdbsearch` |
| `poetry run pylint paper_sorts` | `uv run ruff check src tests` |
| `poetry run python -m unittest discover tests` | `uv run pytest` |
| Requires live personal DB for tests | Ephemeral DB via pytest-postgresql |
| Flat layout `paper_sorts/` | src-layout `src/paper_sorts/` |
| psycopg2 + manual SQL strings | SQLAlchemy 2.x + psycopg v3 |
| ConfigReader (bespoke) | pydantic-settings (four-source priority chain) |
