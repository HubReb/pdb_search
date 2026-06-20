# Quickstart: Modernized paper-sorts CLI

## Requirements

- Python >= 3.11
- PostgreSQL (local instance; `pg_ctl` available for tests)
- uv (`~/.local/bin/uv` or on PATH)

## Install

```bash
git clone <repo>
cd paper_sorts
uv sync --all-extras
```

## Configure

The database URL can be provided in any of these ways (highest priority first):

1. **CLI flag**: `pdbsearch --database-url postgresql+psycopg://user:pass@localhost/mydb`
2. **Environment variable**: `export PDBSEARCH_DATABASE_URL=postgresql+psycopg://...`
3. **`.env` file** in the project root: `PDBSEARCH_DATABASE_URL=postgresql+psycopg://...`
4. **Encrypted INI** (legacy): `pdbsearch --config ../../database.crypt --key ../../key`

## First-Time Setup (new database)

```bash
pdbsearch migrate
```

This runs all Alembic migrations to create the schema.

## Migrate Existing Database

If you have an existing personal database (with either `bibtex_id` or `bibtext_id` column):

```bash
pdbsearch migrate
```

All papers, authors, BibTeX entries, and authorship links are preserved.

## Run

```bash
uv run pdbsearch          # interactive four-option menu
uv run pdbsearch search   # search subcommand directly
uv run pdbsearch add      # add subcommand directly
uv run pdbsearch update   # update subcommand directly
uv run pdbsearch delete   # delete subcommand directly
uv run pdbsearch import literature_overview.tex bib.bib   # bulk import
```

## Test

```bash
uv run pytest             # runs against ephemeral PostgreSQL; no personal DB needed
```

## Lint / Type-check

```bash
uv run ruff check src tests
uv run mypy src
```
