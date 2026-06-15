# Quickstart: Modernized Paper Sorts

## Install

Dependencies are managed with **uv** (Python ≥ 3.11):

```bash
uv sync --all-extras          # install runtime + dev deps
```

## Configure the database

Configuration is resolved from four sources, highest priority first:

1. **CLI flags**: `--database-url postgresql+psycopg://user:pw@host:5432/dbname`
2. **Environment**: `PDBSEARCH_DATABASE_URL=...` (prefix `PDBSEARCH_`)
3. **`.env`** file in the working directory
4. **Fernet-encrypted INI**: `--config ../../database.crypt --key ../../key`

The encrypted INI keeps the legacy `[postgresql]` section format:

```ini
[postgresql]
dbname=your_dbname
user=your_dbuser
password=your_dbuser_password
host=localhost
port=5432
```

## First run on a fresh database

```bash
uv run pdbsearch migrate      # create the schema (Alembic upgrade head)
uv run pdbsearch              # start the interactive four-option menu
```

## Migrate an existing personal database

```bash
uv run pdbsearch migrate
```

Converges either historical schema (`bibtex_id` or the legacy `bibtext_id` typo) onto the
canonical schema in one action, with zero data loss. Idempotent — safe to rerun.

## Everyday use

```bash
uv run pdbsearch                       # interactive menu (search / add / update / quit)
uv run pdbsearch search                # interactive search (by author or title)
uv run pdbsearch add                   # add an entry (inline or from a .bib file)
uv run pdbsearch update                # update title / contents / bibtex / author
uv run pdbsearch delete                # delete an entry (with confirmation)
uv run pdbsearch import --tex lit.tex --bib refs.bib   # bulk import
uv run pdbsearch --help                # list subcommands
```

## Development gates

```bash
uv run ruff check src tests            # lint
uv run ruff format --check src tests   # format check
uv run mypy src                        # type-check (strict on src/)
uv run pytest                          # real-DB suite (ephemeral PG via pytest-postgresql)
uv run pytest --cov=src/paper_sorts    # with coverage
```

No personal database, `database.crypt`, or `key` file is required to run the test suite — it
provisions an ephemeral PostgreSQL from the host `pg_ctl`.

## What changed vs. the legacy tool

- Same operations, same prompts, same data — rebuilt on SQLAlchemy 2.x, Typer, Alembic,
  pydantic-settings, pytest, and ruff.
- A reverse-engineered architecture document lives at `docs/architecture.md`.
- Schema is unchanged (four tables); migrations are versioned and reversible.
