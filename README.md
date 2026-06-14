# Off-line Paper Database Searcher

A small, personal CLI for storing and querying publication metadata in a local PostgreSQL
database — useful when traveling without internet access.

Searches by author or title. Results include: title, authors, summary, and BibTeX entry.

*Note*: Personal-use tool; single-user, offline only.

## Installation

Requires Python ≥ 3.11 and a system PostgreSQL (for the personal database; tests spin up
their own ephemeral instance automatically).

```bash
uv sync --all-extras
```

## Usage

```bash
# Interactive mode (search / add / update / delete menu):
uv run pdbsearch

# With explicit database URL:
uv run pdbsearch --database-url postgresql+psycopg://user:pass@localhost/mydb

# With encrypted credentials file (legacy workflow preserved):
uv run pdbsearch --config ../../database.crypt --key ../../key
```

## Subcommands

```bash
uv run pdbsearch search     # search by title or author
uv run pdbsearch add        # add a new paper
uv run pdbsearch update     # update a field on an existing paper
uv run pdbsearch delete     # delete a paper
uv run pdbsearch import --tex lit.tex --bib refs.bib   # bulk import
uv run pdbsearch migrate    # upgrade personal database schema
```

## Migrate an Existing Database

If you have a personal database from a previous version:

```bash
uv run pdbsearch --database-url postgresql+psycopg://... migrate
```

Idempotent — safe to run multiple times. Handles both historical column-name variants
(`bibtex_id` and `bibtext_id`).

## Configuration

Priority order (highest first):

1. CLI flags (`--database-url`, `--log-level`, etc.)
2. Environment variables (`PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`, …)
3. `.env` file in current directory
4. Fernet-encrypted INI file (`--config` + `--key`)

Encrypted config format (INI, then encrypt with Fernet):
```ini
[postgresql]
dbname = your_dbname
user = your_dbuser
password = your_password
```

## Development

```bash
uv sync --all-extras          # install deps (including dev)
uv run ruff check src tests   # lint
uv run mypy src               # type-check (strict on src/)
uv run pytest                 # run the test suite (ephemeral PostgreSQL)
```
