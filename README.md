# Off-line Paper Database Searcher

A small CLI tool to store and search academic publication metadata in a local PostgreSQL database — for use when no online connection is available (e.g. traveling by train).

Search by author or title. For each paper the tool returns title, authors, summary, and the BibTeX entry.

*Note*: Personal-use tool. Single user, local PostgreSQL, no network surface.

## Installation

Requires Python ≥ 3.11 and [uv](https://docs.astral.sh/uv/).

```bash
git clone <repo>
cd pdb_search
uv sync --all-extras
```

## First Run: Migrate the Database

On a fresh install or when upgrading from an older version:

```bash
uv run pdbsearch migrate
```

## Configuration

Set the database URL via environment variable, `.env` file, or the legacy encrypted config:

```bash
# Option 1: env var
export PDBSEARCH_DATABASE_URL="postgresql+psycopg://user:password@localhost/mydb"

# Option 2: .env file
echo 'PDBSEARCH_DATABASE_URL=postgresql+psycopg://user:password@localhost/mydb' > .env

# Option 3: legacy Fernet-encrypted INI (keeps existing database.crypt + key)
uv run pdbsearch --config ../../database.crypt --key ../../key
```

The encrypted INI must have a `[postgresql]` section with `dbname`, `user`, `password`, `host`, `port`.

## Usage

### Interactive mode

```bash
uv run pdbsearch
```

Shows a four-option menu:

```
1) Search
2) Add
3) Update
4) Delete
q) Quit
```

### Subcommands (scripted)

```bash
uv run pdbsearch search           # search submenu
uv run pdbsearch add              # add a paper
uv run pdbsearch update [--id N]  # update a field
uv run pdbsearch delete [--id N]  # delete a paper
uv run pdbsearch import --tex literature_overview.tex --bib refs.bib
uv run pdbsearch migrate          # apply schema migrations
```

## Running Tests

```bash
uv run pytest                                         # full suite (ephemeral PG)
uv run pytest tests/benchmarks/ -m benchmark -v      # benchmark harness
```

## Linting and Type Checking

```bash
uv run ruff check src tests
uv run mypy src
```

## Architecture

See `docs/architecture.md` for the full architecture description, data model, control flow, and configuration chain.
