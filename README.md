# Off-line Paper Database searcher

A small, personal CLI for storing publication metadata in a local PostgreSQL
database and searching it offline — e.g. when no internet connection is
available to query an online resource (traveling by train). The database can be
searched by author or by paper title; a hit returns the paper title, authors, a
short summary, and the BibTeX entry.

*Note:* this is a single-user, personal tool. If something goes wrong in your
setup, consult the logs.

## Installation

Dependencies are managed with [uv](https://docs.astral.sh/uv/) (Python ≥ 3.11):

```bash
uv sync --all-extras
```

## Running

```bash
uv run pdbsearch            # interactive four-option menu
uv run pdbsearch search     # search by author / title
uv run pdbsearch add        # add an entry (inline or from a .bib file)
uv run pdbsearch update     # update title / contents / bibtex / author
uv run pdbsearch delete     # delete an entry
uv run pdbsearch import --tex lit.tex --bib refs.bib   # bulk import
uv run pdbsearch migrate    # create / upgrade the database schema
uv run pdbsearch --help     # list commands
```

`migrate` creates the schema from scratch on an empty database, and converges an
older database (with the legacy `bibtext_id` column) onto the canonical schema —
idempotently, with no data loss.

## Configuration

Settings are resolved from four sources, highest priority first:

1. CLI flags: `--database-url`, `--config`, `--key`, `--log-level`
2. environment variables: `PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`, …
3. a `.env` file in the working directory
4. a Fernet-encrypted INI file: `--config <path> --key <path>`

Example with an environment variable:

```bash
export PDBSEARCH_DATABASE_URL="postgresql+psycopg://user:pass@localhost:5432/papers"
uv run pdbsearch search
```

Example with the encrypted config (the legacy workflow, still supported):

```bash
uv run pdbsearch --config ../../database.crypt --key ../../key search
```

The encrypted INI should look like:

```ini
[postgresql]
host = localhost
port = 5432
dbname = your_dbname
user = your_dbuser
password = your_dbuser_password
```

A missing key or missing config file produces a clear, actionable message — not
a stack trace. Keep credential and key files out of version control.

## Search

The interactive menu:

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
```

Choosing *Search* offers *by author* or *by title*. If several papers match
(same title, or an author with many papers) you are asked to pick from a
numbered list. Author names use the format `Last, First`.

## Add an entry

The dialog walks you through authors (a comma-separated `Last, First` list),
title, BibTeX key, the BibTeX entry (typed inline or read from a `.bib` file),
and a summary.

## Update an entry

Choose the table (papers / bib / authors), then the column, then the entry id
and the new value. You confirm a summary of the exact change before it is
applied (`1) (Y)es  2) (N)o`, also accepting `y`/`n`/`yes`/`no`). Use *search*
to find a paper's id.

## Development

```bash
uv run ruff check .          # lint
uv run ruff format --check . # format check
uv run mypy src              # strict type-check
uv run pytest                # real-DB suite via an ephemeral PostgreSQL
```

The test suite provisions an ephemeral PostgreSQL via `pytest-postgresql` off
the host's `pg_ctl`; it needs no personal database or credentials, so a fresh
clone runs it green.
