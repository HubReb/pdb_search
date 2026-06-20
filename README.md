# Off-line Paper Database Searcher

A personal CLI tool to store publication metadata in a local PostgreSQL database
and search / manage it offline — useful when traveling without internet access.

Stores: paper title, authors, one-sentence summary, and full BibTeX entry.

## Installation

Requires Python ≥ 3.11 and [uv](https://github.com/astral-sh/uv).

```bash
uv sync --all-extras
```

## Quick start

```bash
# Interactive menu (no subcommand):
uv run pdbsearch

# Or with explicit database URL:
uv run pdbsearch --database-url "postgresql+psycopg://user:pass@localhost/papers"

# Using an existing Fernet-encrypted credentials file:
uv run pdbsearch --config ../../database.crypt --key ../../key
```

## Configuration

Priority (highest first):

1. CLI flag: `--database-url TEXT`
2. Environment variable: `PDBSEARCH_DATABASE_URL`
3. `.env` file in the working directory
4. Fernet-encrypted INI file (`--config` + `--key`)

Encrypted INI format:

```ini
[postgresql]
dbname = your_dbname
user   = your_dbuser
password = your_password
host   = localhost
port   = 5432
```

## Subcommands

```bash
uv run pdbsearch search                         # interactive search
uv run pdbsearch add                            # add a paper
uv run pdbsearch update                         # update a field
uv run pdbsearch delete                         # delete a paper
uv run pdbsearch import --tex LIT.tex --bib REF.bib  # bulk import
uv run pdbsearch migrate                        # apply schema migrations
```

## Migrate an existing database

If you have a database from the old version, run:

```bash
uv run pdbsearch migrate
```

Handles both historical schema variants (`bibtex_id` and the `bibtext_id` typo).
Idempotent — safe to re-run.

## Run tests

```bash
uv run pytest                    # full suite (ephemeral PG, no personal DB required)
uv run pytest --cov=src          # with coverage
```

## Lint / type-check

```bash
uv run ruff check src tests
uv run mypy src
```

## Search

From the interactive menu press `1` to search, then choose:

1. **Search by author** — enter name in `Last, First` form; pick from a list if multiple papers found.
2. **Search by title** — exact title match; if multiple papers share the title, pick from a list.

Output format:

```
title: <title>
authors: <Author1, First1 and Author2, First2>
summary: <one-sentence summary>
bib entry: <full BibTeX string>
```

## Add an entry

```
Author(s), comma-separated in 'Last, First' form: Wang, Lin, Müller, Hans
Paper title: Large-Scale Sentence Alignment
BibTeX key: Wang2021LargeScaleSA
1) From a .bib file
2) Enter inline
Your choice: 1
Enter .bib filename: paper.bib
One-sentence summary: Proposes a scalable attention mechanism.
Proceed?
1) (Y)es
2) (N)o
```

## Notes

- All menus are 1-indexed with an explicit quit/abort option.
- Destructive operations (update, delete) require confirmation.
- Plain-language error messages on stdout; technical details go to the log.
- Personal use only — no web UI, no multi-user support.
