# Off-line Paper Database Searcher

A small, bare-bones CLI for storing and querying academic publication metadata in a local
PostgreSQL database — useful when no internet connection is available (e.g. traveling by train).

Search by author or publication title. Each entry stores:

- Paper title
- Author(s)
- One-sentence summary
- Full BibTeX entry

*Note*: This is a personal-use tool. Multi-user, network-exposed, or concurrent-user modes
are out of scope.

## Installation

Requires Python ≥ 3.11 and [uv](https://docs.astral.sh/uv/):

```bash
uv sync --all-extras
```

## First-time setup

```bash
# Apply schema migrations (creates tables)
uv run pdbsearch migrate

# (Optional) bulk import from a LaTeX literature overview
uv run pdbsearch import --tex literature_overview.tex --bib bib.bib
```

## Configuration

Priority order (highest first): CLI flags → `PDBSEARCH_*` env vars → `.env` file → Fernet-encrypted INI.

**Simplest — environment variable:**
```bash
export PDBSEARCH_DATABASE_URL="postgresql+psycopg://user:pass@localhost/mydb"
uv run pdbsearch
```

**Using a `.env` file:**
```
PDBSEARCH_DATABASE_URL=postgresql+psycopg://user:pass@localhost/mydb
PDBSEARCH_LOG_LEVEL=INFO
```

**Using the legacy encrypted config (backward compatible):**
```bash
uv run pdbsearch --config ../../database.crypt --key ../../key
```

## Interactive mode

Run `uv run pdbsearch` with no subcommand to enter the four-option menu:

```
What do you want to do?
  1) Search the database
  2) Add an entry
  3) Update an entry
  4) (Q)uit
Your choice:
```

## Direct subcommands

```bash
uv run pdbsearch search   # search by author or title
uv run pdbsearch add      # add a new entry
uv run pdbsearch update   # update an existing entry
uv run pdbsearch delete   # delete an entry
uv run pdbsearch import --tex FILE --bib FILE  # bulk import
uv run pdbsearch migrate  # apply schema migrations
```

## Search

### By title

Enter the exact paper title. If multiple papers share the title, you are asked to pick one.

### By author

Enter the author name in `Last, First` form. All papers by that author are listed; you pick one.

## Add an entry

The add dialog prompts for authors (comma-separated), title, BibTeX key, and either
an inline BibTeX string or a path to a `.bib` file, then a one-sentence summary.

## Update an entry

The update dialog prompts for: table (`papers` / `bib` / `authors`) → column → current
identifier → new value → confirmation (`y`/`n`).

## Testing

```bash
uv run pytest                        # full suite (no personal database needed)
uv run pytest --cov=src/paper_sorts  # with per-layer coverage
```

Tests use an ephemeral PostgreSQL instance created automatically by `pytest-postgresql`.
No `database.crypt` or `key` file required.
