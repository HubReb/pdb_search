# Off-line Paper Database Searcher

A small, bare-bones CLI for storing and querying publication metadata in a local
PostgreSQL database — useful when no online connection is available, e.g. when
traveling by train.

The database can be searched by author or title. A matching entry returns:

- paper title
- authors
- short summary
- BibTeX entry

*This application is for personal use only.*

---

## Installation

Dependencies are managed with **uv** (Python ≥ 3.11):

```bash
uv sync --all-extras
```

---

## Running

```bash
# Interactive menu (recommended)
uv run pdbsearch

# With explicit database URL
uv run pdbsearch --database-url postgresql+psycopg://user:pass@host/dbname

# With encrypted config file
uv run pdbsearch --config /path/to/database.crypt --key /path/to/key
```

### Configuration priority

1. CLI flags (`--database-url`, `--log-level`)
2. Environment variables (`PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`)
3. `.env` file
4. Fernet-encrypted INI config (via `--config` + `--key`)

The encrypted INI file must have a `[postgresql]` section:

```ini
[postgresql]
host     = localhost
port     = 5432
dbname   = your_dbname
user     = your_dbuser
password = your_password
```

---

## Subcommands

| Command | Description |
|---------|-------------|
| `pdbsearch search` | Interactive search by author or title |
| `pdbsearch add` | Add a new paper interactively |
| `pdbsearch update` | Update a paper field |
| `pdbsearch delete` | Delete a paper |
| `pdbsearch migrate` | Upgrade the database schema (admin) |
| `pdbsearch import --tex FILE --bib FILE` | Bulk-import from .tex + .bib (admin) |

When invoked with no subcommand, `pdbsearch` enters the interactive top-level
menu:

```
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
```

---

## Database schema migration

To upgrade an existing personal database:

```bash
uv run pdbsearch --database-url <DSN> migrate
```

This runs Alembic migrations to head, handling both the current (`bibtex_id`)
and legacy (`bibtext_id` — typo variant) column names.

---

## Development

```bash
uv run ruff check src tests   # lint
uv run mypy src               # type-check
uv run pytest                 # test suite (ephemeral PostgreSQL, no personal DB needed)
uv run pytest tests/benchmarks/  # performance baseline
```
