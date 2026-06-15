# Off-line Paper Database searcher

A small, single-user CLI for storing publication metadata in a local PostgreSQL database and
querying it offline — for when no online resource is reachable (e.g. traveling by train).

A search by author or title returns:

- paper title
- authors
- a short summary
- the BibTeX entry

This is a personal-use tool. If something goes wrong, consult the logs.

## Installation

Dependencies are managed with [uv](https://docs.astral.sh/uv/) (Python >= 3.11):

```bash
uv sync --all-extras
```

## Configuration

The database connection is resolved from four sources, highest priority first:

1. CLI flags: `--database-url postgresql+psycopg://user:pw@host:5432/dbname`
2. environment: `PDBSEARCH_DATABASE_URL=...`
3. a `.env` file in the working directory
4. a Fernet-encrypted INI file: `--config ../../database.crypt --key ../../key`

The encrypted INI keeps the familiar `[postgresql]` section:

```ini
[postgresql]
dbname=your_dbname
user=your_dbuser
password=your_dbuser_password
host=localhost
port=5432
```

Keep encrypted config files and keys out of the repository.

## Usage

```bash
uv run pdbsearch migrate                 # create / upgrade the schema (idempotent)
uv run pdbsearch                         # interactive menu (search / add / update / quit)
uv run pdbsearch search                  # search by author or title
uv run pdbsearch add                     # add an entry (inline or from a .bib file)
uv run pdbsearch update                  # update title / contents / bibtex / author
uv run pdbsearch delete                  # delete an entry (with confirmation)
uv run pdbsearch import --tex lit.tex --bib refs.bib   # bulk import
uv run pdbsearch --help                  # list subcommands
```

Running `pdbsearch` with no subcommand drops into the four-option top-level menu (search / add /
update / quit). `migrate` and `import` are subcommand-only admin operations and are not in the
menu.

### Migrating an existing database

```bash
uv run pdbsearch migrate
```

Converges either historical schema (`bibtex_id`, or the legacy `bibtext_id` typo) onto the
canonical schema in one action, with zero data loss. It is idempotent — safe to rerun.

## Development

```bash
uv run ruff check src tests              # lint
uv run ruff format --check src tests     # format check
uv run mypy src                          # type-check (strict on src/)
uv run pytest                            # real-DB suite (ephemeral PG via pytest-postgresql)
uv run pytest --cov=src/paper_sorts      # with coverage
```

The test suite provisions an ephemeral PostgreSQL from the host `pg_ctl` — no personal
database, `database.crypt`, or `key` file is required.

## Architecture

A reverse-engineered description of the legacy stack lives in
[`docs/architecture.md`](docs/architecture.md). The modernized layering and design decisions are
documented under [`specs/001-modernize-stack/`](specs/001-modernize-stack/).
