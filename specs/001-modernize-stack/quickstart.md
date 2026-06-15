# Quickstart: Modernized `pdbsearch`

## Install

Dependencies are managed with **uv** (Python ≥ 3.11):

```bash
uv sync --all-extras        # install runtime + dev deps
```

## Run

```bash
uv run pdbsearch            # interactive four-option menu
uv run pdbsearch search     # search by author / title
uv run pdbsearch add        # add an entry (inline or from a .bib file)
uv run pdbsearch update     # update title / contents / bibtex / author
uv run pdbsearch delete     # delete an entry
uv run pdbsearch import --tex lit.tex --bib refs.bib   # bulk import
uv run pdbsearch migrate    # upgrade an existing personal DB to canonical schema
uv run pdbsearch --help     # list commands
```

## Configuration (priority high → low)

1. CLI flags: `--database-url`, `--config`, `--key`, `--log-level`
2. Environment: `PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`, …
3. `.env` file in the working directory
4. Fernet-encrypted INI: `--config <path> --key <path>`

Example (env):
```bash
export PDBSEARCH_DATABASE_URL="postgresql+psycopg://user:pass@localhost:5432/papers"
uv run pdbsearch search
```

Example (encrypted config, the legacy workflow, still supported):
```bash
uv run pdbsearch --config ../../database.crypt --key ../../key search
```

A missing key or missing encrypted-config file produces a clear, actionable
message — not a stack trace.

## First-time database setup

```bash
uv run pdbsearch migrate    # creates the schema from scratch on an empty DB,
                            # or converges a legacy (bibtext_id) DB onto canonical
```

## Quality gates

```bash
uv run ruff check .         # lint
uv run ruff format --check .# format check
uv run mypy src             # strict type-check on src/
uv run pytest               # real-DB suite via ephemeral PostgreSQL
uv run pytest --cov=src/paper_sorts/db   # coverage for the persistence layer (≥80%)
```

The test suite spins up an ephemeral PostgreSQL via `pytest-postgresql` off the
host's `pg_ctl`. No personal database, no `database.crypt`, no `key` file
required — a fresh clone runs the suite green.

## What's new vs. the legacy tool

- One `pdbsearch` entry point with subcommands (was several standalone
  scripts + an argparse loop).
- SQLAlchemy 2.x ORM + repositories instead of hand-written SQL strings.
- Alembic migrations instead of a runtime `create_tables()`.
- pydantic-settings four-source config (env + `.env` added alongside the
  preserved Fernet workflow).
- One structured logger (RichHandler to stdout, optional file) instead of
  per-class `*.log` files.
- A real-DB pytest suite that runs on a fresh checkout.

All user-facing operations, prompts, and outputs match the legacy tool (see
[contracts/cli-commands.md](contracts/cli-commands.md)).
