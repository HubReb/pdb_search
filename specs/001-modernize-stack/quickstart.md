# Quickstart: Modernized paper_sorts

## Install

```bash
uv sync --all-extras          # runtime + dev deps (Python ≥ 3.11)
```

## Run the CLI

```bash
uv run pdbsearch              # bare → interactive four-option menu
uv run pdbsearch search      # search by author/title
uv run pdbsearch add         # add an entry (inline or from a .bib file)
uv run pdbsearch update      # update title/contents/bibtex/author
uv run pdbsearch delete      # delete an entry
uv run pdbsearch import --tex lit.tex --bib refs.bib   # bulk import
uv run pdbsearch migrate     # apply Alembic migrations / converge legacy schema
uv run pdbsearch --help      # list subcommands
```

## Configuration (priority: highest first)

1. CLI flags: `--database-url postgresql+psycopg://user:pass@host:5432/db`, `--log-level`, `--config`, `--key`.
2. Environment: `PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`, …
3. `.env` file in the working directory.
4. Fernet-encrypted INI: `--config database.crypt --key key` (legacy workflow preserved). A missing key file yields a clear, actionable error — never a stack trace.

## Migrate an existing personal database (US4)

```bash
uv run pdbsearch migrate --config ../../database.crypt --key ../../key
```

Upgrades a database in either historical schema (`bibtex_id` or the legacy `bibtext_id` typo) to the canonical schema with zero data loss. Idempotent — safe to rerun.

## Quality gates

```bash
uv run ruff check .          # lint
uv run ruff format --check . # format check
uv run mypy src              # type-check
uv run pytest                # real-DB suite (ephemeral PG via pytest-postgresql)
uv run pytest --cov=src/paper_sorts --cov-report=term-missing   # per-layer coverage
```

The test suite needs no personal database, no `database.crypt`, no `key` — `pytest-postgresql` spins an ephemeral PostgreSQL from the host `pg_ctl`.

## What's new vs. the legacy version

- One installable command `pdbsearch` with subcommands, replacing `python paper_sorts/run.py`.
- ORM + Alembic migrations replace hand-written SQL and lazy `create_tables()`.
- `.env` / environment-variable config alongside the encrypted INI.
- Single structured logger (Rich stdout + optional file) replacing per-class `*.log` files.
- A reproducible, ephemeral-DB test suite — runs on a fresh checkout.
