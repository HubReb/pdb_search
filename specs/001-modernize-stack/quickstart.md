# Quickstart: Modernized paper_sorts

## Install

Dependencies are managed with **uv** (Python ≥ 3.11):

```bash
uv sync --all-extras      # runtime + dev deps
```

## Run the CLI

```bash
uv run pdbsearch                 # interactive top-level menu (Search/Add/Update/Quit)
uv run pdbsearch search          # search by author or title
uv run pdbsearch add             # add a paper (inline or --bib-file)
uv run pdbsearch update          # update title / contents / bibtex / author
uv run pdbsearch delete          # delete a paper (with confirmation)
uv run pdbsearch import --tex literature_overview.tex --bib bib.bib
uv run pdbsearch migrate         # converge a legacy DB onto the canonical schema
uv run pdbsearch --help          # list subcommands
```

## Configuration (priority order, highest first)

1. **CLI flags**: `--database-url postgresql+psycopg://user:pw@host:port/db`,
   `--log-level INFO`, `--log-file pdbsearch.log`.
2. **Environment**: `PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`, …
3. **`.env`** file in the working directory.
4. **Fernet-encrypted INI**: `--config ../../database.crypt --key ../../key`
   (the legacy encrypted-config workflow, preserved as one supported source).

A lost or wrong key produces a clear, actionable error — never a stack trace.

## Develop

```bash
uv run ruff check .          # lint
uv run ruff format --check . # format check
uv run mypy src              # type-check (strict on src/)
uv run pytest                # suite against ephemeral PostgreSQL
uv run pytest --cov=paper_sorts --cov-report=term-missing
```

The test suite spins up an ephemeral PostgreSQL via `pytest-postgresql` from the
host's `pg_ctl` — **no personal database, no `database.crypt`, no `key` file
required**. Seed data is `tests/fixtures/seed_papers.SEED_PAPERS`.

## Migrating an existing personal database

```bash
uv run pdbsearch migrate            # or: uv run alembic upgrade head
```

This upgrades a database in either historical schema (`bibtex_id`, or the legacy
`bibtext_id` typo) to the canonical schema in one action, idempotently, with
zero data loss. Row counts (papers, authors, authorships, bib entries) match
exactly before and after.

## What's new vs. the legacy tool

- src-layout `src/paper_sorts/` package: `cli/` (Typer) → `services/` →
  `db/` (SQLAlchemy 2.x) + `config.py` + `logging_config.py`.
- Versioned Alembic migrations replace the runtime `create_tables()` call.
- pydantic-settings config adds `.env`/env-var support alongside the encrypted
  INI.
- `pytest` + `pytest-postgresql` replace the `unittest` suite that depended on a
  developer-local database.
- `ruff` replaces `pylint`.

Same prompts, same outputs, same data — just a mainstream stack underneath.
