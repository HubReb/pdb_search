# Quickstart: Modernized Paper Sorts

## Install

```bash
uv sync --all-extras          # runtime + dev deps (Python ≥ 3.11)
```

## Run the CLI

```bash
uv run pdbsearch              # interactive four-option menu (Search / Add / Update / Quit)
uv run pdbsearch --help       # list subcommands
uv run pdbsearch search       # interactive search (by author / by title)
uv run pdbsearch add
uv run pdbsearch update
uv run pdbsearch delete
uv run pdbsearch import --tex literature_overview.tex --bib bib.bib
uv run pdbsearch migrate      # upgrade a personal DB to the modern schema (idempotent)
```

## Configuration (priority: CLI > env > .env > Fernet INI)

```bash
# Highest: explicit flag
uv run pdbsearch --database-url postgresql+psycopg://user:pass@localhost/paper_sorts search

# Env
export PDBSEARCH_DATABASE_URL=postgresql+psycopg://user:pass@localhost/paper_sorts

# .env file (same keys)

# Lowest: legacy Fernet-encrypted INI (preserved)
uv run pdbsearch --config ../../database.crypt --key ../../key search
```

A lost/missing key file produces a clear, actionable error — not a stack trace.

## Migrate an existing personal database

```bash
uv run pdbsearch migrate
```
Converges either historical schema (`bibtex_id` or legacy-typo `bibtext_id`) to canonical, in one
action, with zero data loss. Re-running it is a no-op.

## Quality gates

```bash
uv run ruff check .            # lint
uv run ruff format --check .   # format
uv run mypy src                # types (strict on src/)
uv run pytest                  # ephemeral PG via pytest-postgresql; no personal DB needed
uv run pytest --cov=src/paper_sorts --cov-report=term-missing   # whole-package coverage
# per-layer (gate G1 — each layer independently ≥80%):
uv run pytest --cov=src/paper_sorts/db --cov-report=term
uv run pytest --cov=src/paper_sorts/services --cov-report=term
uv run pytest --cov=src/paper_sorts/cli --cov-report=term
uv run pytest --cov=src/paper_sorts/config.py --cov-report=term
```

The suite spins up an ephemeral PostgreSQL from the host's `pg_ctl`; no `database.crypt`/`key` and no
personal database are required on a fresh checkout.
