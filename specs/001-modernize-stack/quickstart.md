# Quickstart: Modernized Paper Sorts

**Feature**: 001-modernize-stack

---

## Install

```bash
# Install uv if not already present
curl -Ls https://astral.sh/uv/install.sh | sh

# Clone and install
git clone <repo>
cd paper_sorts
uv sync --all-extras
```

---

## Run (interactive)

```bash
uv run pdbsearch
```

Drops into the four-option menu. Type a number or `q` to quit.

## Run (subcommand)

```bash
uv run pdbsearch search         # interactive search
uv run pdbsearch add            # interactive add
uv run pdbsearch update         # interactive update
uv run pdbsearch delete         # interactive delete
uv run pdbsearch import --tex literature_overview.tex --bib bib.bib
uv run pdbsearch migrate        # apply Alembic migrations
```

---

## Configuration

Priority order (highest first):

1. **CLI flags**: `--database-url`, `--log-level`, `--config`, `--key`
2. **Environment variables**: `PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`
3. **`.env` file** in the current directory
4. **Fernet-encrypted INI** (via `--config` + `--key` flags)

### Using an encrypted config (same as before)

```bash
uv run pdbsearch --config ../../database.crypt --key ../../key
```

### Using environment variables

```bash
export PDBSEARCH_DATABASE_URL="postgresql+psycopg://user:pass@localhost/dbname"
uv run pdbsearch
```

---

## Run Tests

```bash
# Requires pg_ctl on PATH (PostgreSQL 18 at /usr/bin/pg_ctl)
uv run pytest
```

The test suite spins up an ephemeral PostgreSQL instance automatically. No personal database or credentials needed.

---

## Migrate an Existing Personal Database

```bash
# Point at your existing database, then run:
PDBSEARCH_DATABASE_URL="postgresql+psycopg://..." uv run pdbsearch migrate
```

This applies all Alembic migrations in order, including the legacy `bibtext_id` → `bibtex_id` rename if your database was created with the old `add.py` or `get_data.py` scripts. The migration is idempotent — safe to run multiple times.

---

## Quality Gates

```bash
uv run ruff check src tests      # lint
uv run ruff format --check src   # formatting
uv run mypy src                  # type-check
uv run pytest                    # test suite (includes integration + coverage gate)
```

All four must pass before merging.
