# Quickstart: Modernized pdbsearch

**Feature**: 001-modernize-stack  
**Date**: 2026-06-20

---

## Prerequisites

- Python >= 3.11
- PostgreSQL (any version with `pg_ctl` on PATH, for tests)
- uv (`pip install uv` or see https://docs.astral.sh/uv/getting-started/installation/)

---

## Installation

```bash
# Clone and install
git clone <repo>
cd pdb_search
uv sync --all-extras      # install runtime + dev deps
```

---

## Configuration

Priority order (highest first):

1. **CLI flags**: `--database-url`, `--log-level`, `--config`, `--key`
2. **Environment variables**: `PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`
3. **`.env` file** in the working directory
4. **Fernet-encrypted INI** (original config file format)

### Quickest setup (env var)

```bash
export PDBSEARCH_DATABASE_URL="postgresql+psycopg://user:pass@localhost/mydb"
uv run pdbsearch
```

### Using the encrypted config (existing users)

```bash
uv run pdbsearch --config ../../database.crypt --key ../../key
```

---

## Run the interactive CLI

```bash
uv run pdbsearch          # drops into 5-option menu
```

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) Delete an entry
5) (Q)uit
Your choice:
```

---

## Subcommands

```bash
uv run pdbsearch search                       # interactive search
uv run pdbsearch search --author "Smith, J"   # direct author search
uv run pdbsearch search --title "My Paper"    # direct title search
uv run pdbsearch add                          # interactive add
uv run pdbsearch add --bib-file paper.bib     # add from bib file
uv run pdbsearch update                       # interactive update
uv run pdbsearch delete                       # interactive delete
uv run pdbsearch migrate                      # run DB migrations (admin)
uv run pdbsearch import --tex lit.tex --bib lit.bib  # bulk import (admin)
```

---

## First-time Setup (fresh database)

```bash
# 1. Set connection URL
export PDBSEARCH_DATABASE_URL="postgresql+psycopg://user:pass@localhost/mydb"

# 2. Create the schema
uv run pdbsearch migrate

# 3. (Optional) Bulk import from existing LaTeX/BibTeX files
uv run pdbsearch import --tex literature_overview.tex --bib bib.bib

# 4. Start using the CLI
uv run pdbsearch
```

---

## Migrating from the legacy version

If you have a personal database from the old version:

```bash
# The migrate command detects and handles both legacy column-name variants:
# - bibtex_id  (database_connector.py era)
# - bibtext_id (get_data.py era, sic)
uv run pdbsearch migrate
```

Row counts (papers, authors, BibTeX entries, authorships) are preserved exactly.

---

## Development

```bash
uv run ruff check src tests   # lint
uv run ruff format --check src tests  # format check
uv run mypy src                # type-check (strict)
uv run pytest                  # full test suite (ephemeral PG, no personal DB needed)
uv run pytest -x -v            # stop on first failure, verbose
```

---

## Architecture Overview

```
CLI (Typer)          → cli/app.py, cli/*.py, cli/prompts.py
Services (domain)    → services/paper_service.py, services/import_service.py
Persistence (ORM)    → db/models.py, db/repositories.py, db/session.py
Configuration        → config.py (pydantic-settings, 4-source priority)
Migrations           → migrations/versions/
```

See `docs/architecture.md` for the full reverse-engineered architecture document.
