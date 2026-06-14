# Quickstart: Modernize the Stack

**Feature**: 001-modernize-stack  
**Date**: 2026-06-14

## Prerequisites

- Python ≥ 3.11
- PostgreSQL (local instance; `pg_ctl` on PATH for tests)
- uv: `curl -Ls https://astral.sh/uv/install.sh | sh`

## Installation

```bash
git clone <repo>
cd pdb_search
uv sync --all-extras
```

## Configuration

Choose one configuration source (highest priority wins):

### 1. Environment variable (simplest)

```bash
export PDBSEARCH_DATABASE_URL="postgresql+psycopg://user:password@localhost/mydb"
uv run pdbsearch
```

### 2. .env file

```bash
echo 'PDBSEARCH_DATABASE_URL=postgresql+psycopg://user:password@localhost/mydb' > .env
uv run pdbsearch
```

### 3. Fernet-encrypted config (legacy; keeps existing `database.crypt` + `key`)

```bash
uv run pdbsearch --config ../../database.crypt --key ../../key
```

## First Run: Migrate the Database

On a fresh database or after upgrading from the legacy stack:

```bash
uv run pdbsearch migrate
```

This is idempotent — safe to run multiple times.

## Interactive Use

```bash
uv run pdbsearch        # drops into four-option menu
```

## Subcommands (scripted / non-interactive)

```bash
uv run pdbsearch search
uv run pdbsearch add
uv run pdbsearch update [--id PAPER_ID]
uv run pdbsearch delete [--id PAPER_ID]
uv run pdbsearch import --tex literature_overview.tex --bib refs.bib
uv run pdbsearch migrate
```

## Running Tests

```bash
uv run pytest           # full suite (requires pg_ctl on PATH)
```

## Running the Benchmark

```bash
uv run pytest tests/benchmarks/ -m benchmark -v
```

## Linting and Type-Checking

```bash
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy src
```
