# Quickstart: Modernized `pdbsearch`

**Feature**: 001-modernize-stack
**Audience**: A developer or end-user setting up the modernized stack on a fresh machine. This is the acceptance reference for spec User Story 3 (reproducible test suite without developer-local state).

## Prerequisites

- Python ≥ 3.11
- PostgreSQL ≥ 13 (`psql`, `pg_ctl`, and `initdb` available on `PATH`)
- uv ≥ 0.5 (https://docs.astral.sh/uv/)

> Docker is **not** required. The test suite uses `pytest-postgresql`, which spins up Postgres from the host's binary.

## Install

```bash
git clone <repo>
cd pdb_search
git checkout 001-modernize-stack    # or main, after merge
uv sync --all-extras
```

## Configure

Three configuration sources are supported in priority order: CLI flags → env vars → `.env` file → Fernet-encrypted INI. Pick whichever fits the situation.

### Quickest: env var

```bash
export PDBSEARCH_DATABASE_URL="postgresql+psycopg://user:pass@localhost:5432/papers"
```

### `.env` file (recommended for local dev)

Create `.env` in the project root:

```env
PDBSEARCH_DATABASE_URL=postgresql+psycopg://user:pass@localhost:5432/papers
PDBSEARCH_LOG_LEVEL=INFO
```

### Encrypted INI (preserved from current setup; see spec FR-007)

Same Fernet-encrypted INI file as before — the modernized config layer reads it via a custom pydantic-settings source.

```bash
pdbsearch --config /path/to/database.crypt --key /path/to/key search ...
```

## Migrate

First-time setup of the schema, *or* upgrade an existing personal database (including the legacy `bibtext_id` schema):

```bash
uv run pdbsearch migrate
```

Output on a fresh DB:

```text
Running Alembic upgrade...
  001 initial_schema  ✓
  002 legacy_bibtext_to_bibtex  ✓
Tables: papers=0, authors_id=0, bib=0, authors_papers=0
Schema is at head (002).
```

Output on an already-modern DB:

```text
Schema is at head (002). No migrations to apply.
```

Output on a legacy `bibtext_id` DB:

```text
Running Alembic upgrade...
  002 legacy_bibtext_to_bibtex  ✓
Tables: papers=412, authors_id=287, bib=412, authors_papers=1031
Schema is at head (002).
```

## First search

```bash
uv run pdbsearch
```

Drops into the top-level menu. Select `1) Search the database`, then `2) Search by paper title`, type a title fragment, and the system prints the title / authors / summary / BibTeX entry as before.

For non-interactive use:

```bash
uv run pdbsearch search --by author --query "Pino, J."
uv run pdbsearch search --by title --query "Direct speech-to-speech translation with discrete units"
```

## Add an entry

Interactive (drops into the same step-by-step prompt as before):

```bash
uv run pdbsearch add
```

From a `.bib` file directly:

```bash
uv run pdbsearch add --bib-file paper.bib --summary "one-sentence summary of the paper"
```

## Bulk import (preserves the current `get_data.py` behaviour)

```bash
uv run pdbsearch import literature_overview.tex bib.bib
```

## Run the tests

```bash
uv run pytest
```

This is the SC-003 acceptance command. It:

1. Spins up an ephemeral Postgres instance via `pytest-postgresql`.
2. Runs Alembic migrations against the ephemeral DB.
3. Loads `tests/fixtures/seed_papers.py` so each integration test has known rows to assert on.
4. Runs unit + integration tests.
5. Tears the ephemeral DB down on exit.

There is no dependency on a personal `database.crypt` or `key` file. A fresh clone, with no prior project state, runs the full suite green.

## Lint and format

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src/
```

These are the SC-007 / Principle I gates. CI runs all three.

## Run the baseline benchmark (for SC-006)

The baseline timings are captured **before** any modernization commits land, against the existing implementation, on the same fixture that the modernized integration tests use:

```bash
uv run pytest tests/benchmarks/bench_baseline.py --baseline-record
```

After modernization, re-run with `--baseline-compare` to verify non-regression:

```bash
uv run pytest tests/benchmarks/bench_baseline.py --baseline-compare
```

A regression of more than the configured tolerance (default 10 % wall-clock per operation) fails the build and the gate.

## Roll back

To roll the schema back to revision 001 (e.g. for testing):

```bash
uv run alembic downgrade 001
```

Note: revision 002's `downgrade()` is intentionally `NotImplementedError` — once a database is converged off the legacy `bibtext_id` column, going back is not a supported operation.

## Troubleshooting

- **`PDBSEARCH_DATABASE_URL is required`** — none of the four config sources supplied a URL. Set the env var or pass `--database-url`.
- **`Fernet config requires a key file`** — `--config` was set without `--key` (or `PDBSEARCH_FERNET_CONFIG` without `PDBSEARCH_FERNET_KEY`).
- **`pg_ctl: not found` during `pytest`** — install PostgreSQL on the host. pytest-postgresql uses the host's binary; it does not vendor its own.
- **Migration fails on a legacy DB** — check `pdbsearch_migrate.log`; re-running `pdbsearch migrate` converges. The transaction wrapper means the database is either fully on the previous revision or fully on the new one.
