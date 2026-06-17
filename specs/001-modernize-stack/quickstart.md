# Quickstart: Modernized paper_sorts

Personal, offline CLI to store and search publication metadata (title, authors,
summary, BibTeX) in a local PostgreSQL database.

## Install

Dependencies are managed with **uv** (Python ≥ 3.11):

```bash
uv sync --all-extras        # runtime + dev deps
```

## Run

```bash
uv run pdbsearch            # interactive top-level menu (Search/Add/Update/Quit)
uv run pdbsearch --help     # list subcommands
uv run pdbsearch search     # search by author or title
uv run pdbsearch add        # add an entry (inline or from a .bib file)
uv run pdbsearch update     # update title / contents / bibtex / author
uv run pdbsearch delete     # delete an entry
uv run pdbsearch import --tex lit.tex --bib refs.bib   # bulk import
uv run pdbsearch migrate    # upgrade an existing personal DB to the new schema
```

## Configure

Config sources, priority order (highest first):

1. CLI flags: `--database-url`, `--log-level`, `--config`, `--key`
2. Environment: `PDBSEARCH_*` (e.g. `PDBSEARCH_DATABASE_URL`)
3. `.env` file
4. Fernet-encrypted INI file: `--config <path> --key <path>`

Example:

```bash
uv run pdbsearch --database-url postgresql+psycopg://user:pw@localhost/papers search
# or
export PDBSEARCH_DATABASE_URL=postgresql+psycopg://user:pw@localhost/papers
uv run pdbsearch search
```

The encrypted-INI workflow from the previous version still works as the
lowest-priority source. A missing key file gives a clear error, not a traceback.

## Develop

```bash
uv run ruff check src tests        # lint
uv run ruff format --check src     # format check
uv run mypy src                    # type-check
uv run pytest                      # suite (ephemeral PG via pytest-postgresql)
uv run pytest tests/benchmarks     # baseline benchmark (records/checks baseline.json)
```

Tests need no personal database or credentials — `pytest-postgresql` spins up an
ephemeral PostgreSQL from the host `pg_ctl` and the suite seeds it from
`tests/fixtures/seed_papers.SEED_PAPERS`.

## Migrate an existing database

If you already have a personal DB from the previous version (either the
`bibtex_id` schema or the older `bibtext_id` typo schema), run:

```bash
uv run pdbsearch migrate
```

It is idempotent and preserves every row — paper, author, authorship, and
BibTeX counts are unchanged.
