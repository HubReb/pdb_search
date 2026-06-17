# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Off-line paper-database searcher: a CLI that stores publication metadata (title, authors, summary, BibTeX) in a local PostgreSQL database and lets the user search/add/update entries. Personal-use tool — not a library or service.

## Commands

Dependencies are managed with **uv** (Python ≥ 3.11):

```bash
uv sync --all-extras                        # install runtime + dev deps
uv run pdbsearch                            # start the interactive CLI
uv run ruff check src tests                 # lint
uv run mypy src                             # type-check (strict on src/)
uv run pytest                               # run the suite (ephemeral PG via pytest-postgresql)
```

Subcommands: `pdbsearch search`, `pdbsearch add`, `pdbsearch update`, `pdbsearch delete`, `pdbsearch import`, `pdbsearch migrate`. `pdbsearch --help` lists them.

Configuration sources, priority order (highest first): CLI flags (`--database-url`, `--log-level`, `--config`, `--key`) → env (`PDBSEARCH_*`) → `.env` → Fernet-encrypted INI. See `specs/001-modernize-stack/quickstart.md`.

## Architecture

Modern src-layout package at `src/paper_sorts/`. Top-down:

1. **`cli/`** — Typer subcommands (`search`, `add`, `update`, `delete`, `migrate`, `importer`, plus `app.py` that wires them and drops into the four-option top-level menu when invoked with no subcommand; `migrate` and `import` are subcommand-only — admin/scripted operations, not part of the four-option menu). `cli/prompts.py` is the only module under `src/paper_sorts/` permitted to import `rich.prompt` (constitution Principle III).
2. **`services/`** — `paper_service.py` holds the high-level domain operations (`search_by_title`, `search_by_author`, `add_paper`, `update_field`, `delete_paper`); `import_service.py` exposes `extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]` for the per-paper bulk-import path. Pure orchestration; no SQL, no rich, no I/O. `update_field` uses `match`/`case` over a `Literal[...]` table arg with `assert_never(table)` for compile-time exhaustiveness.
3. **`db/`** — SQLAlchemy 2.x persistence. `db/session.py` exposes `with_session(...)` (commit on success, rollback on exception, deterministic close). `db/repositories.py` defines `PaperRepository` / `AuthorRepository` / `BibRepository` plus pydantic DTOs (`PaperSummary`, `PaperCreate`). `db/models.py` declares the four ORM models. **`db/` is the only place under `src/paper_sorts/` permitted to import `sqlalchemy`** — services depend on DTOs, never on ORM types.

Supporting modules:

- **`config.py`** — pydantic-settings `Settings` model with the four-source priority chain.
- **`logging_config.py`** — single `logging.config.dictConfig` (RichHandler to stdout, optional FileHandler). Called once from `cli/app.py` at startup.

`migrations/versions/` holds the Alembic schema migrations. Revision 0001 is the verbatim port of the original DDL; revision 0002 converges legacy database variants (the `bibtext_id` typo column) onto the canonical schema, idempotently.

For a reverse-engineered description of the *legacy* stack as it was before T026, see `docs/architecture.md`.

### Database schema

Four tables, defined declaratively in `src/paper_sorts/db/models.py` and managed via Alembic:

- `papers(id, title, contents, bibtex_id → bib.bibtex_id)`
- `bib(bibtex_id PK, bibtex UNIQUE)`
- `authors_id(id, author)`
- `authors_papers(id, author_id, paper_id)` — many-to-many link, **no DDL FKs**.

A paper is identified internally by `papers.id`; the BibTeX key (`bibtex_id`) is the user-facing unique identifier and is the FK target from `papers` into `bib`. Schema-preservation contract: do not add NOT NULL outside primary keys, do not add FKs to `authors_papers`, do not add indexes that the original DDL did not have.

## Tests

`uv run pytest` runs the suite against an ephemeral PostgreSQL spun up by `pytest-postgresql` from the host's `pg_ctl`. No personal database, no encrypted config, no key file required. Per constitution Principle II, persistence-layer tests run against a real DB — no mocking the SQLAlchemy session, repositories, or driver.

Session fixtures live in `tests/conftest.py` (`postgresql_proc`, `ephemeral_db_url`, `engine`, `seeded_engine`); the canonical seed dataset is `tests/fixtures/seed_papers.SEED_PAPERS`. The baseline benchmark harness lives under `tests/benchmarks/` (`bench_baseline.py`) and executes as part of the suite, recording `baseline.json` (constitution Principle IV gate). `tests/test_doc_currency.py` enforces the forbidden-legacy-token gate over `README.md`/`CLAUDE.md`.

## SpecKit

`.specify/` contains SpecKit templates and `memory/constitution.md`. The constitution defines four binding principles — Code Quality, Testing Standards, User Experience Consistency, Performance Requirements — plus three mechanical merge-blocking gates: per-layer 80% coverage, an executing baseline benchmark, and the doc-currency forbidden-token check. Read the constitution before generating a plan or making non-trivial changes.

**Feature `001-modernize-stack`**: rebuilds the legacy flat-layout `paper_sorts/` onto the modern stack (SQLAlchemy 2.x, Typer, Alembic, pydantic-settings, uv). See `specs/001-modernize-stack/` for the spec, plan, research, data model, contracts, and tasks.
