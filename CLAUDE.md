# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Off-line paper-database searcher: a CLI that stores publication metadata (title, authors, summary, BibTeX) in a local PostgreSQL database and lets the user search/add/update entries. Personal-use tool — not a library or service.

## Commands

Dependencies are managed with uv (Python >= 3.11):

```bash
uv sync --all-extras                        # install runtime + dev deps
uv run pdbsearch                            # start the interactive CLI (four-option menu)
uv run pdbsearch --help                     # list subcommands
uv run ruff check src tests                 # lint
uv run ruff format --check src tests        # format check
uv run mypy src                             # type-check (strict on src/)
uv run pytest                               # run the suite (ephemeral PG via pytest-postgresql)
```

Subcommands: `pdbsearch search`, `pdbsearch add`, `pdbsearch update`, `pdbsearch delete`, `pdbsearch import`, `pdbsearch migrate`.

Configuration sources, priority order (highest first): CLI flags (`--database-url`, `--log-level`) -> env (`PDBSEARCH_*`) -> `.env` -> Fernet-encrypted INI (`--config <path> --key <path>`). See `specs/001-modernize-stack/quickstart.md`.

## Architecture

Modern src-layout package at `src/paper_sorts/`. Top-down:

1. **`cli/`** — Typer subcommands (`search`, `add`, `update`, `delete`, `migrate`, `importer`, plus `app.py` that wires them and drops into the four-option top-level menu when invoked with no subcommand; `migrate` and `import` are subcommand-only and deliberately absent from the menu — admin/scripted operations). `cli/prompts.py` is the only module under `src/paper_sorts/` permitted to import `rich.prompt`.
2. **`services/`** — `paper_service.py` holds the high-level domain operations (`search_by_title`, `search_by_author`, `add_paper`, `update_field`, `delete_paper`); `import_service.py` exposes `extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]` for the per-paper bulk-import path. Pure orchestration; no SQL, no rich, no I/O. `update_field` uses `match`/`case` over a `Literal[...]` table arg with `assert_never(table)` for compile-time exhaustiveness.
3. **`db/`** — SQLAlchemy 2.x persistence. `db/session.py` exposes `with_session(...)` (commit on success, rollback on exception). `db/repositories.py` defines `PaperRepository` / `AuthorRepository` / `BibRepository` plus pydantic DTOs (`PaperSummary`, `PaperCreate`). `db/models.py` declares the four ORM models. **`db/` is the only place under `src/paper_sorts/` permitted to import `sqlalchemy`** — services depend on DTOs, never on ORM types.

Supporting modules:

- **`config.py`** — pydantic-settings `Settings` model with the four-source priority chain.
- **`logging_config.py`** — single `logging.config.dictConfig` (RichHandler to stdout, optional FileHandler). Called once from `cli/app.py` at startup.

`migrations/versions/` holds the Alembic schema migrations. Revision 001 is the verbatim port of the original DDL; revision 002 converges legacy database variants (e.g. the `bibtext_id` typo column) onto the canonical schema.

For a reverse-engineered description of the legacy stack as it was before this modernization, see `docs/architecture.md`.

### Database schema

Four tables, defined declaratively in `src/paper_sorts/db/models.py` and managed via Alembic:

- `papers(id, title, contents, bibtex_id -> bib.bibtex_id)`
- `bib(bibtex_id PK, bibtex UNIQUE)`
- `authors_id(id, author)`
- `authors_papers(id, author_id, paper_id)` — many-to-many link, **no DDL FKs**.

A paper is identified internally by `papers.id`; the BibTeX key (`bibtex_id`) is the user-facing unique identifier and is the FK target from `papers` into `bib`. Schema-preservation contract: do not add NOT NULL outside primary keys, do not add FKs to `authors_papers`, do not add indexes that the original DDL did not have.

## Tests

`uv run pytest` runs the suite against an ephemeral PostgreSQL spun up by `pytest-postgresql` from the host's `pg_ctl`. No personal database, no `database.crypt`, no `key` file required. Per the constitution, persistence-layer tests run against a real DB — no mocking the SQLAlchemy session, repositories, or driver.

Session fixtures live in `tests/conftest.py` (`postgresql_proc`, `ephemeral_db_url`, `migrated_engine`, `seeded_engine`); the canonical seed dataset is `tests/fixtures/seed_papers.SEED_PAPERS`. The baseline benchmark harness lives under `tests/benchmarks/` (`bench_baseline.py`, recorded in `tests/benchmarks/baseline.json`) and executes as part of the suite.

## SpecKit

`.specify/` contains SpecKit templates and `memory/constitution.md`. The constitution defines four binding principles — Code Quality, Testing Standards, User Experience Consistency, Performance Requirements — and three mechanical merge-blocking gates: per-layer coverage (each of `db/`, `services/`, `cli/`, `config.py` >= 80%), an executing baseline benchmark, and a doc-currency forbidden-token scan over `README.md`/`CLAUDE.md`. The performance principle is framed as "no measurable regression vs. the current baseline" rather than absolute numbers. Read the constitution before generating a plan or making non-trivial changes.

**Feature**: `specs/001-modernize-stack/` — the modernization of the legacy flat-layout stack onto SQLAlchemy 2.x + Typer + Alembic + pydantic-settings + uv + a real-DB pytest suite. See [plan.md](specs/001-modernize-stack/plan.md), [research.md](specs/001-modernize-stack/research.md), [data-model.md](specs/001-modernize-stack/data-model.md), [contracts/](specs/001-modernize-stack/contracts/), and [quickstart.md](specs/001-modernize-stack/quickstart.md).
