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

Configuration sources, priority order (highest first): CLI flags (`--database-url`, `--log-level`) → env (`PDBSEARCH_*`) → `.env` → Fernet-encrypted INI (`--config <path> --key <path>`). See `specs/001-modernize-stack/quickstart.md`.

## Architecture

Modern src-layout package at `src/paper_sorts/`. Top-down:

1. **`cli/`** — Typer subcommands (`search`, `add`, `update`, `delete`, `migrate`, `importer`, plus `app.py` that wires them and drops into the four-option top-level menu when invoked with no subcommand). `cli/prompts.py` is the only module under `src/paper_sorts/` permitted to import `rich.prompt` (constitution Principle III).
2. **`services/`** — `paper_service.py` holds the high-level domain operations (`search_by_title`, `search_by_author`, `add_paper`, `update_field`, `delete_paper`); `import_service.py` exposes `extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]` for the per-paper bulk-import path. Pure orchestration; no SQL, no rich, no I/O. `update_field` uses `match`/`case` over a `Literal[...]` table arg with `assert_never` for compile-time exhaustiveness.
3. **`db/`** — SQLAlchemy 2.x persistence. `db/session.py` exposes `with_session(...)` (commit on success, rollback on exception). `db/repositories.py` defines `PaperRepository` / `AuthorRepository` / `BibRepository` plus pydantic DTOs (`PaperSummary`, `PaperCreate`). `db/models.py` declares the four ORM models. **`db/` is the only place under `src/paper_sorts/` permitted to import `sqlalchemy`** — services depend on DTOs, never on ORM types.

Supporting modules:

- **`config.py`** — pydantic-settings `Settings` model with the four-source priority chain.
- **`logging_config.py`** — single `logging.config.dictConfig` (RichHandler to stdout, optional FileHandler). Called once from `cli/app.py` at startup.

`migrations/versions/` holds the Alembic schema migrations. Revision 001 is the verbatim port of the original DDL; Revision 002 handles the legacy `bibtext_id` typo column rename.

### Database schema

Four tables, defined declaratively in `src/paper_sorts/db/models.py` and managed via Alembic:

- `papers(id, title, contents, bibtex_id → bib.bibtex_id)`
- `bib(bibtex_id PK, bibtex UNIQUE)`
- `authors_id(id, author)`
- `authors_papers(id, author_id, paper_id)` — many-to-many link, **no DDL FKs**.

A paper is identified internally by `papers.id`; the BibTeX key (`bibtex_id`) is the user-facing unique identifier and is the FK target from `papers` into `bib`. Schema-preservation contract: do not add NOT NULL outside primary keys, do not add FKs to `authors_papers`, do not add indexes that the original DDL did not have.

## Tests

`uv run pytest` runs the suite against an ephemeral PostgreSQL spun up by `pytest-postgresql` from the host's `pg_ctl`. No personal database, no `database.crypt`, no `key` file required.

Session fixtures live in `tests/conftest.py` (`postgresql_proc`, `ephemeral_db_url`); the canonical seed dataset is `tests/fixtures/seed_papers.py`.

## SpecKit

<!-- SPECKIT START -->
Active feature: `001-modernize-stack`. Implementation plan: `specs/001-modernize-stack/plan.md`. Constitution: `.specify/memory/constitution.md` (v1.3.0-b2-hardened). Read the plan and constitution before making non-trivial changes.
<!-- SPECKIT END -->
