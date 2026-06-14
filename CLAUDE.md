# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Off-line paper-database searcher: a CLI that stores publication metadata (title, authors, summary, BibTeX) in a local PostgreSQL database and lets the user search/add/update entries. Personal-use tool — not a library or service.

## Commands

Dependencies are managed with **uv** (Python >= 3.11):

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

1. **`cli/`** — Typer subcommands (`search`, `add`, `update`, `delete`, `migrate`, `importer`, plus `app.py` that wires them and drops into the four-option top-level menu when invoked with no subcommand). `cli/prompts.py` is the only module permitted to call `rich.prompt` or bare `input()`.
2. **`services/`** — `paper_service.py` holds the high-level domain operations (`search_by_title`, `search_by_author`, `add_paper`, `update_field`, `delete_paper`); `import_service.py` exposes `extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]` for bulk import. Pure orchestration; no SQL, no rich, no I/O.
3. **`db/`** — SQLAlchemy 2.x persistence. `db/session.py` exposes `with_session(...)` (commit on success, rollback on exception). `db/repositories.py` defines `PaperRepository` / `AuthorRepository` / `BibRepository` plus pydantic DTOs (`PaperSummary`, `PaperCreate`). `db/models.py` declares the four ORM models. **`db/` is the only place permitted to import `sqlalchemy`** — services depend on DTOs, never on ORM types.

Supporting modules:

- **`config.py`** — pydantic-settings `Settings` model with the four-source priority chain.
- **`logging_config.py`** — single `logging.config.dictConfig` (RichHandler to stdout, optional FileHandler). Called once from `cli/app.py` at startup.

`migrations/versions/` holds the Alembic schema migrations. Revision 001 is the verbatim port of the original DDL; Revision 002 handles legacy schema variants (the `bibtext_id` typo column).

For a reverse-engineered description of the legacy stack (useful when reading historical commits), see `docs/architecture.md`.

### Database schema

Four tables, defined declaratively in `src/paper_sorts/db/models.py` and managed via Alembic:

- `papers(id, title, contents, bibtex_id → bib.bibtex_id)`
- `bib(bibtex_id PK, bibtex UNIQUE)`
- `authors_id(id, author)`
- `authors_papers(id, author_id, paper_id)` — many-to-many link, no DDL FKs.

A paper is identified internally by `papers.id`; the BibTeX key (`bibtex_id`) is the user-facing unique identifier and is the FK target from `papers` into `bib`.

## Tests

`uv run pytest` runs the suite against an ephemeral PostgreSQL spun up by `pytest-postgresql` from the host's `pg_ctl`. No personal database or credential files are required.

Session fixtures live in `tests/conftest.py`; the canonical seed dataset is `tests/fixtures/seed_papers.py`. Benchmark harness lives under `tests/benchmarks/`.

## SpecKit

`.specify/` contains SpecKit templates and `memory/constitution.md` (ratified 2026-04-26, current v1.3.0-b2-hardened). The constitution defines four binding principles — Code Quality, Testing Standards, User Experience Consistency, Performance Requirements.

**Active feature**: `specs/001-modernize-stack/` — full modernization of the stack. See [plan.md](specs/001-modernize-stack/plan.md) for the implementation plan.
