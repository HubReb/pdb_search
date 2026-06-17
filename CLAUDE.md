# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

## Project

Off-line paper-database searcher: a CLI that stores publication metadata (title,
authors, summary, BibTeX) in a local PostgreSQL database and lets the user
search/add/update/delete entries. Personal-use tool — not a library or service.

## Commands

Dependencies are managed with **uv** (Python ≥ 3.11):

```bash
uv sync --all-extras                        # install runtime + dev deps
uv run pdbsearch                            # start the interactive CLI
uv run ruff check src tests                 # lint
uv run ruff format --check src tests        # format check
uv run mypy src                             # type-check (strict on src/)
uv run pytest                               # run the suite (ephemeral PG via pytest-postgresql)
```

Subcommands: `pdbsearch search`, `pdbsearch add`, `pdbsearch update`,
`pdbsearch delete`, `pdbsearch import`, `pdbsearch migrate`. `pdbsearch --help`
lists them. Invoked with no subcommand, `pdbsearch` drops into the four-option
top-level menu (Search / Add / Update / Quit); `import` and `migrate` are
admin/scripted subcommands and are deliberately absent from that menu.

Configuration sources, priority order (highest first): CLI flags
(`--database-url`, `--log-level`) → env (`PDBSEARCH_*`) → `.env` →
Fernet-encrypted INI (`--config <path> --key <path>`). See
`specs/001-modernize-stack/quickstart.md`.

## Architecture

Modern src-layout package at `src/paper_sorts/`. Top-down:

1. **`cli/`** — Typer subcommands (`search`, `add`, `update`, `delete`,
   `migrate`, `importer`, plus `app.py` that wires them and drops into the
   four-option top-level menu when invoked with no subcommand). `cli/prompts.py`
   is the only module under `src/paper_sorts/` permitted to import `rich.prompt`.
2. **`services/`** — `paper_service.py` holds the high-level domain operations
   (`search_by_title`, `search_by_author`, `add_paper`, `update_field`,
   `delete_paper`); `import_service.py` exposes
   `extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]` for the
   per-paper bulk-import path. Pure orchestration; no SQL, no rich, no I/O.
   `update_field` uses `match`/`case` over a `Literal[...]` table argument with
   `assert_never(table)` for compile-time exhaustiveness.
3. **`db/`** — SQLAlchemy 2.x persistence. `db/session.py` exposes
   `with_session(...)` (commit on success, rollback on exception).
   `db/repositories.py` defines `PaperRepository` / `AuthorRepository` /
   `BibRepository` plus pydantic DTOs (`PaperSummary`, `PaperCreate`).
   `db/models.py` declares the four ORM models. **`db/` is the only place under
   `src/paper_sorts/` permitted to import `sqlalchemy`** — services depend on
   DTOs, never on ORM types.

Supporting modules:

- **`config.py`** — pydantic-settings `Settings` model with the four-source
  priority chain.
- **`logging_config.py`** — single `logging.config.dictConfig` (RichHandler to
  stdout, optional FileHandler). Called once from `cli/app.py` at startup.

`migrations/versions/` holds the Alembic schema migrations. Revision 001 is the
verbatim port of the original DDL; revision 002 converges legacy database
variants (the `bibtext_id` typo column) onto the canonical schema idempotently.

For a reverse-engineered description of the *legacy* stack as it was before the
modernization, see `docs/architecture.md`.

### Database schema

Four tables, defined declaratively in `src/paper_sorts/db/models.py` and managed
via Alembic:

- `papers(id, title, contents, bibtex_id → bib.bibtex_id)`
- `bib(bibtex_id PK, bibtex UNIQUE)`
- `authors_id(id, author)`
- `authors_papers(id, author_id, paper_id)` — many-to-many link, **no DDL FKs**.

A paper is identified internally by `papers.id`; the BibTeX key (`bibtex_id`) is
the user-facing unique identifier and is the FK target from `papers` into `bib`.
Schema-preservation contract: do not add NOT NULL outside primary keys, do not
add FKs to `authors_papers`, do not add indexes the original DDL did not have.

## Tests

`uv run pytest` runs the suite against an ephemeral PostgreSQL spun up by
`pytest-postgresql` from the host's `pg_ctl`. No personal database and no
credentials required. Persistence-layer tests run against a real DB — no mocking
of the SQLAlchemy session, repositories, or driver.

Session fixtures live in `tests/conftest.py` (`postgresql_proc`,
`ephemeral_db_url`, `migrated_engine`, `seeded_engine`, `seeded_db_url`); the
canonical seed dataset is `tests/fixtures/seed_papers.SEED_PAPERS`. A baseline
benchmark for the five interactive operations lives under `tests/benchmarks/`.

## SpecKit

`.specify/` contains SpecKit templates and `memory/constitution.md`. The
constitution defines four binding principles — Code Quality, Testing Standards,
User Experience Consistency, Performance Requirements. Read it before generating
a plan or making non-trivial changes.

**Active feature**: `specs/001-modernize-stack/` — Modernize the Stack. See
[plan.md](specs/001-modernize-stack/plan.md) for the implementation plan,
[research.md](specs/001-modernize-stack/research.md) for the framework decisions,
[data-model.md](specs/001-modernize-stack/data-model.md) for the preserved
schema, and [contracts/](specs/001-modernize-stack/contracts/) for the CLI and
repository/service API contracts.
