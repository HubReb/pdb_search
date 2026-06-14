# Architecture: paper_sorts

**Version**: modernized stack (001-modernize-stack)  
**Date**: 2026-06-14

## Purpose

`paper_sorts` is an offline, single-user CLI tool for storing and searching academic paper metadata in a local PostgreSQL database. The user can search by title or author, add new papers, update fields, delete papers, and bulk-import from a LaTeX + BibTeX file pair.

## User Journeys

| Journey | Entry Point | Actions |
|---------|-------------|---------|
| Find a paper by title | `pdbsearch search` → "Search by title" | Type part of a title; if multiple matches, pick from a numbered list; paper details displayed |
| Find papers by author | `pdbsearch search` → "Search by author" | Type author name; all matching papers listed |
| Add a paper manually | `pdbsearch add` | Enter title, authors, BibTeX key, summary, BibTeX text or .bib path |
| Update a field | `pdbsearch update` | Find paper, pick field, enter new value, confirm |
| Delete a paper | `pdbsearch delete` | Find paper, confirm deletion |
| Bulk import | `pdbsearch import --tex TEX --bib BIB` | Imports all cited entries from a .tex + .bib pair |
| Migrate schema | `pdbsearch migrate` | Applies all pending Alembic migrations; idempotent |

## Data Model

### Tables

```
bib (bibtex_id TEXT PK, bibtex TEXT UNIQUE)
papers (id SERIAL PK, title TEXT, contents TEXT, bibtex_id TEXT FK→bib.bibtex_id)
authors_id (id SERIAL PK, author TEXT)
authors_papers (id SERIAL PK, author_id INT, paper_id INT)   ← no FK constraints
```

Key constraints:
- `bib.bibtex_id` is the user-facing unique identifier for a paper.
- `bib.bibtex` has a UNIQUE constraint (same BibTeX text cannot appear twice).
- `authors_papers` has **no DDL foreign keys** (preserved from original schema).
- A paper is identified internally by `papers.id`; `bibtex_id` is the FK target.

### Relationships

```
bib (1) ──── (0..1) papers (many) ─── (many) authors_papers ─── (many) authors_id
```

A paper must have exactly one BibTeX entry. A paper can have zero or more authors linked via `authors_papers`.

### Known Limitations

- Duplicate `"Last, First"` author names are treated as the same author. There is no deduplication beyond the exact string match.
- `authors_papers` has no DDL foreign keys, so orphaned rows can exist if the underlying paper or author is deleted without cleaning up links. The delete operation removes links explicitly.

## Control Flow

### CLI → Service → Repository → DB

```
User input
    │
    ▼
cli/app.py            # Typer entry point; parses global flags; drops into menu
    │
    ▼
cli/<subcommand>.py   # Calls prompts.py for I/O; validates inputs
    │                 # Passes DTOs to service layer
    ▼
services/paper_service.py   # Domain logic; orchestrates repository calls
    │                       # No SQL, no I/O
    ▼
db/repositories.py    # SQL via SQLAlchemy ORM; returns DTOs
    │
    ▼
db/session.py         # with_session() context manager
    │                 # Commits on success; rolls back on exception
    ▼
PostgreSQL
```

### Adding a New Paper (detailed)

1. `cli/add.py` prompts for title, authors, bibtex_id, contents, bibtex.
2. Constructs a `PaperCreate` DTO and calls `paper_service.add_paper(session, paper)`.
3. `paper_service.add_paper` delegates to `PaperRepository.add(paper)`.
4. `PaperRepository.add` inserts into `bib`, then `papers`, then iterates authors (insert-or-reuse via `authors_id`), then inserts `authors_papers` rows.
5. All inserts share the same session; `with_session` commits atomically.
6. On any exception, the session is rolled back — no partial rows remain.

### Ctrl+C Mid-Dialog

`with_session()` is a context manager. If the user hits Ctrl+C, Python raises `KeyboardInterrupt` which exits the `with` block via the `except` path, triggering `session.rollback()`. The database is left in its pre-operation state.

## Configuration

Four-source priority chain (highest first):

1. **CLI flags** — `--database-url`, `--log-level`, `--config`, `--key`
2. **Environment variables** — `PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`
3. **`.env` file** — same keys without prefix
4. **Fernet-encrypted INI** — `[postgresql]` section; decrypted at startup if `--config` + `--key` provided

Config layout (`config_reader.py` replacement):
```ini
[postgresql]
dbname = mydb
user = myuser
password = secret
host = localhost
port = 5432
```

## Install / Run

```bash
uv sync --all-extras        # install deps
uv run pdbsearch migrate    # create / upgrade schema
uv run pdbsearch            # interactive CLI
uv run pytest               # test suite (ephemeral DB)
```

## Source Layout

```
src/paper_sorts/
├── cli/
│   ├── app.py         # Typer entry point + top-level menu
│   ├── search.py      # search subcommands
│   ├── add.py         # add subcommand
│   ├── update.py      # update subcommand
│   ├── delete.py      # delete subcommand
│   ├── importer.py    # import subcommand
│   ├── migrate.py     # migrate subcommand
│   └── prompts.py     # sole prompt/display module
├── services/
│   ├── paper_service.py   # domain operations
│   └── import_service.py  # bulk import parser
├── db/
│   ├── models.py      # ORM models
│   ├── repositories.py # repositories + DTOs
│   └── session.py     # session factory
├── config.py          # pydantic-settings Settings
└── logging_config.py  # dictConfig setup
```

## Where to Add a New Field

1. Add the column to `src/paper_sorts/db/models.py` (e.g. `year: Mapped[int | None]`).
2. Create an Alembic migration: `uv run alembic revision -m "add_year_to_papers"`.
3. Add the field to `PaperCreate` and `PaperSummary` in `db/repositories.py`.
4. Update the relevant repository method (e.g. `PaperRepository.add`) to write and read the field.
5. Add a `paper_service.update_field` case for the new field.
6. Add a `cli/update.py` menu option and prompt.
7. Update tests in `tests/test_repositories.py` and `tests/test_services.py`.
