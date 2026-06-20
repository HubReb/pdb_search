# Architecture: paper-sorts

## Purpose

paper-sorts is a personal, off-line CLI tool for storing and searching academic paper metadata. It stores publication titles, authors, summaries, and BibTeX entries in a local PostgreSQL database. It is strictly single-user, single-machine, and offline — no network API, no multi-user concerns.

The user interacts entirely through the command line. There is no web UI or REST API.

## User Journeys

### 1. Search by title

The user runs `pdbsearch search` (or selects "Search" from the interactive menu), chooses "Search by title", and types a paper title. The system returns matching papers. If more than one paper matches, the user picks one from a numbered list. The selected paper's title, authors, summary, and BibTeX entry are printed.

### 2. Search by author

Same as above but the user enters an author name in "Last, First" form. The system finds all papers by that author and presents them for selection.

### 3. Add a paper

The user runs `pdbsearch add` (or selects "Add" from the menu), enters the paper's title, summary, BibTeX citation key, and author names. Alternatively the user points to a `.bib` file which is parsed for the BibTeX data and author names. The entry is committed to the database.

### 4. Update a paper

The user runs `pdbsearch update`, searches for a paper, selects which field to update (title, summary, BibTeX text, or author), enters the new value, and confirms. On confirmation "n" or "no", no change is written.

### 5. Delete a paper

The user runs `pdbsearch delete`, searches for a paper, reviews the summary, and confirms deletion. Author links are removed; the bib row is preserved (other papers may reference the same key).

### 6. Bulk import from LaTeX + BibTeX

The user runs `pdbsearch import literature.tex bib.bib`. The system finds all `\cite{key}` references in the `.tex` file, looks them up in the `.bib` file, and inserts each matched paper. Unmatched citation keys are skipped with a warning. Each paper is committed individually so a partial failure is recoverable.

### 7. Migrate schema

The user runs `pdbsearch migrate` once on a new machine or after upgrading. Alembic applies all pending migrations. Safe to re-run (idempotent).

## Data Model

Four tables. No schema changes from the original — the modernization is internal only.

### papers

Stores one row per publication.

| Column | Type | Notes |
|--------|------|-------|
| id | SERIAL PK | internal surrogate key |
| title | TEXT | publication title |
| contents | TEXT | one-sentence summary |
| bibtex_id | TEXT FK→bib | user-facing citation key |

### bib

Stores the full BibTeX source string for each citation key.

| Column | Type | Notes |
|--------|------|-------|
| bibtex_id | TEXT PK | citation key (e.g. "Wang2021LargeScaleSA") |
| bibtex | TEXT UNIQUE | full BibTeX source string |

### authors_id

One row per distinct author name.

| Column | Type | Notes |
|--------|------|-------|
| id | SERIAL PK | internal surrogate key |
| author | TEXT | "Last, First" format |

**Known limitation**: deduplication is by exact string match. "Lee, Ann" and "Lee, A." are treated as different authors.

### authors_papers

Many-to-many link table between papers and authors. **No DDL foreign keys** — preserved from legacy schema.

| Column | Type | Notes |
|--------|------|-------|
| id | SERIAL PK | |
| author_id | INT | references authors_id.id (no DDL FK) |
| paper_id | INT | references papers.id (no DDL FK) |

## Control Flow

```
User types a command
      ↓
pdbsearch CLI (Typer) — src/paper_sorts/cli/app.py
      ↓ (dispatches to subcommand or interactive menu)
Subcommand module — src/paper_sorts/cli/{search,add,update,delete,importer}.py
      ↓ (calls service)
Service layer — src/paper_sorts/services/paper_service.py
      ↓ (opens session, calls repository)
Repository layer — src/paper_sorts/db/repositories.py
      ↓ (parameterised SQLAlchemy ORM queries)
SQLAlchemy Session
      ↓ (psycopg v3 driver)
PostgreSQL (local)
```

All user prompts route through `src/paper_sorts/cli/prompts.py` — no bare `input()` elsewhere.

Errors are logged via the stdlib logger (RichHandler to stdout + optional file sink) and presented as plain-language messages — no raw exceptions or stack traces on stdout.

## Configuration

Settings are loaded by `src/paper_sorts/config.py` (pydantic-settings) in this priority order (highest first):

1. CLI flags (`--database-url`, `--log-level`, `--config`, `--key`)
2. Environment variables (`PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`, etc.)
3. `.env` file in the current working directory
4. Fernet-encrypted INI file (the legacy `database.crypt` format) — requires both `--config` and `--key` or the corresponding env vars

If no database URL is configured from any source, the CLI exits with a clear error.

**Security note**: Plaintext credentials, decryption keys, and encrypted config files must never be committed to the repository.

## Install and Run

```bash
uv sync --all-extras          # install all dependencies
pdbsearch                     # interactive four-option menu
pdbsearch migrate             # run schema migrations (first-time setup)
pdbsearch search              # search subcommand
pdbsearch add                 # add subcommand
uv run pytest                 # run test suite (no personal DB needed)
uv run ruff check src tests   # lint
uv run mypy src               # type-check
```

Set `PDBSEARCH_DATABASE_URL=postgresql+psycopg://user:pass@localhost/mydb` to point at your database.

## Known Limitations

- **Author deduplication is by string equality.** Two spellings of the same person's name create two author rows and are not linked.
- **No DDL foreign keys on `authors_papers`.** Referential integrity is enforced at the application layer, not the database layer. This is preserved from the original schema.
- **Single-user, offline only.** No multi-tenant, no network API, no authentication.
- **Bulk import does not capture per-paper summaries** from the `.tex` file — summaries are left empty and can be filled in via `pdbsearch update`.
- **BibTeX keys must be unique in `bib`.** Attempting to import a paper whose key already exists in the database will fail with an integrity error and be logged/skipped.

## Where to Add a New Field

To add a new field to papers (e.g. "journal"):

1. Write a new Alembic migration in `migrations/versions/` that adds the column to the `papers` table.
2. Add the column to the `Paper` ORM model in `src/paper_sorts/db/models.py`.
3. Add the field to `PaperCreate` and `PaperSummary` DTOs in `src/paper_sorts/db/repositories.py`.
4. Update `PaperRepository.create` and `_build_summary` to populate the new field.
5. Add `"journal"` to `UpdatableField` in `src/paper_sorts/services/paper_service.py` and handle it in the `match`/`case` in `update_field`.
6. Update `src/paper_sorts/cli/update.py` to include "Journal" in the field selection menu.
7. Add tests in `tests/test_repositories.py` and `tests/test_services.py`.
