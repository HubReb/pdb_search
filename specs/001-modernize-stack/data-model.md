# Phase 1 Data Model: Modernize the Stack

The schema is **preserved verbatim** from the legacy canonical DDL — four tables, no new
columns, no new indexes, no FKs added to the link table. This document maps the legacy DDL to
the SQLAlchemy 2.x declarative models and the pydantic DTOs the service layer consumes.

## Schema-preservation contract

- `papers`, `bib`, `authors_id`, `authors_papers` only — no fifth table.
- No NOT NULL outside primary keys.
- `authors_papers` has **no DDL foreign keys** (the legacy link table never declared them).
- No indexes beyond the existing primary keys / the `bib.bibtex` UNIQUE constraint.
- The only FK is `papers.bibtex_id → bib.bibtex_id` (named `fk_bibtex_id` in the legacy DDL).

## ORM Models (`src/paper_sorts/db/models.py`)

### `Paper` → table `papers`

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | INTEGER | PRIMARY KEY (SERIAL) |
| `title` | TEXT | nullable |
| `contents` | TEXT | nullable |
| `bibtex_id` | TEXT | FK → `bib.bibtex_id` (`fk_bibtex_id`), nullable |

### `Bib` → table `bib`

| Column | Type | Constraints |
|--------|------|-------------|
| `bibtex_id` | TEXT | PRIMARY KEY |
| `bibtex` | TEXT | UNIQUE |

### `AuthorId` → table `authors_id`

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | INTEGER | PRIMARY KEY (SERIAL) |
| `author` | TEXT | nullable |

### `AuthorPaper` → table `authors_papers`

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | INTEGER | PRIMARY KEY (SERIAL) |
| `author_id` | INTEGER | nullable, **no FK** |
| `paper_id` | INTEGER | nullable, **no FK** |

## Relationships

- A **Paper** is linked to exactly one **Bib** via `papers.bibtex_id` (the user-facing unique
  BibTeX key).
- **Paper** ↔ **AuthorId** is many-to-many through `authors_papers` (resolved in the repository
  layer via explicit joins, not an ORM `relationship()` with FK metadata, since the link table
  carries no DDL FKs — preserving the legacy shape).
- A paper is identified internally by `papers.id`; the BibTeX key (`bibtex_id`) is the
  user-facing unique identifier.

## DTOs (`src/paper_sorts/db/repositories.py`)

Services depend on these pydantic models, never on ORM types — the constitution's driver/ORM
isolation rule (only `db/` imports `sqlalchemy`).

### `PaperSummary` (read model)

| Field | Type | Source |
|-------|------|--------|
| `paper_id` | `int` | `papers.id` |
| `title` | `str` | `papers.title` |
| `authors` | `str` | `" and "`-joined `authors_id.author` for the paper |
| `summary` | `str` | `papers.contents` |
| `bibtex_id` | `str` | `papers.bibtex_id` |
| `bibtex` | `str` | `bib.bibtex` |

Mirrors the legacy "pretty print" record (title, authors, summary, bib entry).

### `PaperCreate` (write model)

| Field | Type | Notes |
|-------|------|-------|
| `title` | `str` | required |
| `summary` | `str` | required (maps to `papers.contents`) |
| `bibtex_id` | `str` | required, unique BibTeX key |
| `bibtex` | `str` | required, full BibTeX source |
| `authors` | `list[str]` | one `"Last, First"` per author |

`import_service.extract_papers_from_tex_bib(...)` yields `PaperCreate` instances for the
per-paper bulk-import path.

## Update field table (`update_field`)

`paper_service.update_field` accepts a `Literal["papers", "bib", "authors_id"]` table argument
and a column, dispatched via `match`/`case` with `assert_never(table)` for exhaustiveness:

| table | editable column(s) | identifier |
|-------|--------------------|------------|
| `papers` | `title`, `contents` | `papers.id` |
| `bib` | `bibtex` (key is immutable) | `bib.bibtex_id` |
| `authors_id` | `author` | author name |

`authors_papers` is intentionally **not** updatable (matches legacy: raises on attempt).

## Configuration entity (`src/paper_sorts/config.py`)

`Settings` (pydantic-settings) resolves the database connection from four sources in priority
order: CLI flags > `PDBSEARCH_*` env > `.env` > Fernet-encrypted INI. Fields: `database_url`
(assembled or provided), `log_level`, plus the Fernet inputs (`config_path`, `key_path`,
`section`). Secrets are never logged.

## Migration entity

Alembic revisions under `migrations/versions/`:

- **001** — creates the four canonical tables (verbatim legacy DDL).
- **002** — converges a database carrying the legacy `bibtext_id`/`bibtext` typo columns onto
  the canonical `bibtex_id`/`bibtex` names; idempotent (no-op once converged).
