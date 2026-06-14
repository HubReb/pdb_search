# Data Model: Modernize the Stack

**Feature**: 001-modernize-stack | **Date**: 2026-06-14

## Overview

No schema changes. The four-table schema is preserved verbatim from `DatabaseConnector.create_tables()`. The only structural change is the expression of that schema via SQLAlchemy ORM models and Alembic migrations rather than runtime SQL strings.

## Database Tables

### `bib`

| Column | Type | Constraints |
|--------|------|-------------|
| bibtex_id | VARCHAR | PRIMARY KEY |
| bibtex | TEXT | UNIQUE |

ORM model: `Bib`

### `papers`

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| title | TEXT | |
| contents | TEXT | |
| bibtex_id | VARCHAR | FK → bib.bibtex_id |

ORM model: `Paper`

### `authors_id`

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| author | TEXT | |

ORM model: `Author`

### `authors_papers`

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| author_id | INTEGER | (no DDL FK — preserved per schema-preservation contract) |
| paper_id | INTEGER | (no DDL FK — preserved per schema-preservation contract) |

ORM model: `AuthorPaper`

**Schema-preservation contract**: Do NOT add `NOT NULL` constraints outside primary keys, do NOT add DDL foreign keys to `authors_papers`, do NOT add indexes that the original DDL did not have. These constraints existed in the original schema and are preserved in Revision 001 verbatim.

## Pydantic DTOs (in-process only, no new tables)

### `PaperCreate`
Fields: `title: str`, `contents: str`, `bibtex_id: str`, `bibtex: str`, `authors: list[str]`
Used by: `import_service.extract_papers_from_tex_bib()` → `paper_service.add_paper()`

### `PaperSummary`
Fields: `id: int`, `title: str`, `authors: list[str]`, `contents: str`, `bibtex_id: str`, `bibtex: str`
Used by: repository search results → CLI display

## ORM Relationships

```
Bib (1) ──── (N) Paper (N) ──── (N) Author
                       via AuthorPaper (no DDL FK)
```

SQLAlchemy relationship on `Paper.bib` → `Bib` via `bibtex_id` FK.
`AuthorPaper` is a plain mapped class (no `relationship()` shortcuts) — access is via explicit queries joining on `author_id` / `paper_id`.

## Alembic Migrations

| Revision | Description |
|----------|-------------|
| 001 | Initial schema — verbatim port of `DatabaseConnector.create_tables()` DDL |
| 002 | Legacy column fix — detect `bibtext_id` (sic) in `papers` table and rename to `bibtex_id`; idempotent |

## In-Memory Structures (not persisted)

`MenuOption` (used by `cli/prompts.py`): not a data model, just a runtime enum/namedtuple for numbered menu display. No storage impact.
