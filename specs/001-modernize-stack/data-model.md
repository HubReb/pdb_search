# Data Model: Modernize the Stack

**Feature**: 001-modernize-stack | **Date**: 2026-06-14

## Schema Overview

Four PostgreSQL tables. Schema-preservation rule: no new NOT NULL columns outside PKs, no new FKs on `authors_papers`, no new indexes beyond existing PKs. The DDL is a verbatim port of `DatabaseConnector.create_tables()`.

```
papers
  id        SERIAL PRIMARY KEY
  title     TEXT
  contents  TEXT
  bibtex_id TEXT  (FK -> bib.bibtex_id)

bib
  bibtex_id TEXT PRIMARY KEY
  bibtex    TEXT UNIQUE

authors_id
  id      SERIAL PRIMARY KEY
  author  TEXT

authors_papers
  id         SERIAL PRIMARY KEY
  author_id  INT   (no DDL FK)
  paper_id   INT   (no DDL FK)
```

## Entities

### Paper

User-visible entity representing one publication.

| Field | Type | Source table | Notes |
|-------|------|--------------|-------|
| id | int | papers.id | internal PK |
| title | str | papers.title | not unique by constraint |
| contents | str | papers.contents | one-sentence summary |
| bibtex_id | str | papers.bibtex_id | unique; FK -> bib |
| authors | list[str] | authors_id.author (via authors_papers) | assembled by query |
| bibtex | str | bib.bibtex | full BibTeX source string |

### Author

| Field | Type | Notes |
|-------|------|-------|
| id | int | internal PK |
| author | str | "Last, First" form; not unique by constraint (known limitation: duplicates treated as same author) |

### BibTeX Entry

| Field | Type | Notes |
|-------|------|-------|
| bibtex_id | str | PK; user-visible unique cite key |
| bibtex | str | full BibTeX source; UNIQUE constraint |

### Authorship (link table)

| Field | Type | Notes |
|-------|------|-------|
| id | int | PK |
| author_id | int | no DDL FK (schema-preservation rule) |
| paper_id | int | no DDL FK |

## Pydantic DTOs (in `db/repositories.py`)

### PaperCreate

Used to pass data from CLI/import service into the repository (create path).

```python
class PaperCreate(BaseModel):
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]   # ["Last, First", ...]
```

### PaperSummary

Returned by search operations (read path). Carries all display-relevant fields.

```python
class PaperSummary(BaseModel):
    id: int
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]   # ["Last, First", ...]
```

## In-Memory Enumerations

No new schema objects. The update operation's `table` argument is typed as `Literal["papers", "bib", "authors_id"]` in `paper_service.update_field`, with `assert_never` for compile-time exhaustiveness.

## Legacy Schema Variants

Two historical DDL variants exist (discovered from `get_data.py` and `add.py` vs `database_connector.py`):

| Variant | bib column | papers FK column |
|---------|-----------|-----------------|
| Current (database_connector.py) | `bibtex_id` | `bibtex_id` |
| Legacy (get_data.py / add.py) | `bibtext_id` (sic) | `bibtext_id` (sic) |

Alembic revision 002 detects and renames `bibtext_id` to `bibtex_id` idempotently (only if the typo column exists).

## Relationships

```
papers  -->  bib         (many-to-one; papers.bibtex_id -> bib.bibtex_id)
papers  <->  authors_id  (many-to-many via authors_papers; no DDL FK)
```

One paper has exactly one BibTeX entry. One paper may have one or more authors. One author may be credited on zero or more papers.
