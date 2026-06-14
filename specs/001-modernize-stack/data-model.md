# Data Model: Modernize the Stack

**Feature**: 001-modernize-stack  
**Date**: 2026-06-14

## Overview

No new entities are introduced. The four existing database tables are ported verbatim to SQLAlchemy 2.x ORM models. Schema-preservation contract: no NOT NULL constraints beyond primary keys, no FK on `authors_papers`, no new indexes.

## Entities

### Paper

Represents a publication record.

| Field | Column | Type | Constraints |
|-------|--------|------|------------|
| id | `id` | `SERIAL` | PRIMARY KEY |
| title | `title` | `TEXT` | nullable |
| contents | `contents` | `TEXT` | nullable (summary) |
| bibtex_id | `bibtex_id` | `TEXT` | FK → bib.bibtex_id, nullable |

**ORM model**: `Paper` in `src/paper_sorts/db/models.py`

### Bib

Full BibTeX source string for a paper, keyed by BibTeX key.

| Field | Column | Type | Constraints |
|-------|--------|------|------------|
| bibtex_id | `bibtex_id` | `TEXT` | PRIMARY KEY |
| bibtex | `bibtex` | `TEXT` | UNIQUE |

**ORM model**: `Bib` in `src/paper_sorts/db/models.py`

### Author

A person credited on one or more papers.

| Field | Column | Type | Constraints |
|-------|--------|------|------------|
| id | `id` | `SERIAL` | PRIMARY KEY |
| author | `author` | `TEXT` | nullable (name in "Last, First" form) |

**ORM model**: `Author` in `src/paper_sorts/db/models.py`

### Authorship (authors_papers)

Many-to-many link between papers and authors. **No DDL foreign keys** (schema-preservation contract).

| Field | Column | Type | Constraints |
|-------|--------|------|------------|
| id | `id` | `SERIAL` | PRIMARY KEY |
| author_id | `author_id` | `INT` | (no FK constraint) |
| paper_id | `paper_id` | `INT` | (no FK constraint) |

**ORM model**: `AuthorPaper` in `src/paper_sorts/db/models.py`

## DTOs (Pydantic)

Defined in `src/paper_sorts/db/repositories.py`. Services depend on these, never on ORM types.

### PaperSummary

Read-model for search results.

```python
class PaperSummary(BaseModel):
    id: int
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]
```

### PaperCreate

Write-model for add operations.

```python
class PaperCreate(BaseModel):
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]
```

## Relationships

```
bib (1) ──── (0..1) papers (many) ─── (many) authors_papers ─── (many) authors_id
```

- One `bib` entry is referenced by at most one `paper` (bibtex keys are unique per paper).
- One paper links to zero or more authors via `authors_papers`.
- Duplicate `"Last, First"` author names are treated as the same author (documented limitation; current behaviour preserved).

## In-Memory Structures

No new in-memory data structures beyond Pydantic DTOs.

## Migration Plan

| Revision | Description |
|----------|-------------|
| 001 | Initial schema — verbatim port of DDL from `database_connector.py`. Creates all four tables. |
| 002 | Legacy guard — if column `bibtext_id` (typo) exists in `papers`, rename it to `bibtex_id`. Idempotent: skips if `bibtex_id` already present. |

## Validation Rules

- `bibtex_id` must be non-empty when adding a paper (enforced in service layer).
- `title` must be non-empty when adding a paper (enforced in service layer).
- At least one author must be provided when adding a paper (enforced in service layer).
- Empty input on any required prompt is rejected and re-prompted (constitution Principle III).
