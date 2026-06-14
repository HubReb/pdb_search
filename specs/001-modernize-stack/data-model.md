# Data Model: Modernize the Stack

**Feature**: 001-modernize-stack  
**Date**: 2026-06-15

---

## Entities

### Paper

Represents a publication record.

| Attribute | Type | Notes |
|-----------|------|-------|
| `id` | `int` (SERIAL PK) | Internal row identifier |
| `title` | `str` | Publication title; non-null per existing data |
| `contents` | `str` | Summary / abstract; non-null per existing data |
| `bibtex_id` | `str` | FK into `bib.bibtex_id`; user-facing unique key |

ORM model: `src/paper_sorts/db/models.py::Paper`

### BibEntry

Holds the full BibTeX source for a paper.

| Attribute | Type | Notes |
|-----------|------|-------|
| `bibtex_id` | `str` (PK) | BibTeX citation key (e.g. `Wang2021LargeScaleSA`) |
| `bibtex` | `str` (UNIQUE) | Full BibTeX source string |

ORM model: `src/paper_sorts/db/models.py::BibEntry`

### Author

A person credited on one or more papers.

| Attribute | Type | Notes |
|-----------|------|-------|
| `id` | `int` (SERIAL PK) | Internal row identifier |
| `author` | `str` | Name in `"Last, First"` form |

ORM model: `src/paper_sorts/db/models.py::Author`

### AuthorPaper (link table)

Many-to-many link between Author and Paper.

| Attribute | Type | Notes |
|-----------|------|-------|
| `id` | `int` (SERIAL PK) | Row identifier |
| `author_id` | `int` | References `authors_id.id` (no DDL FK — schema preservation rule) |
| `paper_id` | `int` | References `papers.id` (no DDL FK — schema preservation rule) |

ORM model: `src/paper_sorts/db/models.py::AuthorPaper`

**Schema-preservation rule**: No NOT NULL constraints outside primary keys, no FKs on `authors_papers`, no indexes beyond original primary keys. This is a hard contract from the constitution.

---

## Pydantic DTOs (in `src/paper_sorts/db/repositories.py`)

### PaperCreate

Input DTO for creating a paper. Used by services layer; never exposes ORM types.

```python
class PaperCreate(BaseModel):
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]  # "Last, First" form
```

### PaperSummary

Output DTO returned by search operations.

```python
class PaperSummary(BaseModel):
    id: int
    title: str
    contents: str
    bibtex_id: str
    authors: list[str]
    bibtex: str
```

---

## In-Memory Only: No New Tables

No new database tables are introduced by this feature. The modernization maps onto the existing four-table schema exactly. Alembic revision 001 is a verbatim DDL port; revision 002 converges legacy `bibtext_id` typo column if found.

---

## Relationships

```
Paper (papers.id)
  └── has one BibEntry via papers.bibtex_id → bib.bibtex_id
  └── linked to 0..* Author via authors_papers (no DDL FK)

Author (authors_id.id)
  └── linked to 0..* Paper via authors_papers (no DDL FK)

AuthorPaper (authors_papers.id)
  └── author_id (soft ref to authors_id.id)
  └── paper_id (soft ref to papers.id)
```

---

## Alembic Revisions

| Rev | Description |
|-----|-------------|
| `001_initial_schema` | Verbatim port of current DDL: papers, bib, authors_id, authors_papers |
| `002_converge_bibtext_typo` | IF column `bibtext_id` exists, rename to `bibtex_id`; idempotent |
