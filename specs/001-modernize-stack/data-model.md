# Data Model: Modernize the Stack

**Feature**: 001-modernize-stack  
**Date**: 2026-06-20

---

## Entity Overview

No schema changes are introduced by this modernization. The four-table schema
is preserved verbatim from the legacy `DatabaseConnector.create_tables()` DDL.
The only structural change is column-name correction (`bibtext_id` → `bibtex_id`)
applied via Alembic Revision 002 to databases created by the older `get_data.py`.

---

## Database Entities (unchanged schema)

### papers

| Column      | Type              | Constraints                                  |
|-------------|-------------------|----------------------------------------------|
| `id`        | SERIAL (INTEGER)  | PRIMARY KEY                                  |
| `title`     | TEXT              | —                                            |
| `contents`  | TEXT              | —                                            |
| `bibtex_id` | TEXT              | FK → `bib.bibtex_id` (named `fk_bibtex_id`) |

### bib

| Column      | Type | Constraints      |
|-------------|------|------------------|
| `bibtex_id` | TEXT | PRIMARY KEY      |
| `bibtex`    | TEXT | UNIQUE           |

### authors_id

| Column   | Type             | Constraints |
|----------|------------------|-------------|
| `id`     | SERIAL (INTEGER) | PRIMARY KEY |
| `author` | TEXT             | —           |

### authors_papers

| Column      | Type             | Constraints |
|-------------|------------------|-------------|
| `id`        | SERIAL (INTEGER) | PRIMARY KEY |
| `author_id` | INTEGER          | —           |
| `paper_id`  | INTEGER          | —           |

**Note**: `authors_papers` has no DDL foreign key constraints — this is a
schema-preservation contract; no FKs shall be added (see constitution + CLAUDE.md).

---

## ORM Models (`src/paper_sorts/db/models.py`)

```python
# Declarative SQLAlchemy 2.x models

class Bib(Base):          # bib table
    bibtex_id: Mapped[str]   # PK
    bibtex:    Mapped[str]   # UNIQUE

class Paper(Base):         # papers table
    id:        Mapped[int]   # PK SERIAL
    title:     Mapped[str]
    contents:  Mapped[str]
    bibtex_id: Mapped[str]  # FK → bib.bibtex_id

class Author(Base):        # authors_id table
    id:     Mapped[int]    # PK SERIAL
    author: Mapped[str]

class AuthorPaper(Base):   # authors_papers table
    id:        Mapped[int] # PK SERIAL
    author_id: Mapped[int]
    paper_id:  Mapped[int]
```

---

## Pydantic DTOs (`src/paper_sorts/db/repositories.py`)

### PaperSummary (read model)

```python
class PaperSummary(BaseModel):
    paper_id:  int
    title:     str
    contents:  str
    bibtex_id: str
    authors:   list[str]   # joined from authors_id
    bibtex:    str         # joined from bib
```

### PaperCreate (write model)

```python
class PaperCreate(BaseModel):
    title:     str
    contents:  str
    bibtex_id: str
    bibtex:    str
    authors:   list[str]
```

---

## In-Memory Structures

No new in-memory data structures beyond the Pydantic DTOs listed above.

---

## Migrations

| Revision | Description                                                 |
|----------|-------------------------------------------------------------|
| 001      | Initial schema — verbatim port of `create_tables()` DDL    |
| 002      | Handle `bibtext_id` typo — rename to `bibtex_id` if needed |

Alembic manages version tracking in the `alembic_version` table.

---

## Validation Rules

- `bibtex_id` must be unique (enforced by `bib.bibtex_id` PRIMARY KEY).
- `bibtex` must be unique (enforced by `bib.bibtex` UNIQUE constraint).
- `bibtex_id` in `papers` must reference `bib.bibtex_id` (FK constraint).
- `author` in `PaperCreate.authors` is a non-empty list.
- All `str` fields are non-empty (validated at service layer, not ORM).
