# Data Model: Modernize the Stack

**Feature**: 001-modernize-stack  
**Date**: 2026-04-26  
**Status**: Complete

---

## Schema Overview

Four tables, unchanged from the existing schema. The modernization does NOT alter the logical schema — it only migrates the DDL into Alembic-managed versioned files and replaces hand-written SQL with SQLAlchemy ORM.

```
papers(id SERIAL PK, title TEXT, contents TEXT, bibtex_id TEXT → bib.bibtex_id)
bib(bibtex_id TEXT PK, bibtex TEXT UNIQUE)
authors_id(id SERIAL PK, author TEXT)
authors_papers(id SERIAL PK, author_id INT, paper_id INT)   ← no DDL FKs
```

---

## ORM Models (`src/paper_sorts/db/models.py`)

### `Bib`
| Column     | Type          | Constraints              |
|------------|---------------|--------------------------|
| bibtex_id  | String (TEXT) | Primary Key              |
| bibtex     | String (TEXT) | Unique, Not Null         |

### `Paper`
| Column    | Type          | Constraints                                |
|-----------|---------------|--------------------------------------------|
| id        | Integer       | Primary Key, auto-increment (SERIAL)       |
| title     | String (TEXT) | Nullable                                   |
| contents  | String (TEXT) | Nullable                                   |
| bibtex_id | String (TEXT) | FK → bib.bibtex_id, Nullable               |

### `Author`
| Column | Type          | Constraints                          |
|--------|---------------|--------------------------------------|
| id     | Integer       | Primary Key, auto-increment (SERIAL) |
| author | String (TEXT) | Nullable                             |

### `AuthorPaper` (many-to-many link)
| Column    | Type    | Constraints                          |
|-----------|---------|--------------------------------------|
| id        | Integer | Primary Key, auto-increment (SERIAL) |
| author_id | Integer | Nullable (no DDL FK per schema preservation rule) |
| paper_id  | Integer | Nullable (no DDL FK per schema preservation rule) |

**Schema preservation rule**: Do NOT add NOT NULL outside primary keys, do NOT add FKs to `authors_papers`, do NOT add indexes beyond the original primary keys.

---

## Pydantic DTOs (`src/paper_sorts/db/repositories.py`)

### `PaperCreate`
Fields used when inserting a new paper (from `add` subcommand or bulk import):
```python
class PaperCreate(BaseModel):
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]   # "Last, First" format
```

### `PaperSummary`
Fields returned by search queries:
```python
class PaperSummary(BaseModel):
    paper_id: int
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: str   # " and "-joined display string
```

---

## Relationships

- **Paper → Bib**: Many-to-one (FK `papers.bibtex_id → bib.bibtex_id`). Each paper has exactly one BibTeX entry.
- **Paper ↔ Author**: Many-to-many via `authors_papers`. No DDL foreign keys per schema preservation rule.

---

## Alembic Migrations

### Revision 001 — Canonical Schema
Creates all four tables with canonical column name `bibtex_id` (not the legacy `bibtext_id` typo). This is the baseline for new installs.

### Revision 002 — Legacy Typo Convergence
Handles databases created by `add.py` / `get_data.py` which used `bibtext_id` (sic). Conditionally renames `bibtext_id` → `bibtex_id` in both `bib` and `papers` tables if the typo column exists. Idempotent via `IF EXISTS` / `IF NOT EXISTS` checks.

---

## Known Limitations (preserved from legacy)

- Two authors with identical "Last, First" strings are treated as the same author (current behaviour; documented limitation).
- `authors_papers` has no DDL foreign keys, so cascade-on-delete is not enforced at the DB level; application logic handles cleanup.
