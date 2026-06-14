# Data Model: Modernize the Stack

**Feature**: 001-modernize-stack | **Date**: 2026-06-15

## Overview

No schema changes are introduced by this feature. The four-table schema is preserved verbatim. The modernization replaces the persistence layer implementation (raw psycopg2 SQL strings → SQLAlchemy 2.x ORM) without altering the schema contract.

## Database Schema (preserved verbatim)

### Table: `bib`
| Column | Type | Constraints |
|--------|------|-------------|
| bibtex_id | TEXT | PRIMARY KEY |
| bibtex | TEXT | UNIQUE |

### Table: `papers`
| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| title | TEXT | |
| contents | TEXT | |
| bibtex_id | TEXT | FK → bib.bibtex_id |

### Table: `authors_id`
| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| author | TEXT | |

### Table: `authors_papers`
| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| author_id | INT | (no DDL FK — preserved from original) |
| paper_id | INT | (no DDL FK — preserved from original) |

**Schema-preservation contract**: No NOT NULL constraints added outside PKs; no DDL FKs added to `authors_papers`; no indexes added beyond existing PKs.

## ORM Models (`src/paper_sorts/db/models.py`)

```python
class Bib(Base):
    __tablename__ = "bib"
    bibtex_id: Mapped[str] = mapped_column(primary_key=True)
    bibtex: Mapped[str] = mapped_column(unique=True)
    paper: Mapped["Paper"] = relationship(back_populates="bib_entry")

class Paper(Base):
    __tablename__ = "papers"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    title: Mapped[str]
    contents: Mapped[str]
    bibtex_id: Mapped[str] = mapped_column(ForeignKey("bib.bibtex_id"))
    bib_entry: Mapped["Bib"] = relationship(back_populates="paper")
    author_links: Mapped[list["AuthorPaper"]] = relationship()

class Author(Base):
    __tablename__ = "authors_id"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    author: Mapped[str]
    paper_links: Mapped[list["AuthorPaper"]] = relationship()

class AuthorPaper(Base):
    __tablename__ = "authors_papers"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    author_id: Mapped[int]
    paper_id: Mapped[int]
```

## DTOs (Pydantic models in `src/paper_sorts/db/repositories.py`)

```python
class PaperSummary(BaseModel):
    """Read-only view of a paper with its authors and BibTeX."""
    id: int
    title: str
    contents: str
    bibtex_id: str
    authors: list[str]
    bibtex: str

class PaperCreate(BaseModel):
    """Input DTO for creating a new paper."""
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]
```

## Key Entities

| Entity | Class | Layer |
|--------|-------|-------|
| Paper | `Paper` ORM + `PaperSummary`/`PaperCreate` DTOs | db/ → services/ |
| Author | `Author` ORM | db/ |
| BibTeX Entry | `Bib` ORM | db/ |
| Authorship Link | `AuthorPaper` ORM | db/ (join table, no DDL FKs) |
| Configuration | `Settings` (pydantic-settings) | config.py |
| Migration | Alembic revision | migrations/versions/ |

## Alembic Migration Plan

### Revision 001: `initial_schema`
- Creates all four tables exactly as `DatabaseConnector.create_tables()` did
- Uses `bibtex_id` (correct spelling)

### Revision 002: `converge_legacy_bibtext_id`
- Detects if `bibtext_id` (typo) column exists in `papers` table
- If present: renames to `bibtex_id`; also renames in `bib` table if needed
- Idempotent: skips if `bibtex_id` already exists
- Downgrade: reverses the rename (best-effort; data is preserved)

## State Transitions

A paper's lifecycle:
1. **Created**: `PaperCreate` DTO → `PaperRepository.add_paper()` → writes bib, paper, authors in one session (rolls back atomically on any failure)
2. **Read**: `PaperRepository.search_by_title()` / `search_by_author()` → returns `PaperSummary`
3. **Updated**: `PaperRepository.update_field(paper_id, field, value)` → writes one column; confirmed by user before call
4. **Deleted**: `PaperRepository.delete_paper(paper_id)` → removes author links, paper row, bib row in one session

## Validation Rules

- `bibtex_id` must be unique (enforced by `bib.bibtex_id PRIMARY KEY`)
- `bibtex` content must be unique (enforced by `bib.bibtex UNIQUE`)
- All prompt inputs: non-empty (enforced in `cli/prompts.py` re-prompt loop)
- Author names: stored as `"Last, First"` strings (no normalisation; duplicates are same person per constitution)
