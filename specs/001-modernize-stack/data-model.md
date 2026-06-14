# Data Model: Modernize the Stack

**Feature**: 001-modernize-stack | **Date**: 2026-06-14

## Overview

No schema changes. The four-table PostgreSQL schema is preserved verbatim as a contract. The
modernization expresses it as SQLAlchemy ORM declarative models and Alembic migrations rather than
`create_tables()` SQL strings.

## Database Tables (preserved from legacy)

### papers

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| title | TEXT | |
| contents | TEXT | |
| bibtex_id | TEXT | FK → bib.bibtex_id |

### bib

| Column | Type | Constraints |
|--------|------|-------------|
| bibtex_id | TEXT | PRIMARY KEY |
| bibtex | TEXT | UNIQUE |

### authors_id

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| author | TEXT | |

### authors_papers (many-to-many link)

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| author_id | INT | (no DDL FK — preserved per schema contract) |
| paper_id | INT | (no DDL FK — preserved per schema contract) |

**Schema preservation rules** (from memory/schema_preservation.md):
- Do NOT add NOT NULL outside primary keys
- Do NOT add FKs to authors_papers
- Do NOT add indexes beyond existing primary keys

## In-Memory DTOs (pydantic models)

### PaperCreate

Used when adding a new paper:

```python
class PaperCreate(BaseModel):
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]  # "Last, First" format
```

### PaperSummary

Returned from search operations:

```python
class PaperSummary(BaseModel):
    paper_id: int
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]  # "Last, First" format
```

## SQLAlchemy ORM Models (`src/paper_sorts/db/models.py`)

```python
# Declarative models — db/ is the ONLY place that imports sqlalchemy (constitution I)

class Bib(Base):
    __tablename__ = "bib"
    bibtex_id: Mapped[str] = mapped_column(primary_key=True)
    bibtex: Mapped[str] = mapped_column(unique=True)

class Paper(Base):
    __tablename__ = "papers"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    title: Mapped[str]
    contents: Mapped[str]
    bibtex_id: Mapped[str] = mapped_column(ForeignKey("bib.bibtex_id"))

class Author(Base):
    __tablename__ = "authors_id"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    author: Mapped[str]

class AuthorPaper(Base):
    __tablename__ = "authors_papers"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    author_id: Mapped[int]   # no DDL FK
    paper_id: Mapped[int]    # no DDL FK
```

## Alembic Migration Strategy

### Revision 001 — Initial Schema

- Create all four tables with exact original DDL
- Detect legacy `bibtext_id` (sic) variant and rename to `bibtex_id`
- Idempotent: uses `IF NOT EXISTS`, `IF EXISTS`
- Wrapped in a single transaction

### Running migrations

```bash
uv run pdbsearch migrate          # applies pending Alembic migrations
# OR directly:
uv run alembic upgrade head
```

## Configuration Model (`src/paper_sorts/config.py`)

```python
class Settings(BaseSettings):
    # Database connection
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str
    db_user: str
    db_password: SecretStr

    # Optional encrypted-config source
    config_file: Path | None = None
    key_file: Path | None = None

    # Logging
    log_level: str = "INFO"
    log_file: Path | None = None

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
    )
    # Custom Fernet source registered as additional_source
```

Priority order: CLI flags > PDBSEARCH_* env vars > .env file > Fernet-encrypted INI
