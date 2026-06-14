# Data Model: Modernize the Stack

**Feature**: 001-modernize-stack  
**Date**: 2026-06-15

## Overview

No schema changes. The four-table schema is preserved verbatim as the canonical shape.
All modifications to ORM models, repositories, or DTOs must honour the schema-preservation
contract: no new NOT NULL columns (outside PKs), no DDL FKs on `authors_papers`, no new indexes.

## Database Tables (unchanged)

### `papers`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | SERIAL | PRIMARY KEY |
| `title` | TEXT | |
| `contents` | TEXT | |
| `bibtex_id` | TEXT | FK → `bib.bibtex_id` |

### `bib`
| Column | Type | Constraints |
|--------|------|-------------|
| `bibtex_id` | TEXT | PRIMARY KEY |
| `bibtex` | TEXT | UNIQUE |

### `authors_id`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | SERIAL | PRIMARY KEY |
| `author` | TEXT | |

### `authors_papers`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | SERIAL | PRIMARY KEY |
| `author_id` | INT | (no DDL FK — legacy contract) |
| `paper_id` | INT | (no DDL FK — legacy contract) |

## SQLAlchemy ORM Models (`src/paper_sorts/db/models.py`)

Four `DeclarativeBase` model classes mapping 1:1 to the tables above. The `bibtex_id` FK
from `papers` to `bib` is expressed as a SQLAlchemy `ForeignKey` in the ORM layer only
(no DDL FK added for `authors_papers`).

## Pydantic DTOs (`src/paper_sorts/db/repositories.py`)

### `PaperCreate`
Used for add and bulk-import operations.

```python
class PaperCreate(BaseModel):
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]  # "Last, First" strings
```

### `PaperSummary`
Returned by search operations.

```python
class PaperSummary(BaseModel):
    paper_id: int
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]  # "Last, First" strings
```

## Configuration Entity (`src/paper_sorts/config.py`)

```python
class Settings(BaseSettings):
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str
    db_user: str
    db_password: SecretStr
    log_level: str = "INFO"
    config_file: Path | None = None   # path to Fernet-encrypted INI
    key_file: Path | None = None      # path to decryption key

    model_config = SettingsConfigDict(
        env_prefix="PDBSEARCH_",
        env_file=".env",
        extra="ignore",
    )
```

Priority chain: CLI flags → `PDBSEARCH_*` env vars → `.env` → Fernet INI.

## Alembic Migrations

### Revision 001 — canonical schema
Creates all four tables in their canonical form (`bibtex_id` column, no DDL FKs on
`authors_papers`). Safe for fresh installs.

### Revision 002 — legacy typo fix
If `bibtext_id` column exists on `papers` or `bib`, renames it to `bibtex_id`.
Idempotent: checks column existence before rename.
