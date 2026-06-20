# Data Model: Modernize the Stack

## Schema (unchanged from legacy)

No schema changes are introduced by this modernization. The four-table schema is preserved exactly as defined in the legacy `DatabaseConnector.create_tables()`. The ORM models are a direct mapping.

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

### authors_papers

| Column | Type | Constraints |
|--------|------|-------------|
| id | SERIAL | PRIMARY KEY |
| author_id | INT | (no DDL FK — preserved from legacy) |
| paper_id | INT | (no DDL FK — preserved from legacy) |

**Schema-preservation rule**: No NOT NULL constraints added outside primary keys. No FKs added to `authors_papers`. No indexes beyond existing primary keys.

## Domain Objects (Pydantic DTOs)

### PaperCreate

Used when inserting a new paper (add, bulk import).

```python
class PaperCreate(BaseModel):
    title: str
    authors: list[str]          # "Last, First" form
    bibtex_key: str             # unique identifier
    summary: str
    bibtex_text: str            # full bibtex source string
```

### PaperSummary

Returned from search operations.

```python
class PaperSummary(BaseModel):
    paper_id: int
    title: str
    authors: list[str]
    summary: str
    bibtex_key: str
    bibtex_text: str
```

## In-Memory Structures

These are ephemeral — not stored in the database.

### Settings (pydantic-settings)

Four-source priority: CLI args > env (`PDBSEARCH_*`) > `.env` > Fernet-encrypted INI

```python
class Settings(BaseSettings):
    database_url: PostgresDsn   # e.g. postgresql+psycopg://user:pass@localhost/db
    log_level: str = "INFO"
    config_file: str | None = None   # path to encrypted INI (optional)
    key_file: str | None = None      # path to Fernet key file (optional)
```

## Alembic Migration Revisions

### 001_initial_schema

Verbatim port of `DatabaseConnector.create_tables()`. Creates all four tables from scratch using `op.create_table`. Downgrade drops all four tables.

### 002_converge_schema

Handles legacy `bibtext_id` typo variant. Checks if `bibtext_id` column exists in `papers`; if so, renames it to `bibtex_id`. Idempotent: uses `op.execute` with conditional DDL. Downgrade is a no-op (schema was already wrong before, don't re-introduce the typo).
