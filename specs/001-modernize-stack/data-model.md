# Data Model: Modernize the Stack (001-modernize-stack)

**Feature Branch**: `001-modernize-stack`
**Date**: 2026-06-20
**Status**: Complete

---

## Entities

### Paper

Represents a publication record.

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| `id` | `int` | PK, SERIAL | Internal surrogate key |
| `title` | `str` | TEXT | Searchable; not unique (two papers may share a title) |
| `contents` | `str` | TEXT | Summary / abstract |
| `bibtex_id` | `str` | TEXT, FK → bib.bibtex_id | User-facing unique identifier (BibTeX key) |

**ORM model**: `src/paper_sorts/db/models.py::Paper`
**Pydantic DTOs**: `PaperSummary` (read), `PaperCreate` (write) in `src/paper_sorts/db/repositories.py`

---

### BibEntry (table: `bib`)

Stores the full BibTeX source string for a Paper.

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| `bibtex_id` | `str` | TEXT, PK | BibTeX citation key (e.g. `Wang2021`) |
| `bibtex` | `str` | TEXT, UNIQUE | Full BibTeX entry string |

**ORM model**: `src/paper_sorts/db/models.py::Bib`

---

### Author (table: `authors_id`)

A person credited on one or more papers.

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| `id` | `int` | PK, SERIAL | Internal surrogate key |
| `author` | `str` | TEXT | Name in "Last, First" form |

**ORM model**: `src/paper_sorts/db/models.py::Author`

---

### Authorship (table: `authors_papers`)

Many-to-many link between Author and Paper. **No DDL foreign keys** (matches original schema contract).

| Field | Type | Constraints | Notes |
|-------|------|-------------|-------|
| `id` | `int` | PK, SERIAL | Internal surrogate key |
| `author_id` | `int` | INT | Logical FK → authors_id.id (no DDL FK) |
| `paper_id` | `int` | INT | Logical FK → papers.id (no DDL FK) |

**ORM model**: `src/paper_sorts/db/models.py::AuthorPaper`

---

## Relationships

```
Paper (1) ──────── FK bibtex_id ──────── (1) Bib
Paper (1) ──── via AuthorPaper ────── (N) Author
Author (1) ──── via AuthorPaper ──── (N) Paper
```

No SQLAlchemy `relationship()` objects are declared on the ORM models (the schema preservation contract forbids adding FKs to `authors_papers`, and relationship navigation is done via explicit JOIN queries in the repositories).

---

## Pydantic DTOs (in `src/paper_sorts/db/repositories.py`)

```python
class PaperSummary(BaseModel):
    id: int
    title: str
    contents: str
    bibtex_id: str
    authors: list[str]       # joined from authors_id
    bibtex: str              # joined from bib

class PaperCreate(BaseModel):
    title: str
    contents: str
    bibtex_id: str
    authors: list[str]
    bibtex: str
```

Services accept and return these DTOs — never ORM model instances.

---

## Configuration Entity (`src/paper_sorts/config.py`)

Not persisted; loaded at startup by pydantic-settings.

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| `database_url` | `PostgresDsn` | env `PDBSEARCH_DATABASE_URL` / `.env` / Fernet INI | SQLAlchemy connection URL |
| `log_level` | `str` | env `PDBSEARCH_LOG_LEVEL` / `.env` | Default: `INFO` |
| `config_file` | `Path \| None` | CLI `--config` | Path to Fernet-encrypted INI |
| `key_file` | `Path \| None` | CLI `--key` | Path to Fernet decryption key |

Priority: CLI flags > env vars > `.env` > Fernet INI.

---

## Schema Preservation Constraints

Inherited from CLAUDE.md and constitution:

1. Do **not** add `NOT NULL` constraints outside primary keys.
2. Do **not** add DDL foreign keys to `authors_papers`.
3. Do **not** add indexes beyond the existing primary keys.
4. Column `bibtex_id` (not `bibtext_id`) is canonical in the modernized schema; Revision 002 migration handles the rename.

---

## Migration Revisions

| Revision | Description | Reversible |
|----------|-------------|------------|
| 001 | Create four tables in canonical schema (IF NOT EXISTS) | Yes (drop tables) |
| 002 | Convergence: rename `bibtext_id` → `bibtex_id` and `bibtext` → `bibtex` in both `papers` and `bib` if old column names detected | Yes (rename back) |
