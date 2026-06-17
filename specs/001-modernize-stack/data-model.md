# Phase 1 Data Model: Modernize the Stack

The schema is **preserved verbatim** from the legacy `DatabaseConnector.create_tables()`. No new tables, columns, NOT NULLs (outside PKs), FKs, or indexes. Modernization expresses the *same* four tables as SQLAlchemy 2.x declarative models and manages them via Alembic.

## Tables (canonical, post-migration)

### `papers`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | SERIAL / INTEGER | PRIMARY KEY |
| `title` | TEXT | nullable |
| `contents` | TEXT | nullable (paper summary) |
| `bibtex_id` | TEXT | FK → `bib.bibtex_id` (`fk_bibtex_id`), nullable |

Internal identity is `papers.id`. The user-facing unique key is `bibtex_id`.

### `bib`
| Column | Type | Constraints |
|--------|------|-------------|
| `bibtex_id` | TEXT | PRIMARY KEY |
| `bibtex` | TEXT | UNIQUE |

### `authors_id`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | SERIAL / INTEGER | PRIMARY KEY |
| `author` | TEXT | nullable; value is `"Last, First"` |

### `authors_papers` (many-to-many link)
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | SERIAL / INTEGER | PRIMARY KEY |
| `author_id` | INTEGER | **no DDL FK** (preserved) |
| `paper_id` | INTEGER | **no DDL FK** (preserved) |

> Schema-preservation contract (constitution + spec edge cases): `authors_papers` has **no** foreign keys by design. Do not add them. Do not add NOT NULL on `author`/`title`/`contents`/`bibtex_id`. Do not add indexes beyond the existing primary keys and the `bib.bibtex` UNIQUE.

## Legacy schema variant (migration source, US4 / FR-011)

An older personal DB created by the procedural `get_data.py` uses the **typo column `bibtext_id`** (sic) instead of `bibtex_id`, in both `bib` and `papers`, and `bib.bibtext` instead of `bib.bibtex` with no UNIQUE. Revision 0002 converges it:

1. If `bib.bibtext_id` exists and `bib.bibtex_id` does not → rename `bibtext_id` → `bibtex_id`.
2. If `bib.bibtext` exists and `bib.bibtex` does not → rename `bibtext` → `bibtex`.
3. Same rename for `papers.bibtext_id` → `bibtex_id`.
4. All steps guarded by live-schema inspection → **idempotent**: rerun is a no-op; a fresh canonical DB is untouched; a partial run resumes cleanly (US4 AS3).

No rows are created or deleted; counts (papers, authors, authorships, bib entries) are invariant (SC-004).

## Key Entities → ORM models (`db/models.py`)

- **Paper** ↔ `Paper(id, title, contents, bibtex_id)`
- **BibTeX Entry** ↔ `Bib(bibtex_id, bibtex)`
- **Author** ↔ `AuthorId(id, author)`
- **Authorship** ↔ `AuthorPaper(id, author_id, paper_id)`

Relationships are navigated via explicit joins in repositories (matching legacy SQL), not via ORM `relationship()` backrefs with cascades — keeping behaviour identical and avoiding implicit FK-driven deletes the schema cannot express.

## DTOs (`db/repositories.py`, cross the service boundary)

### `PaperSummary` (pydantic)
| Field | Type | Source |
|-------|------|--------|
| `authors` | `str` | `" and "`-joined `"Last, First"` names |
| `paper_id` | `int` | `papers.id` |
| `title` | `str` | `papers.title` |
| `bibtex_id` | `str` | `papers.bibtex_id` |
| `contents` | `str` | `papers.contents` |
| `bibtex` | `str \| None` | `bib.bibtex` (filled when a single paper is resolved) |

Mirrors the legacy "down_papers" tuple `[authors, id, title, bibtex_id, contents]` so CLI rendering parity is mechanical.

### `PaperCreate` (pydantic)
| Field | Type |
|-------|------|
| `title` | `str` |
| `contents` | `str` |
| `bibtex_id` | `str` |
| `bibtex` | `str` |
| `authors` | `list[str]` (each `"Last, First"`) |

Produced by both the interactive add path and the bulk-import extractor (`extract_papers_from_tex_bib → Iterator[PaperCreate]`).

## Update field table (services.update_field)

`update_field(table: Literal["papers", "bib", "authors_id"], column, identifier, value)` dispatches via `match`/`case` with `assert_never(table)`:

- `papers` → `title` | `contents` (by `papers.id`)
- `bib` → `bibtex` only (by `bibtex_id`); rejects duplicate bibtex (UNIQUE)
- `authors_id` → `author` only (by old author name); merges onto existing author if the new name exists, else renames; drops authors left with no papers
- `authors_papers` → rejected (link table, not user-editable) — preserves legacy `ValueError`
- IDs (`*_id` columns) → rejected ("IDs are unique and must not be changed!")
