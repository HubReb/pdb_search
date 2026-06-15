# Phase 1 Data Model: Modernize the Stack

The schema is **preserved verbatim** from the legacy DDL — modernization changes
the *access layer*, not the tables. Four tables, declared as SQLAlchemy 2.x
declarative models in `src/paper_sorts/db/models.py`, managed by Alembic.

## Schema-preservation contract

These are binding constraints carried from the legacy `create_tables()` DDL
(see research.md R2). Revision 001 reproduces them exactly and MUST NOT tighten:

- No `NOT NULL` outside primary keys.
- No DDL foreign keys on `authors_papers`.
- No indexes beyond the original primary keys and the `bib.bibtex` UNIQUE.
- The sole declared FK is `papers.bibtex_id → bib.bibtex_id` (`fk_bibtex_id`).

## Entities (ORM models)

### Bib — `bib`

The full BibTeX source for a paper, keyed by its BibTeX key.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `bibtex_id` | TEXT | PRIMARY KEY | user-facing unique identifier (the BibTeX key) |
| `bibtex` | TEXT | UNIQUE, nullable | the full BibTeX source string |

### Paper — `papers`

A publication record.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | SERIAL | PRIMARY KEY | internal identity |
| `title` | TEXT | nullable | publication title |
| `contents` | TEXT | nullable | one-sentence summary |
| `bibtex_id` | TEXT | FK → `bib.bibtex_id`, nullable | links to exactly one Bib |

### Author — `authors_id`

A credited person.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | SERIAL | PRIMARY KEY | internal identity |
| `author` | TEXT | nullable | name in `"Last, First"` form |

### Authorship — `authors_papers`

Many-to-many link between papers and authors. **No DDL foreign keys.**

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | SERIAL | PRIMARY KEY | |
| `author_id` | INT | nullable, no FK | references `authors_id.id` logically |
| `paper_id` | INT | nullable, no FK | references `papers.id` logically |

## Relationships

- **Paper → Bib**: many-to-one, by `papers.bibtex_id` (the only real FK). A
  paper has exactly one BibTeX entry; a BibTeX key identifies one paper.
- **Paper ↔ Author**: many-to-many via `authors_papers`. ORM models declare a
  `relationship(secondary=authors_papers)` for navigation, but **no DDL FK is
  added** to the link table (schema-preservation).

## Pydantic DTOs (the service boundary)

Defined in `db/repositories.py`; services use only these, never ORM types.

### `PaperSummary` (read model)

Returned by search operations. Mirrors the legacy "pretty print" shape.

| Field | Type | Source |
|-------|------|--------|
| `paper_id` | `int` | `papers.id` |
| `title` | `str` | `papers.title` |
| `authors` | `str` | authors joined with `" and "` (legacy display string) |
| `summary` | `str` | `papers.contents` |
| `bibtex_id` | `str` | `papers.bibtex_id` |
| `bibtex` | `str` | `bib.bibtex` |

### `PaperCreate` (write model)

Consumed by add / bulk-import.

| Field | Type | Notes |
|-------|------|-------|
| `title` | `str` | |
| `summary` | `str` | |
| `authors` | `list[str]` | each `"Last, First"` |
| `bibtex_id` | `str` | unique BibTeX key |
| `bibtex` | `str` | full BibTeX source |

## Known limitations (carried from legacy, documented per FR-001)

- Two authors with identical `"Last, First"` strings are treated as the **same**
  author (no disambiguation) — a documented limitation, unchanged.
- BibTeX-key uniqueness is what makes bulk re-import idempotent (re-running skips
  already-present keys).
- Deleting the last paper of an author deletes that author row (no orphan
  authors), matching legacy cleanup behaviour.

## Migration data flows (FR-011)

- **Canonical `bibtex_id` database** → revision 002 is a no-op (catalog probe
  finds no `bibtext_id` column).
- **Legacy `bibtext_id` (typo) database** → revision 002 renames
  `papers.bibtext_id → bibtex_id`, `bib.bibtext_id → bibtex_id`,
  `bib.bibtext → bibtex`, preserving every row (rename, not copy). Idempotent:
  re-running finds the already-renamed columns and does nothing.
- Row counts (papers, authors, authorships, bib) are identical before/after
  (SC-004): renames move no data.
