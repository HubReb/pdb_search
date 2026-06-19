# Phase 1 Data Model: Modernize the Stack

The schema is **preserved verbatim** from the legacy canonical DDL (`DatabaseConnector.create_tables`).
Modernization changes the *access layer* (SQLAlchemy ORM + Alembic), not the table shapes. No new
NOT NULL outside primary keys, no foreign keys on `authors_papers`, no indexes beyond the original
primary keys and the one UNIQUE constraint. This preservation is a hard contract (constitution memory
+ spec edge cases).

## Tables (canonical `bibtex_id` schema — Alembic revision 001)

### `bib`
| Column | Type | Constraints |
|--------|------|-------------|
| `bibtex_id` | TEXT | PRIMARY KEY |
| `bibtex` | TEXT | UNIQUE |

The full BibTeX source string, keyed by the user-facing BibTeX key.

### `papers`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | SERIAL | PRIMARY KEY |
| `title` | TEXT | — |
| `contents` | TEXT | — (the one-sentence summary) |
| `bibtex_id` | TEXT | FK → `bib.bibtex_id` |

Internal identity is `papers.id`; the user-facing unique identifier is `bibtex_id`.

### `authors_id`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | SERIAL | PRIMARY KEY |
| `author` | TEXT | — |

Author name in `"Last, First"` form. **Documented limitation**: two authors with identical
`"Last, First"` strings collapse to one row (legacy behaviour, preserved).

### `authors_papers`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | SERIAL | PRIMARY KEY |
| `author_id` | INT | — (no DDL FK — preserved) |
| `paper_id` | INT | — (no DDL FK — preserved) |

The many-to-many link table between papers and authors. **No DDL foreign keys** — this is part of the
preservation contract, even though the relationship is conceptually an FK pair.

## Relationships

- `papers` *N*→*1* `bib` (each paper has exactly one BibTeX entry; FK `papers.bibtex_id → bib.bibtex_id`).
- `papers` *M*↔*N* `authors_id` via `authors_papers` (a paper has ≥1 authors; an author has ≥0 papers — orphan authors are deleted on the last unlink).

## Legacy schema variant (Alembic revision 002 converges it)

The older procedural modules (`get_data.py`) created the typo columns `bib.bibtext_id`/`bib.bibtext`
and `papers.bibtext_id`. Revision 002 renames these to the canonical `bibtex_id`/`bibtex` **only when
present**, guarded on `information_schema.columns`, so the migration:
- is a no-op on an already-canonical database (idempotent, FR-011 / US4 #1),
- converges a legacy-typo database (FR-011 / US4 #2),
- runs inside a transaction so a mid-run failure leaves the pre-migration state intact (US4 #3).

## ORM models (`src/paper_sorts/db/models.py`)

Four typed declarative models mirroring the tables one-to-one: `Bib`, `Paper`, `Author` (table
`authors_id`), `AuthorPaper` (table `authors_papers`). `Paper.authors` is a relationship via the link
table for join convenience; the link table has no FK in DDL but the relationship is expressed at the
ORM level for query ergonomics (no schema change).

## DTOs (`src/paper_sorts/db/repositories.py`) — the service boundary

Pydantic models that cross the `db/` boundary so SQLAlchemy types never leak into services (Principle I):

### `PaperSummary`
| Field | Type | Notes |
|-------|------|-------|
| `paper_id` | int | `papers.id` |
| `title` | str | |
| `authors` | list[str] | resolved author names, display order |
| `summary` | str | `papers.contents` |
| `bibtex_id` | str | |
| `bibtex` | str | full BibTeX source |

The display unit for search results (replaces the legacy positional `List[str]` rows).

### `PaperCreate`
| Field | Type | Notes |
|-------|------|-------|
| `title` | str | |
| `summary` | str | |
| `bibtex_id` | str | unique |
| `bibtex` | str | |
| `authors` | list[str] | ≥1 |

The input unit for `add_paper` and the per-paper bulk-import path
(`extract_papers_from_tex_bib -> Iterator[PaperCreate]`).

## Updatable fields (service `update_field`)

`update_field(table, column, identifier, new_value)` with `table: Literal["papers","bib","authors_id"]`
dispatched via `match`/`case` with `assert_never(table)` for exhaustiveness. Allowed
(column, table) pairs, mirroring legacy:
- `papers.title`, `papers.contents`
- `bib.bibtex` (the bibtex source; the `bibtex_id` key is immutable)
- `authors_id.author`

Any `*_id` column is refused (`"IDs are unique and must not be changed!"`). `authors_papers` is never
user-updatable.

## Configuration (not persisted)

`Settings` (pydantic-settings): `database_url` (assembled from host/port/db/user/password or supplied
directly), `log_level`, optional `log_file`, and the Fernet inputs (`config` path + `key` path) used by
the custom encrypted-INI source. Priority: CLI > env (`PDBSEARCH_*`) > `.env` > Fernet INI. Secrets are
never written to logs or committed.
