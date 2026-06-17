# Phase 1 Data Model: Modernize the Stack

The schema is **preserved verbatim** from the legacy DDL — modernization swaps
the access layer, not the tables. Four tables, no new constraints. Modelled in
`src/paper_sorts/db/models.py` (SQLAlchemy 2.x typed declarative) and managed by
Alembic.

## Tables (canonical schema)

### `papers`
| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | INTEGER | PRIMARY KEY (SERIAL) | internal paper identity |
| `title` | TEXT | nullable | publication title |
| `contents` | TEXT | nullable | one-sentence summary |
| `bibtex_id` | TEXT | FK → `bib.bibtex_id` | user-facing BibTeX key |

### `bib`
| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `bibtex_id` | TEXT | PRIMARY KEY | BibTeX key (the join target) |
| `bibtex` | TEXT | UNIQUE | full BibTeX source string |

### `authors_id`
| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | INTEGER | PRIMARY KEY (SERIAL) | author identity |
| `author` | TEXT | nullable | name in `"Last, First"` form |

### `authors_papers` (many-to-many link)
| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | INTEGER | PRIMARY KEY (SERIAL) | link identity |
| `author_id` | INTEGER | **no DDL FK** | references `authors_id.id` logically |
| `paper_id` | INTEGER | **no DDL FK** | references `papers.id` logically |

**Schema-preservation contract** (Principle IV + memory): do NOT add NOT NULL
outside primary keys, do NOT add DDL FKs to `authors_papers`, do NOT add indexes
beyond the original primary keys. Revision `001` is a verbatim port; any
divergence is a defect.

## Relationships (modelled in the ORM, not new DDL)

- `Paper` —*many-to-one*→ `Bib` via `papers.bibtex_id = bib.bibtex_id`.
- `Paper` *many-to-many* `Author` through `authors_papers` (association via the
  link table; the relationship lives in Python, the link table has no DDL FKs).
- A `Bib` row owns exactly one `Paper`'s BibTeX source; `bib.bibtex` is UNIQUE
  (round-trips LaTeX accents unchanged — Edge case).

## Pydantic DTOs (the service/persistence boundary)

Services never see ORM instances; repositories return DTOs.

### `PaperSummary` (read model)
Fields: `paper_id: int`, `title: str`, `authors: str` (pretty `" and "`-joined),
`bibtex_id: str`, `contents: str`, `bibtex: str`.
- Mirrors the legacy "pretty print" tuple shape
  (`title`, `authors`, `summary`, `bib entry`) so search output is unchanged.

### `PaperCreate` (write model)
Fields: `title: str`, `contents: str`, `bibtex_id: str`, `bibtex: str`,
`authors: list[str]` (each `"Last, First"`).
- Produced inline by the `add` flow and one-at-a-time by
  `import_service.extract_papers_from_tex_bib`.

## Entities ↔ spec Key Entities

| Spec entity | Realisation |
|-------------|-------------|
| Paper | `papers` row + `Paper` ORM model; read via `PaperSummary` |
| Author | `authors_id` row + `Author` ORM model |
| BibTeX Entry | `bib` row + `Bib` ORM model |
| Authorship | `authors_papers` link row (M:N) |
| Configuration | `config.Settings` (pydantic-settings), not a DB table |
| Migration | Alembic revision under `migrations/versions/` |

## Update field table (services, FR-002)

`paper_service.update_field` updates exactly the legacy-editable columns,
dispatched by a `match`/`case` over a `Literal[...]` table argument with
`assert_never` for compile-time exhaustiveness:

| `table` | editable column | identifier |
|---------|-----------------|------------|
| `"papers"` | `title` or `contents` | `papers.id` |
| `"bib"` | `bibtex` (key immutable) | `bibtex_id` |
| `"authors_id"` | `author` | `authors_id.id` |
| `"authors_papers"` | — (rejected: no editable column) | — |

IDs are never editable (legacy `"_id" in update_column` rule preserved).

## State / invariants

- Adding a paper is atomic per paper: bib row → paper row → author links, all in
  one transaction; failure rolls back the whole paper (legacy rollback
  semantics, now via `with_session` rollback-on-exception).
- Bulk import commits **per paper** (US5 #3): a partial failure leaves earlier
  papers persisted and is re-runnable (BibTeX-key uniqueness skips dupes).
- Deleting a paper removes its `authors_papers` links, then orphaned
  `authors_id` rows (authors with no remaining papers), then the `papers` and
  `bib` rows — preserving legacy "delete author with no papers" behaviour.
