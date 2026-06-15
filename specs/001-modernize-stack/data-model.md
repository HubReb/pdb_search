# Phase 1 Data Model: Modernize the Stack

The data model is **preserved exactly** from the legacy schema — this feature
is a stack modernization, not a schema redesign. The ORM models in
`src/paper_sorts/db/models.py` mirror the original DDL bit-for-bit, including
its looseness (nullable text columns, no FKs on the link table). The
schema-preservation contract (Constitution Principle IV + repo memory) forbids
tightening it.

## Tables (canonical schema, Alembic revision 001)

### `bib`
| Column | Type | Constraints | Notes |
|---|---|---|---|
| `bibtex_id` | TEXT | PRIMARY KEY | User-facing unique paper key (BibTeX key) |
| `bibtex` | TEXT | UNIQUE | Full BibTeX source string |

Legacy DDL: `bib (bibtex_id text primary key, bibtex text unique (bibtex))`.
The malformed `unique (bibtex)` is read as a column-level UNIQUE on `bibtex`.

### `papers`
| Column | Type | Constraints | Notes |
|---|---|---|---|
| `id` | SERIAL | PRIMARY KEY | Internal paper identity |
| `title` | TEXT | nullable | Paper title |
| `contents` | TEXT | nullable | One-sentence summary |
| `bibtex_id` | TEXT | FK → `bib.bibtex_id` (`fk_bibtex_id`) | Links paper to its BibTeX entry |

`title`/`contents` are intentionally nullable (original DDL has no NOT NULL).

### `authors_id`
| Column | Type | Constraints | Notes |
|---|---|---|---|
| `id` | SERIAL | PRIMARY KEY | Internal author identity |
| `author` | TEXT | nullable | Name in `"Last, First"` form |

### `authors_papers` (many-to-many link)
| Column | Type | Constraints | Notes |
|---|---|---|---|
| `id` | SERIAL | PRIMARY KEY | Link identity |
| `author_id` | INT | nullable, **no FK** | → `authors_id.id` (no DDL FK, by contract) |
| `paper_id` | INT | nullable, **no FK** | → `papers.id` (no DDL FK, by contract) |

**Contract**: do NOT add FKs to `author_id`/`paper_id`, do NOT add NOT NULL,
do NOT add indexes beyond the SERIAL PKs and the `bib` PK/UNIQUE.

## Relationships

- A **Paper** has exactly one **BibTeX Entry** (`papers.bibtex_id` → `bib`).
- A **Paper** has one or more **Authors** via **Authorship**
  (`authors_papers`), a many-to-many link enforced only in application code,
  not by DDL FKs.
- An **Author** appears on zero or more Papers.
- Two authors with identical `"Last, First"` strings are treated as the **same**
  author (documented legacy limitation, preserved).

## Entity → spec mapping

| Spec Key Entity | Table(s) | DTO (pydantic, in `db/repositories.py`) |
|---|---|---|
| Paper | `papers` (+ `bib` for key) | `PaperSummary`, `PaperCreate` |
| Author | `authors_id` | (name strings on the Paper DTOs) |
| BibTeX Entry | `bib` | (bibtex string field on DTOs) |
| Authorship | `authors_papers` | (implicit; author-name list on DTOs) |
| Configuration | — (not persisted) | `paper_sorts.config.Settings` |
| Migration | Alembic `versions/` | revisions 001, 002 |

## DTOs (service ↔ persistence contract)

Services never import ORM types; they exchange these pydantic DTOs with the
repository layer (Principle I isolation).

### `PaperSummary` (read model)
- `title: str`
- `authors: list[str]` — `"Last, First"` names, ordered as stored
- `summary: str` — `papers.contents`
- `bibtex_id: str`
- `bibtex: str` — full BibTeX source from `bib`

Backs the "pretty print" of a search hit (title / authors / summary / bib).

### `PaperCreate` (write model)
- `title: str`
- `authors: list[str]`
- `summary: str`
- `bibtex_id: str` — must be unique (DB PK + UNIQUE bib enforce it)
- `bibtex: str`

Produced inline by `pdbsearch add`, and per-paper by
`import_service.extract_papers_from_tex_bib(...)`.

## Update dispatch (replaces `DatabaseConnector.update_entry`)

`paper_service.update_field` dispatches on a `Literal` table argument with
compile-time exhaustiveness:

```python
table: Literal["papers", "bib", "authors_id"]
match table:
    case "papers":     # title | contents
    case "bib":        # bibtex only (key immutable)
    case "authors_id": # author rename, with merge/cleanup of orphaned authors
    case _: assert_never(table)
```

`authors_papers` is **not** an updatable target (the legacy code raised on it);
IDs are immutable. Updating an author name preserves the legacy merge
behaviour: if the new name already exists, repoint the link and dedupe; if the
old author then has no papers, delete it.

## State / lifecycle notes

- **Add**: insert `bib` row, then `papers` row, then per-author insert into
  `authors_id` (if new) + `authors_papers`. On any failure mid-add, roll back
  the bib + author rows created so far (legacy `rollback_database_addition`
  semantics) — now a single transaction via `with_session`, so the rollback is
  automatic on exception rather than hand-coded.
- **Delete**: remove `authors_papers` links; delete now-orphaned `authors_id`
  rows; delete the `papers` row; delete the `bib` row.
- **Bulk import**: per-paper commit (Constitution IV) — a partial failure
  leaves earlier papers persisted; rerun skips already-present `bibtex_id`
  keys (PK uniqueness).
- **Migration (legacy `bibtext_id` → `bibtex_id`)**: idempotent column
  convergence; row counts (papers, authors, authorships, bib) identical before
  and after (SC-004).
