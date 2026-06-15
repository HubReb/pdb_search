# Contract: Persistence Layer (Repositories, Session, Services)

This fixes the interface between `services/` and `db/`. Only `db/` imports
`sqlalchemy`/`psycopg` (Principle I). Services depend on DTOs.

## Session — `db/session.py`

```python
def with_session(engine: Engine) -> ContextManager[Session]: ...
```

- Yields a SQLAlchemy `Session`. Commits on clean exit, rolls back on
  exception, and **always closes** (Principle IV — deterministic close, no
  long-lived sessions). Implemented as a `@contextmanager`.

## DTOs — `db/repositories.py`

```python
class PaperSummary(BaseModel):
    paper_id: int
    title: str
    authors: str        # joined with " and "
    summary: str
    bibtex_id: str
    bibtex: str

class PaperCreate(BaseModel):
    title: str
    summary: str
    authors: list[str]
    bibtex_id: str
    bibtex: str
```

## Repositories — `db/repositories.py`

Each takes a `Session` in its constructor. No repository opens its own
connection; the caller owns the session lifecycle via `with_session`.

### `PaperRepository`

| Method | Returns | Behaviour |
|--------|---------|-----------|
| `search_by_title(title)` | `list[PaperSummary]` | INNER JOIN papers↔authors_papers↔authors_id, filter `papers.title == title`; collapse authors into the joined string; one `PaperSummary` per distinct paper. Empty list if none. |
| `search_by_author(author)` | `list[PaperSummary]` | Same join filtered on `authors_id.author == author`. Empty list if none. |
| `add(paper: PaperCreate)` | `int` (new `papers.id`) | Insert bib (if key absent), insert paper, link authors (reusing existing `authors_id` rows by name, else insert). Raises `ValueError` on duplicate BibTeX key. |
| `delete(paper_id)` | `None` | Remove `authors_papers` links, the paper row, its bib row, and any now-orphaned `authors_id` rows. |
| `get_by_id(paper_id)` | `PaperSummary \| None` | Single-paper fetch for confirmation summaries. |

### `AuthorRepository`

| Method | Returns | Behaviour |
|--------|---------|-----------|
| `rename(old_name, new_name)` | `None` | Repoint `authors_papers` to the new author (reuse if it exists, else create), delete the old `authors_id` row, drop duplicate links and orphaned authors — legacy `__update_author` semantics. |

### `BibRepository`

| Method | Returns | Behaviour |
|--------|---------|-----------|
| `update_bibtex(bibtex_id, new_source)` | `None` | Update `bib.bibtex`; raise `ValueError` if `new_source` already exists (UNIQUE constraint, legacy behaviour). |

## Services — `services/paper_service.py`

Pure orchestration over DTOs + repositories; no SQL, no rich, no I/O.

```python
def search_by_title(engine, title) -> list[PaperSummary]: ...
def search_by_author(engine, author) -> list[PaperSummary]: ...
def add_paper(engine, paper: PaperCreate) -> int: ...
def delete_paper(engine, paper_id: int) -> None: ...
def update_field(
    engine,
    table: Literal["papers", "bib", "authors_id"],
    column: str,
    identifier: str,
    value: str,
) -> None: ...
```

`update_field` uses `match table` with `assert_never(table)` on the final case
for compile-time exhaustiveness (research.md R12). It rejects ID columns
(immutable) and the `authors_papers` link table before touching persistence,
mirroring legacy `update_entry`.

## Import service — `services/import_service.py`

```python
def extract_papers_from_tex_bib(tex: str, bib: str) -> Iterator[PaperCreate]: ...
```

Parses the `.tex` overview (pylatexenc) for cited titles + keys, joins against
the `.bib` (pybtex) for authors + source, yields one `PaperCreate` per matched
entry. Unmatched keys are skipped (caller logs a warning). The CLI driver
commits **per paper** so a partial failure leaves earlier papers persisted
(FR-005 AC-3 / Principle IV).
