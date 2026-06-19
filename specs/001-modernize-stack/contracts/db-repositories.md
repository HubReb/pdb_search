# Contract: Persistence Layer (`db/`) and Service Layer (`services/`)

These are the in-process interfaces between layers. ORM/SQLAlchemy types never appear in signatures
crossing out of `db/` — only DTOs and primitives (Principle I isolation).

## Session (`db/session.py`)

```python
def make_engine(database_url: str) -> Engine: ...

@contextmanager
def with_session(engine: Engine) -> Iterator[Session]:
    """Yield a Session; commit on clean exit, rollback on exception, always close."""
```

Deterministic close + commit/rollback semantics (Principle IV: no long-lived sessions).

## Repositories (`db/repositories.py`)

All methods take a `Session` and return DTOs / primitives.

```python
class PaperRepository:
    def get_by_title(self, s: Session, title: str) -> list[PaperSummary]: ...
    def add(self, s: Session, paper: PaperCreate) -> int: ...          # returns papers.id
    def update_title(self, s: Session, paper_id: int, value: str) -> None: ...
    def update_contents(self, s: Session, paper_id: int, value: str) -> None: ...
    def delete(self, s: Session, paper_id: int) -> None: ...
    def exists_bibtex_id(self, s: Session, bibtex_id: str) -> bool: ...

class AuthorRepository:
    def get_papers_by_author(self, s: Session, author: str) -> list[PaperSummary]: ...
    def rename(self, s: Session, author_id: int, value: str) -> None: ...
    def link(self, s: Session, author: str, paper_id: int) -> None: ...   # create author if absent
    def unlink_all_for_paper(self, s: Session, paper_id: int) -> None: ... # drop orphan authors

class BibRepository:
    def add(self, s: Session, bibtex_id: str, bibtex: str) -> None: ...
    def update(self, s: Session, bibtex_id: str, bibtex: str) -> None: ...  # rejects duplicate bibtex
    def delete(self, s: Session, bibtex_id: str) -> None: ...
```

Queries use parameterised SQLAlchemy expressions with joins over the four-table schema (Principle IV).

## Services (`services/paper_service.py`)

Pure orchestration over the repositories; no SQL, no rich, no I/O. Each opens a `with_session`.

```python
def search_by_title(self, title: str) -> list[PaperSummary]: ...
def search_by_author(self, author: str) -> list[PaperSummary]: ...
def add_paper(self, paper: PaperCreate) -> None: ...                 # bib + paper + author links, rollback on failure
def update_field(self, table: Literal["papers","bib","authors_id"],
                 column: str, identifier: str, new_value: str) -> None: ...
def delete_paper(self, paper_id: int) -> None: ...                   # links (orphan authors) + paper + bib
```

`update_field` dispatches on `table` via `match`/`case` with `assert_never(table)` for compile-time
exhaustiveness. Refuses `*_id` columns and the `authors_papers` table.

## Import service (`services/import_service.py`)

```python
def extract_papers_from_tex_bib(tex: str, bib: str) -> Iterator[PaperCreate]:
    """Yield one PaperCreate per cited entry that has a matching .bib record.
    Citation keys with no .bib match are skipped (caller logs a warning)."""
```

The `import` CLI iterates this and calls `add_paper` per item (per-paper commit, FR / Principle IV).

## Migration (`migrations/`)

- Revision 001: create the four tables exactly as the legacy canonical DDL (`bibtex_id` schema,
  `bib.bibtex UNIQUE`, `papers.bibtex_id` FK → `bib`). No extra NOT NULL/FK/index.
- Revision 002: rename `bibtext_id`/`bibtext` → `bibtex_id`/`bibtex` where present (guarded on
  `information_schema`), idempotent, transactional.
