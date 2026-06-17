# Repository / Service API Contract

The persistence layer (`db/`) exposes repositories returning **DTOs**, never ORM
instances. The service layer (`services/`) orchestrates them. Only `db/` imports
`sqlalchemy`/`psycopg` (Principle I).

## Session (`db/session.py`)

```python
def with_session(engine: Engine) -> ContextManager[Session]: ...
```
Context manager: commit on success, rollback on exception, deterministic close
(Principle IV — no long-lived sessions). All repository calls run inside one.

## Repositories (`db/repositories.py`)

```python
class PaperRepository:
    def __init__(self, session: Session) -> None: ...
    def search_by_title(self, title: str) -> list[PaperSummary]: ...
    def search_by_author(self, author: str) -> list[PaperSummary]: ...
    def add(self, paper: PaperCreate) -> None: ...
    def update_title(self, paper_id: int, value: str) -> None: ...
    def update_contents(self, paper_id: int, value: str) -> None: ...
    def delete(self, paper_id: int) -> None: ...

class AuthorRepository:
    def __init__(self, session: Session) -> None: ...
    def rename(self, author_id: int, new_name: str) -> None: ...

class BibRepository:
    def __init__(self, session: Session) -> None: ...
    def update_bibtex(self, bibtex_id: str, value: str) -> None: ...
```

- `search_by_title`/`search_by_author` use parameterised joins over the four
  tables (Principle IV); return `PaperSummary` DTOs with `" and "`-joined
  authors (legacy pretty-print parity).
- `add` is atomic: bib → paper → author links in one transaction.
- `delete` removes links, then orphaned authors, then paper + bib.

## DTOs

```python
class PaperSummary(BaseModel):
    paper_id: int
    title: str
    authors: str        # " and "-joined "Last, First" names
    bibtex_id: str
    contents: str
    bibtex: str

class PaperCreate(BaseModel):
    title: str
    contents: str
    bibtex_id: str
    bibtex: str
    authors: list[str]  # each "Last, First"
```

## Services

```python
# services/paper_service.py
def search_by_title(engine, title: str) -> list[PaperSummary]: ...
def search_by_author(engine, author: str) -> list[PaperSummary]: ...
def add_paper(engine, paper: PaperCreate) -> None: ...
def update_field(
    engine,
    table: Literal["papers", "bib", "authors_id", "authors_papers"],
    column: str,
    value: str,
    identifier: str,
) -> None: ...   # match/case + assert_never(table) for exhaustiveness
def delete_paper(engine, paper_id: int) -> None: ...

# services/import_service.py
def extract_papers_from_tex_bib(tex: str, bib: str) -> Iterator[PaperCreate]: ...
```

- `update_field` rejects ID columns and the `authors_papers` table (no editable
  column), mirroring legacy `update_entry`.
- `extract_papers_from_tex_bib` yields one `PaperCreate` per cited entry that has
  a matching `.bib` record; unmatched keys are skipped (logged warning) so the
  caller can commit per paper.
- Services raise typed domain errors (e.g. `PaperNotFoundError`,
  `DuplicateBibtexError`, `ConfigError`) that the CLI maps to plain messages.
