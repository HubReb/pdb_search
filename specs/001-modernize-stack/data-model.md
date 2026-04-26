# Phase 1 Data Model: Modernize the Stack

**Feature**: 001-modernize-stack
**Date**: 2026-04-26

This document describes the domain entities and their ORM mappings for the modernized stack. The schema is **identical in shape** to the current four-table layout — modernization is a refactor of the access path, not a rework of the storage model. The only schema-level change is normalising the historical `bibtext_id` column name (legacy `add.py`/`search.py`/`get_data.py`) to `bibtex_id` (as already used by the OO `DatabaseConnector`); see Migration 002 in [contracts/database-schema.md](./contracts/database-schema.md).

## Application-level invariants vs. schema-level constraints

This codebase is unusual in that the database schema is far looser than the data the application actually produces. The original `DatabaseConnector.create_tables()` declares only column types and one foreign key (`papers.bibtex_id → bib.bibtex_id`). Everything else — non-empty titles, valid BibTeX, well-formed author names, link integrity between `authors_papers` and its targets — is enforced *by the application*, not by the schema. Modernization preserves this split exactly.

Why preserve it: Revision 001 has to succeed against any current personal database. Adding `NOT NULL` or extra foreign keys at the schema level can fail the migration on a row that the application happens to never produce in clean steady-state but that the original schema permits. A safe modernization keeps the schema a pure superset of what existed and moves all tightening to the application layer (where it already lives anyway).

| Invariant | Schema-enforced? | Application-enforced? | Enforcement site (modernized) |
|-----------|------------------|-----------------------|-------------------------------|
| `papers.title` is non-empty | no | yes | `cli/prompts.py` re-prompts on empty input |
| `papers.contents` (summary) is non-empty | no | yes | `cli/prompts.py` re-prompts on empty input |
| `papers.bibtex_id` points at an existing `bib` row | yes (FK) | yes (sanity check before insert) | DDL FK; `services/paper_service.py` `add` flow |
| `bib.bibtex_id` is unique | yes (PK) | yes (sanity check) | DDL PK; `services/paper_service.py` `add` flow |
| `bib.bibtex` source is unique across rows | yes (`UNIQUE`, preserved from original) | yes (pybtex parse before insert) | DDL `UNIQUE`; `services/import_service.py` |
| `bib.bibtex` parses as valid BibTeX | no | yes | `services/import_service.py` (pybtex) |
| `authors_id.author` (the name) is non-empty | no | yes | `cli/prompts.py`, `services/import_service.py` |
| `authors_papers.author_id` points at a real author row | no | yes (the link is only created with an id the same flow just inserted or fetched) | `services/paper_service.py` add/update flows |
| `authors_papers.paper_id` points at a real paper row | no | yes (same mechanism) | `services/paper_service.py` |
| `authors_papers.(author_id, paper_id)` is unique | no | **no** (duplicates are tolerated — preserved current behaviour, documented quirk) | n/a; out of scope for this work |
| Author rows are deduplicated by exact `author` string | no | yes (upsert by name) | `db/repositories.py::AuthorRepository.upsert` |

**Reader rule**: if an invariant is not in this table, it is not enforced anywhere. Adding a new invariant means adding both a schema migration (with a Complexity Tracking entry per constitution Principle IV) *and* a code path that defends it in the application layer. Tightening one without the other is the kind of change that breaks Revision 001 against existing personal data.

## Entities

### Paper

A publication record.

| Field      | Type | Schema constraint (per Revision 001 — verbatim from the existing `create_tables()`) |
|------------|------|--------------------------------------------------------------------------------------|
| id         | int  | Primary key, auto-incrementing                                                       |
| title      | str  | None at the schema level (the original column is bare `TEXT`)                        |
| contents   | str  | None at the schema level                                                             |
| bibtex_id  | str  | Foreign key → `bib.bibtex_id` (the only FK the original schema declares)             |

Relationships:
- Many-to-one with `BibEntry` (`paper.bib_entry`).
- Many-to-many with `Author` via the `authors_papers` link table (`paper.authors` ordered list).

Application-level validation (NOT enforced by the schema; preserves current behaviour):
- `title`, `contents`, and `bibtex_id` are required by the prompt grammar — empty input re-prompts (constitution Principle III). The import service validates the BibTeX entry via pybtex before insert.
- `bibtex_id` is checked for prior existence in `bib` before insert (preserves current `sanity_checks` behaviour). If the bib row has been deleted out from under the FK, the insert fails with a plain-language error.

State transitions: none. Paper is immutable except via the `update` flow, which mutates `title` or `contents` in place.

### Author

A person credited on one or more papers. The table is named `authors_id` and the name column is `author` — both preserved verbatim from the original schema.

| Field | Type | Schema constraint                  |
|-------|------|------------------------------------|
| id    | int  | Primary key, auto-incrementing     |
| name  | str  | None at the schema level (original column is bare `TEXT` named `author`) |

Relationships:
- Many-to-many with `Paper` via the `authors_papers` link table.

Application-level validation:
- Author name is provided by the prompt grammar or the import service; both sources guarantee non-empty values. The schema permits NULL but the application never produces it.
- Name format `"Last, First"` is conventional, not enforced. The import service produces this format from `pybtex` author records (`author.last_names[0]`, `author.first_names[0]`).
- Two authors with identical `name` strings are treated as the same author. This is a documented limitation, not a bug (spec edge case).

### BibEntry

The full BibTeX source string for a paper.

| Field      | Type | Schema constraint                                                    |
|------------|------|----------------------------------------------------------------------|
| bibtex_id  | str  | Primary key (implicit NOT NULL via PK)                               |
| bibtex     | str  | None on null; table-level `UNIQUE (bibtex)` (preserved from original) |

Relationships:
- One-to-many with `Paper` (logically one-to-one — the Paper FK is unique-by-construction in this app, but the schema does not enforce uniqueness on the Paper side because `bibtex_id` is not unique-indexed on `papers`. Modernization preserves this current behaviour to avoid altering a non-empty production database).

Application-level validation:
- `bibtex_id` uniqueness on insert is enforced by the application's `add` flow (`sanity_checks` equivalent). The PK constraint also enforces this at the schema level.
- `bibtex` content is not parsed or validated by the database; the import service validates via pybtex before insert.
- `bibtex` uniqueness across rows is enforced by the schema's table-level `UNIQUE (bibtex)` clause — preserved verbatim from the original.

### Authorship

Many-to-many link between `Paper` and `Author`. Table name `authors_papers`, preserved verbatim.

| Field      | Type | Schema constraint                                          |
|------------|------|------------------------------------------------------------|
| id         | int  | Primary key, auto-incrementing                             |
| author_id  | int  | None at the schema level — bare `INT`, **no FK** in the original |
| paper_id   | int  | None at the schema level — bare `INT`, **no FK** in the original |

Relationships:
- Pure link entity. Surfaced in code as a SQLAlchemy `relationship(secondary=...)`. The model code MAY use `ForeignKey(...)` in the SQLAlchemy mapping for ORM-side join navigation, but Revision 001 does NOT emit `FOREIGN KEY` DDL on the actual table — preserving the original.

Application-level validation:
- A given (`author_id`, `paper_id`) pair MAY repeat in the table (current behaviour: the `add` flow inserts one link row per author per paper, and re-running an import creates duplicates). Documented quirk, preserved for compatibility. A future spec may normalise it; **out of scope** here.
- Dangling `author_id` or `paper_id` (referencing a row that has since been deleted) is permitted by the schema and possible against existing personal databases. The application's `delete` flow cleans up its own links; pre-existing dangling links from earlier scripts are tolerated, not repaired by the migration.

### Configuration (in-memory only — not persisted)

Loaded by `paper_sorts/config.py` via pydantic-settings.

| Field            | Type | Source                                         |
|------------------|------|------------------------------------------------|
| database_url     | str  | env `PDBSEARCH_DATABASE_URL` / `.env` / Fernet INI key `[postgresql] dbname/user/password/host/port` decoded to a URL |
| log_level        | str  | env `PDBSEARCH_LOG_LEVEL` / default `INFO`     |
| log_file         | path | env `PDBSEARCH_LOG_FILE` / default unset (stdout only) |
| fernet_config    | path | optional; if set, points at the encrypted INI |
| fernet_key       | path | optional; required iff `fernet_config` is set  |

Validation:
- `database_url` must be set (from any source) before the app starts.
- If `fernet_config` is set without `fernet_key`, the load fails with a plain-language error (spec edge case: lost key produces a clear error, not a stack trace).

### Migration (Alembic revision)

Versioned, idempotent change to the schema. Not a runtime entity — covered in [contracts/database-schema.md](./contracts/database-schema.md).

## Entity-relationship summary

```
                      ┌──────────────┐
                      │   BibEntry   │
                      │ bibtex_id PK │
                      │ bibtex       │
                      └──────┬───────┘
                             │ 1
                             │
                             │ many
                      ┌──────▼───────┐         ┌─────────────┐
                      │    Paper     │ many to │  Authorship │ many to ┌────────┐
                      │ id PK        ├─────────┤  paper_id   ├─────────┤ Author │
                      │ title        │  many   │  author_id  │  many   │ id PK  │
                      │ contents     │         │             │         │ name   │
                      │ bibtex_id FK │         │             │         │        │
                      └──────────────┘         └─────────────┘         └────────┘
```

## SQLAlchemy 2.x mapping sketch

The full mapping lives in `src/paper_sorts/db/models.py`. Sketch (typed, 2.x style). Columns that are bare `TEXT` (i.e. nullable) in the actual schema are typed `Mapped[str | None]` here — accurately modelling what the DB permits, not what the application happens to produce. Likewise the `ForeignKey(...)` arguments on `authors_papers.author_id` / `paper_id` are intentionally absent so that the ORM `MetaData` matches the schema 1:1; ORM-side joins use explicit `relationship(primaryjoin=..., secondaryjoin=...)` instead of FK reflection.

```python
class Base(DeclarativeBase):
    pass

class BibEntry(Base):
    __tablename__ = "bib"
    bibtex_id: Mapped[str] = mapped_column(primary_key=True)
    bibtex: Mapped[str | None] = mapped_column(unique=True)   # UNIQUE (bibtex) preserved

class Paper(Base):
    __tablename__ = "papers"
    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str | None]
    contents: Mapped[str | None]
    bibtex_id: Mapped[str | None] = mapped_column(ForeignKey("bib.bibtex_id"))   # the only FK the original declares
    bib_entry: Mapped["BibEntry | None"] = relationship()
    authors: Mapped[list["Author"]] = relationship(
        secondary="authors_papers",
        primaryjoin="Paper.id == foreign(Authorship.paper_id)",
        secondaryjoin="Author.id == foreign(Authorship.author_id)",
        viewonly=False,
    )

class Author(Base):
    __tablename__ = "authors_id"          # historical name preserved
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str | None] = mapped_column("author")   # historical column name preserved

class Authorship(Base):
    __tablename__ = "authors_papers"
    id: Mapped[int] = mapped_column(primary_key=True)
    author_id: Mapped[int | None]   # bare INT in DDL — no FK constraint emitted
    paper_id: Mapped[int | None]    # bare INT in DDL — no FK constraint emitted
```

The `authors_id` / `author` table and column names, plus the absence of FKs on `authors_papers` and the absence of NOT NULL anywhere, are all preserved verbatim from the original `create_tables()`. Spec FR-011 keeps the migration footprint to the minimum needed to converge the historical `bibtext_id` / `bibtex_id` rename, and *only* that.

## Repository surface (the "interface" `services/` depends on)

`services/` MUST NOT import `sqlalchemy` directly. It calls these repository methods:

- `PaperRepository.find_by_title(title: str) -> list[PaperSummary]`
- `PaperRepository.find_by_author(author_name: str) -> list[PaperSummary]`
- `PaperRepository.add(paper: PaperCreate) -> Paper`
- `PaperRepository.update_field(paper_id: int, field: Literal["title", "contents"], value: str) -> None`
- `PaperRepository.delete(paper_id: int) -> None`
- `BibRepository.add(bibtex_id: str, bibtex: str) -> None`
- `BibRepository.update(bibtex_id: str, bibtex: str) -> None`
- `AuthorRepository.upsert(name: str) -> Author`
- `AuthorRepository.update_name(author_id: int, name: str) -> None`

`PaperSummary` and `PaperCreate` are pydantic models defined alongside the repositories so that `services/` and `cli/` can use them without crossing the SQLAlchemy boundary.
