# Legacy Architecture (reverse-engineered)

This document is the reverse-engineered description of the **pre-modernization** paper-database
tool. It is the acceptance reference for the modernization: the rebuilt system must do
everything described here. It deliberately describes the *legacy* stack (the flat-layout
`paper_sorts/` package that the modernization replaced), so it remains accurate when reading
historical commits or interpreting the seed dataset's edge-case coverage.

## Purpose

A single-user, offline CLI for storing and querying publication metadata (title, authors, a
one-line summary, and a full BibTeX entry) in a local PostgreSQL database — for use when no
online resource is reachable (the README cites "traveling by train"). It is a personal tool, not
a library or service.

## User journeys

The user launches the program and is shown a four-option top-level menu:

1. Search the database (by author or by paper title),
2. Add an entry (typing the fields, with the BibTeX entry typed inline or read from a file),
3. Update an entry (title / contents / bibtex / author, with a confirmation step),
4. Quit.

Two further capabilities exist as standalone scripts rather than menu options: a **bulk import**
from a `.tex` literature overview plus a matching `.bib` file (`get_data.py`), and a
**single-entry add** from one `.bib` file (`add.py`).

## Data model

Four tables (created lazily by `DatabaseConnector.create_tables()`):

- `papers(id SERIAL PK, title TEXT, contents TEXT, bibtex_id TEXT → bib.bibtex_id)`
- `bib(bibtex_id TEXT PK, bibtex TEXT UNIQUE)`
- `authors_id(id SERIAL PK, author TEXT)`
- `authors_papers(id SERIAL PK, author_id INT, paper_id INT)` — the many-to-many link, with
  **no declared foreign keys**.

A paper is identified internally by `papers.id`; the BibTeX key (`bibtex_id`) is the
user-facing unique identifier and the FK target from `papers` into `bib`. Authors are stored
once in `authors_id` and linked to papers through `authors_papers`. **Two authors with an
identical `"Last, First"` string are treated as the same author** — a documented limitation.

### Schema variants

Two historical column-naming variants exist in the wild:

- the object-oriented stack (`DatabaseConnector`) uses the canonical `bibtex_id` / `bibtex`;
- the older procedural scripts (`add.py`, `get_data.py`, `search.py`) use the misspelled
  `bibtext_id` and a `bibtext` data column, and import `psycopg` v3 rather than `psycopg2`.

A migration must converge either variant onto the canonical schema with zero data loss.

## Control flow

```
UserInteraction (CLI dialog: input()/print())
        │  drives based on menu choices
        ▼
DatabaseConnector (domain ops + hand-written SQL strings + transaction logic)
        │  delegates every query to
        ▼
PsycopgDB (the only module importing psycopg2: store/fetch/delete/update)
        │
        ▼
PostgreSQL
```

- **`UserInteraction`** — the only place stdin/stdout is touched. `get_user_input` re-prompts
  on empty input; `get_user_choice` shows a 1-indexed disambiguation list and re-prompts on
  out-of-range; confirmation accepts numeric (`1`/`2`) and word (`y`/`n`/`yes`/`no`) forms.
- **`DatabaseConnector`** — high-level operations (`search_by_author`, `search_by_title`,
  `add_entry_to_db`, `update_entry`, `delete_paper_entry_from_database`) plus the multi-step
  transactional logic: an add inserts the bib row, the paper row, then links each author,
  rolling back the bib entry and any partial author links if a later step fails
  (`rollback_database_addition`). `update_entry` dispatches over the table via `match`/`case`
  and refuses to touch `authors_papers`.
- **`PsycopgDB`** — a thin wrapper exposing `store_in_db` / `fetch_from_db` / `delete_from_db` /
  `update_db_entry`, each opening a connection, executing a parameterised query, committing or
  rolling back, and closing. This is the single point of change if the driver is swapped.

### What happens if a partial add fails midway?

`add_entry_to_db` deletes the just-inserted bib entry and unwinds any author-paper links created
so far (`rollback_database_addition` → `rollback_author_addition`), then raises a `ValueError`
with a plain message; technical detail goes to the log. The database is left in its
pre-add state.

## Configuration

Database credentials live in a Fernet-encrypted INI file (default `../../database.crypt`)
decrypted at startup with a key file (default `../../key`). The INI's `[postgresql]` section
carries `dbname` / `user` / `password`. A missing key file (lost key) must produce a clear,
actionable error rather than a stack trace.

## Logging

Each class builds its own file-backed logger via `helpers.create_logger`
(`db_connector.log`, `psycopg_logger.log`, `interaction.log`, …). Failures are logged in full;
the user sees only a short plain-language message.

## Install / run (legacy)

Dependencies were managed with Poetry (Python `^3.10`). The program was launched from
`paper_sorts/run.py` with `-c <config> --section <section> -k <key>`; the argparse defaults
(`../../database.crypt`, `../../key`) assume launching from inside `paper_sorts/`.

## Known limitations and quirks

- Duplicate `"Last, First"` author strings collapse to one author.
- `authors_papers` has no foreign keys, so referential integrity of links is enforced only in
  code.
- The legacy test suite was an integration test silently coupled to a developer-local database
  (`"Pino, J."`, `"Wang2021LargeScaleSA"`) plus an always-failing placeholder test.
- The procedural scripts (`add.py`/`search.py`/`get_data.py`) and the OO stack disagree on
  column spelling (`bibtext_id` vs `bibtex_id`) and driver (`psycopg` vs `psycopg2`).
- Hand-written SQL strings throughout `DatabaseConnector` — the correctness of which only the
  integration tests protected.

## What the modernization changes (pointer)

The rebuilt stack replaces this with a src-layout package (`src/paper_sorts/`): SQLAlchemy 2.x
over psycopg v3 (isolated to `db/`), a Typer CLI routing prompts through `cli/prompts.py`,
Alembic migrations, pydantic-settings configuration, a real-DB pytest suite, and ruff. See
`specs/001-modernize-stack/` for the plan and contracts.
