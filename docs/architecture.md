# Architecture (reverse-engineered legacy stack)

This document captures the **pre-modernization** design of `paper_sorts` — the
flat-layout, hand-written-SQL version that existed before the modern src-layout
rebuild. It is the acceptance reference for the modernization: the rebuilt system
must reproduce every user-facing behaviour described here. It is also a guide for
reading historical commits and for understanding the edge cases the seed dataset
(`tests/fixtures/seed_papers.py`) was designed to cover.

> The modern stack (SQLAlchemy 2.x, Typer, Alembic, pydantic-settings, pytest)
> is documented in `CLAUDE.md`, `README.md`, and `specs/001-modernize-stack/`.
> This file is intentionally about the *old* design.

## Purpose

An offline, single-user CLI to store and search publication metadata — title,
authors, one-sentence summary, and the full BibTeX entry — in a local PostgreSQL
database. It is a personal tool, not a library or a service: no network surface,
no multi-user access, no authentication.

## User journeys

The legacy entry point (`paper_sorts/run.py`) parsed command-line arguments for
the encrypted config path and key, then dropped the user into a numbered
top-level menu:

1. **Search the database** — by author or by paper title. A title search that
   matched several papers asked the user to disambiguate from a numbered list.
   Output was a "pretty print" of title, authors, summary, and BibTeX entry.
2. **Add an entry** — the user typed the authors, title, BibTeX key, and summary,
   then either pointed at a `.bib` file or typed the BibTeX entry inline. The new
   paper, its BibTeX record, and its author links were written together.
3. **Update an entry** — pick a table, pick an editable column (IDs were never
   editable; the `bib` key was immutable), give the row identifier and the new
   value, then confirm. Declining the confirmation wrote nothing.
4. **Delete an entry** — identify a paper, see it summarised, confirm, and remove
   it along with its authorship links and any authors left with no papers.
5. **Quit.**

A separate bulk-import path (`paper_sorts/get_data.py`) read a `.tex` literature
overview plus its `.bib` file and inserted every cited entry that had a matching
BibTeX record, one paper at a time.

Empty input on a required prompt re-prompted until something non-empty was
entered (`helpers.get_user_input`). An out-of-range menu choice re-prompted.

## Data model

Four tables in PostgreSQL, created lazily at runtime by
`DatabaseConnector.create_tables()`:

| Table | Columns | Notes |
|-------|---------|-------|
| `papers` | `id` (PK), `title`, `contents`, `bibtex_id` → `bib.bibtex_id` | internal `id`; `bibtex_id` is the user-facing key |
| `bib` | `bibtex_id` (PK), `bibtex` (unique) | the full BibTeX source string |
| `authors_id` | `id` (PK), `author` | name in `"Last, First"` form |
| `authors_papers` | `id` (PK), `author_id`, `paper_id` | many-to-many link, **no DDL foreign keys** |

Relationships:

- A **paper** has exactly one **BibTeX entry** (`papers.bibtex_id = bib.bibtex_id`).
- A **paper** has one or more **authors** through the `authors_papers` link table.
- The link table carries no database-level foreign keys; the references are
  logical only. This is a deliberate part of the schema contract — do not add
  NOT NULL outside primary keys, foreign keys to `authors_papers`, or indexes the
  original DDL lacked.

**Where would I add a new field?** A new paper attribute is a new column on
`papers` (plus the read/write paths that surface it); a new BibTeX attribute
lives on `bib`. In the modern stack this is an ORM column on the model in
`src/paper_sorts/db/models.py` and an Alembic migration; in the legacy stack it
was a change to the `create_tables()` DDL string and the relevant SQL in
`DatabaseConnector`.

## Control flow (CLI dialog → domain → persistence)

Three layers, top to bottom:

1. **`UserInteraction`** (`paper_sorts/user_interaction.py`) — all CLI dialog
   (`input()` / `print()`). The only place stdin/stdout was touched. It drove a
   `DatabaseConnector` based on the user's menu choices.
2. **`DatabaseConnector`** (`paper_sorts/database_connector.py`) — the high-level
   domain operations: `search_by_author`, `add_entry_to_db`, `update_entry`,
   `delete_paper_entry_from_database`, and `rollback_database_addition`. It held
   the SQL strings and the multi-step transactional logic.
3. **`PsycopgDB`** (`paper_sorts/psycopg_db.py`) — a thin wrapper over the
   PostgreSQL driver exposing `store_in_db`, `fetch_from_db`, `delete_from_db`,
   `update_db_entry`. Per its docstring this was the *only* module that imported
   the driver, so swapping drivers was a single point of change.

A "search by author" therefore flowed: menu choice in `UserInteraction` →
`DatabaseConnector.search_by_author(name)` builds the parameterised SQL and joins
→ `PsycopgDB.fetch_from_db(...)` runs it → results bubble back up and are
pretty-printed.

### Rollback semantics (what happens if a partial add fails midway)

Adding a paper was multi-step: insert the paper row, insert the BibTeX row, then
link each author. If a later step failed, `DatabaseConnector` called
`rollback_database_addition` to undo the rows already written, so a half-inserted
paper never persisted. The modern stack preserves this guarantee through a
session context manager that commits on success and rolls back on any exception.

## Configuration

Database credentials were stored in a **Fernet-encrypted INI file**
(`config_reader.py`) decrypted at startup with a separate key file. The INI
section was `[postgresql]` with `dbname` / `user` / `password`. The default
argparse paths (`../../database.crypt`, `../../key`) assumed the program was
launched from inside the `paper_sorts/` directory.

The modern stack keeps the encrypted-INI workflow as one supported source and
adds environment variables and a `.env` file in a priority chain.

## Install / run (legacy)

Dependencies were managed with the legacy packaging tool; the program was run as
`python paper_sorts/run.py -c <config> --section <section> -k <key>`. Tests were
an integration suite that required a live PostgreSQL seeded with specific rows
(`Pino, J.`, `Wang2021LargeScaleSA`) — they could not run on a fresh checkout
without that developer-local database. Removing that hidden dependency was one of
the modernization's explicit goals.

## Known limitations and quirks

- **Duplicate-author identity.** Two distinct people with the same
  `"Last, First"` string were treated as one author. This is unchanged in the
  modern stack and is a documented limitation, not a bug.
- **The `bibtext_id` typo schema.** Older procedural modules (`add.py`,
  `search.py`, `get_data.py`) used the column name `bibtext_id` (sic) and the
  `bibtext` source column, and imported a different driver version than the OO
  stack's `bibtex_id` / `bibtex`. A personal database could be in either schema.
  The modern `migrate` command converges the legacy typo columns onto the
  canonical names idempotently, preserving every row.
- **Lazy table creation.** The schema was created at runtime rather than through
  versioned migrations, so there was no upgrade path between schema variants.
  The modern stack replaces this with Alembic revisions.
- **Per-class log files.** Each class built its own file-backed logger via
  `helpers.create_logger`. The modern stack replaces this with configurable
  sinks (stdout by default, optional file).
- **LaTeX accents in BibTeX.** Entries containing LaTeX accents/escapes
  (`\"o`, `\&`, `{Pino}`) had to round-trip through the BibTeX parser without
  corruption when displayed — a behaviour the seed dataset and import tests guard.
- **Ctrl+C mid-dialog.** Interrupting a dialog exited without leaving the
  database in an inconsistent partial state (thanks to the rollback semantics).
