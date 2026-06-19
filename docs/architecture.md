# Legacy Architecture (pre-modernization)

This document reverse-engineers the **legacy** Paper Sorts stack as it existed
before the 001-modernize-stack rebuild. It is the acceptance reference for the
modernization: the rebuilt system must reproduce every behaviour described here.
(The modern stack supersedes this; see `README.md` and `specs/001-modernize-stack/`.)

## Purpose

Paper Sorts is a personal, offline, single-user command-line tool. It stores
publication metadata — title, authors, a one-sentence summary, and the full
BibTeX source — in a local PostgreSQL database, and lets the owner search, add,
update, delete, and bulk-import entries. It is a desk tool ("traveling by
train"), not a library or a service: there is no network surface, no
authentication, and no concurrency.

## User journeys

The program starts via `paper_sorts/run.py`, which decrypts database credentials,
connects, and drops the user into a four-option text menu:

```
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
```

- **Search** asks "by author" or "by paper title". By title: one match prints the
  record; multiple distinct-title matches show a numbered list to disambiguate. By
  author: all of that author's papers are gathered, disambiguated if several, then
  the chosen record is printed. The print format is:
  `title / authors (joined by " and ") / summary / bib entry`.
- **Add** prompts for a comma-separated author list, the title, the BibTeX key,
  then offers to read the BibTeX entry from a file or inline, then the summary. It
  inserts the bib row, the paper row, and the author links — rolling back the bib
  row and any partial author links if a later step fails.
- **Update** chooses a table (papers / bib / authors), a column, the entry id, and
  the new value, then asks for confirmation (numeric `1`/`2` or word `y`/`n`)
  before writing. `*_id` columns are immutable.
- **Delete** removes a paper and its bib row and unlinks its authors (deleting
  authors left with no papers).
- **Bulk import** (a separate script, `paper_sorts/get_data.py`) reads a LaTeX
  literature overview plus a `.bib` file and inserts every cited entry that has a
  matching BibTeX record, committing per paper.

Any required prompt re-prompts until the user types something non-empty.

## Data model

Four tables (created lazily by `DatabaseConnector.create_tables()`):

- `papers(id SERIAL PK, title TEXT, contents TEXT, bibtex_id TEXT → bib.bibtex_id)`
- `bib(bibtex_id TEXT PK, bibtex TEXT UNIQUE)`
- `authors_id(id SERIAL PK, author TEXT)` — author name as `"Last, First"`
- `authors_papers(id SERIAL PK, author_id INT, paper_id INT)` — the many-to-many
  link table, with **no** database-level foreign keys.

Relationships: each paper points at exactly one bib entry (`papers.bibtex_id` FK);
papers and authors are many-to-many through `authors_papers`. A paper is
identified internally by `papers.id`; the user-facing unique identifier is the
BibTeX key `bibtex_id`.

**Where would I add a new field?** A new paper attribute is a column on `papers`
plus a touch in the add/search/update SQL in `DatabaseConnector`; in the modern
stack it is a column on the `Paper` ORM model, an Alembic migration, and a field
on the `PaperSummary`/`PaperCreate` DTOs.

## Control flow (three layers)

1. **Presentation** — `paper_sorts/user_interaction.py` (`UserInteraction`): all
   `input()`/`print()` dialog. Drives the connector based on user choices.
2. **Domain** — `paper_sorts/database_connector.py` (`DatabaseConnector`):
   high-level operations (`search_by_author`, `add_entry_to_db`, `update_entry`,
   `delete_paper_entry_from_database`, plus the multi-step add and its rollback).
   Holds the hand-written SQL strings.
3. **Persistence** — `paper_sorts/psycopg_db.py` (`PsycopgDB`): a thin wrapper
   around the database driver (`store_in_db` / `fetch_from_db` / `delete_from_db`
   / `update_db_entry`), the single point where the driver is imported.

Supporting: `paper_sorts/config_reader.py` decrypts a Fernet-encrypted INI for
credentials; `paper_sorts/helpers.py` holds shared pure functions (logger
construction, safe int cast, prompt helpers, pretty-print, LaTeX/BibTeX parsing).

A second, older procedural stack (`paper_sorts/add.py`, `search.py`,
`get_data.py`) duplicates some of this functionality using a different driver and
the typo column name `bibtext_id`. It is not wired into `run.py`.

## Configuration

Credentials live in a Fernet-encrypted INI file (default `../../database.crypt`)
decrypted with a key file (default `../../key`), section `[postgresql]` with
`dbname`/`user`/`password`. Paths are overridable via `-c`/`-k`/`--section`.

## Install / run

Poetry-managed, Python ^3.10. `poetry install`, then
`poetry run python paper_sorts/run.py -c <config> -k <key>`. The legacy test suite
(`tests/test_database_connector.py`) is an integration test that requires a live
database seeded with specific rows; `tests/test_user_interaction.py` is an
always-failing placeholder.

## Known limitations and quirks

- **Schema split**: the OO stack uses `bibtex_id`; the older procedural scripts
  use `bibtext_id` (sic). A database created by the old scripts has the typo
  columns — the modernization migration must converge both.
- **Duplicate authors**: two distinct people with the same `"Last, First"` string
  collapse to a single `authors_id` row. Documented, not fixed.
- **Mid-add rollback**: if adding authors fails partway through `add_entry_to_db`,
  the connector deletes the just-inserted bib row and any author links already
  created, so a partial add does not leave the database inconsistent.
- **Interrupted bulk import**: rerunning skips already-imported BibTeX keys
  (enforced by the `bib` primary-key / uniqueness), so entries are not duplicated.
- **Lost key**: if the encrypted config is present but the key file is missing or
  wrong, the legacy code raises rather than printing a clear message — the
  modernization fixes this to an actionable error.
