# Architecture: paper_sorts (Legacy / Pre-Modernization)

This document describes the `paper_sorts` application **as it existed before the 001-modernize-stack refactor**. It is the acceptance reference for User Story 2 (the rebuilt system must preserve everything described here) and the baseline for User Story 1 (readability for a new contributor).

---

## Purpose

`paper_sorts` is an offline, personal-use CLI tool for managing a local library of publication metadata. A single user stores paper titles, authors, summaries, and BibTeX entries in a local PostgreSQL database, and retrieves them interactively by title or author name.

---

## User Journeys

### 1. Search by Author
1. Start `pdbsearch`.
2. Choose "Search the database" → "Search by author".
3. Enter author name (e.g. `Pino, J.`).
4. Application returns all papers by that author: title, summary, BibTeX.

### 2. Search by Title
1. Start `pdbsearch`.
2. Choose "Search the database" → "Search by paper title".
3. Enter (partial) title.
4. If multiple matches: numbered disambiguation list; user selects one.
5. Application returns paper details.

### 3. Add a Paper (Inline)
1. Choose "Add an entry".
2. Enter author(s) (comma-separated `Last, First` names), title, bibtex key, summary.
3. Optionally point at a `.bib` file for the BibTeX string; otherwise type it inline.
4. Entry is inserted into `papers`, `bib`, `authors_id`, `authors_papers`.

### 4. Update a Field
1. Choose "Update an entry".
2. Select table: papers / bib / authors.
3. Select field: title / contents / bibtex / author name.
4. Enter the identifier of the entry to change.
5. Enter new value.
6. Confirm (`y`/`1`) or abort (`n`/`2`).

### 5. Delete a Paper
1. Choose "Delete an entry" (accessed via update flow in legacy code).
2. Enter identifier; confirm deletion.
3. Paper, its BibTeX entry, and orphan author links are removed.

### 6. Bulk Import
Run `python -m paper_sorts.get_data` (legacy) with a `.tex` file and a `.bib` file.
Every `\cite{key}` in the `.tex` that has a matching record in the `.bib` is inserted.

---

## Data Model

Four PostgreSQL tables; no DDL foreign keys on `authors_papers`.

```
papers
  id        SERIAL PRIMARY KEY
  title     TEXT
  contents  TEXT
  bibtex_id TEXT  (references bib.bibtex_id; no DDL FK)

bib
  bibtex_id TEXT PRIMARY KEY
  bibtex    TEXT UNIQUE

authors_id
  id        SERIAL PRIMARY KEY
  author    TEXT

authors_papers
  id        SERIAL PRIMARY KEY
  author_id INTEGER  (soft ref to authors_id.id)
  paper_id  INTEGER  (soft ref to papers.id)
```

**Schema variant**: The older procedural modules (`add.py`, `get_data.py`, `search.py`) used the column name `bibtext_id` (with a typo). `DatabaseConnector` uses `bibtex_id`. Both variants exist in production databases; migration must handle both.

---

## Control Flow: CLI Dialog → Domain Layer → DB Layer

```
run.py::run()
  │
  ├─ argparse: --config, --key, --section
  ├─ ConfigReader.read_config()  →  db_config dict
  ├─ DatabaseConnector(db_config)
  │     └─ PsycopgDB(db_config)
  │
  └─ UserInteraction.interact(db_connector)
        │
        ├─ input() menu loop
        │
        ├─ search()  →  db_connector.search_by_author() / search_by_title()
        │                   └─ PsycopgDB.fetch_from_db(raw SQL)
        │
        ├─ add()     →  db_connector.add_entry_to_db()
        │                   └─ PsycopgDB.store_in_db() × N inserts
        │
        ├─ update()  →  db_connector.update_entry()
        │                   └─ PsycopgDB.update_db_entry() / store_in_db() / delete_from_db()
        │
        └─ delete()  →  db_connector.delete_paper_entry_from_database()
                            └─ PsycopgDB.delete_from_db() × N deletes
```

---

## Configuration

`ConfigReader(filename, section, key_file)` reads a Fernet-encrypted INI file. Returns a `db_config` dict with keys: `host`, `port`, `dbname`, `user`, `password`. Passed to psycopg2 `connect(**db_config)`.

Only one configuration source is supported in the legacy code. No `.env`, no environment variables.

---

## Install / Run

**Legacy** (pre-modernization):
```bash
pip install poetry
poetry install
python -m paper_sorts.run  # or: python paper_sorts/run.py
```
Requires `../../database.crypt` and `../../key` at those relative paths.

**Modernized** (post-001-modernize-stack):
```bash
uv sync --all-extras
uv run pdbsearch
```
See `specs/001-modernize-stack/quickstart.md`.

---

## Known Limitations and Quirks

1. **Two authors with identical `Last, First`**: treated as the same author. System continues this behaviour — it is a data-entry constraint, not a system bug.
2. **No DDL foreign keys on `authors_papers`**: deletes can leave orphan rows if done outside `DatabaseConnector`. The ORM layer must replicate the manual cleanup logic.
3. **Per-operation connection open/close**: `PsycopgDB` opens a new `psycopg2.connect()` per call. No connection pool.
4. **Schema typo**: `bibtext_id` in older modules vs. `bibtex_id` in `DatabaseConnector`. Migration must handle both.
5. **Ctrl+C mid-dialog**: the legacy code has no explicit signal handler. If interrupted during an insert loop, partial inserts may be committed. The ORM layer fixes this via per-operation sessions with rollback-on-exception.
6. **`create_tables()` at runtime**: schema creation is triggered at app startup. This is replaced by Alembic migrations.
7. **Tests depend on personal database**: `tests/test_database_connector.py` hard-codes `../../database.crypt` and asserts on specific live rows (`Pino, J.`, `Wang2021LargeScaleSA`). Not runnable on a fresh machine.
