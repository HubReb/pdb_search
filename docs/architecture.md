# Architecture: Paper Sorts (Legacy Stack)

**Status**: Reverse-engineered from the pre-modernization codebase (flat-layout `paper_sorts/`).
This document is the acceptance reference for the modernization (US2–US5).

---

## Purpose

`paper-sorts` is a personal, offline command-line tool for managing a local library of academic paper
metadata. The user stores publication titles, authors, one-sentence summaries, and full BibTeX entries
in a local PostgreSQL database. The tool supports:

- Searching for papers by title or author
- Adding new entries (interactively, or loading BibTeX from a `.bib` file)
- Updating existing entries (title, summary, BibTeX, author name)
- Deleting entries
- Bulk-importing a literature overview from a `.tex` + `.bib` file pair

It is a single-user, single-machine tool. There is no network access, no authentication, and no
multi-user concurrency.

---

## User Journeys

### 1. Search by Title

1. User launches `python paper_sorts/run.py -c database.crypt --section postgresql -k key`
2. Top-level menu appears: `1) Search  2) Add  3) Update  4) Quit`
3. User enters `1`, then `2) Search by paper title`
4. System prompts for a title string (exact match, case-sensitive)
5. If no result: "Paper was not found in db_connector."
6. If exactly one result: displays title, authors, summary, BibTeX
7. If multiple results share a title: disambiguation menu lists them by number; user selects one

### 2. Search by Author

1. From top-level menu, user selects `1) Search`, then `1) Search by author`
2. System prompts for author name in `"Last, First"` form (exact match)
3. If no result: "Author was not found in db_connector."
4. If results: disambiguation menu lists all papers by that author; user picks one; full record shown

### 3. Add a New Entry

1. From top-level menu, user selects `2) Add`
2. System prompts: Author(s) comma-separated, paper title, BibTeX key, BibTeX source (file or inline), summary
3. Empty input on any prompt causes re-prompt
4. On success: entry stored across four tables (bib, papers, authors_id, authors_papers)
5. On failure: logged to `db_connector_test.log`; "failed to add entry … please study logs" to stdout

### 4. Update an Existing Entry

1. From top-level menu, user selects `3) Update`
2. System asks which table to update: `1) papers  2) bib  3) authors  4) abort`
3. For `papers`: asks which column — `1) title  2) contents  3) abort`
4. System asks for the row identifier (numeric id for papers/authors, bibtex_id for bib)
5. System asks for new value
6. Confirmation: `1) (Y)es  2) (N)o` — accepts `1/y/yes` or `2/n/no`
7. On confirm: update committed; on deny: silently returns to menu

### 5. Delete an Entry

No direct interactive delete flow is exposed in the current `UserInteraction` — `delete_paper_entry_from_database` exists in `DatabaseConnector` but has no wired menu option in the current interactive loop. (Note: this is a known gap documented here for completeness; the modernization adds an explicit delete subcommand.)

### 6. Bulk Import

Run standalone: `python paper_sorts/get_data.py -l literature.tex -b bib.bib -c database.crypt -k key`

1. `get_data.py` reads a LaTeX file, extracting paper titles and BibTeX citation keys from lines containing `\cite{...}`
2. `get_bibtex_information` reads the matching `.bib` file, adding authors and full BibTeX strings
3. `load_data_into_db` inserts each paper: one commit per paper; existing BibTeX keys are skipped (idempotent)

---

## Data Model

### Four Tables

```
bib
├── bibtex_id  TEXT  PRIMARY KEY
└── bibtex     TEXT  UNIQUE

papers
├── id         SERIAL  PRIMARY KEY
├── title      TEXT
├── contents   TEXT
├── bibtex_id  TEXT  → bib.bibtex_id (FK, DDL-enforced)
└── (FK constraint: fk_bibtex_id)

authors_id
├── id         SERIAL  PRIMARY KEY
└── author     TEXT    (format: "Last, First")

authors_papers   ← many-to-many link (NO DDL FKs)
├── id         SERIAL  PRIMARY KEY
├── author_id  INT     (logical FK → authors_id.id)
└── paper_id   INT     (logical FK → papers.id)
```

### Relationships

- A **Paper** has exactly one **BibTeX Entry** (`papers.bibtex_id → bib.bibtex_id`).
- A **Paper** has one or more **Authors** via `authors_papers`.
- An **Author** may appear on multiple papers.
- Two authors with identical `"Last, First"` strings are treated as the same author (known limitation).

### Adding a New Field

1. Add column to `DatabaseConnector.create_tables()` SQL string
2. Add it to `add_entry_to_db` parameter list and `store_paper_in_db` INSERT
3. Add it to all three search SELECT statements
4. Add a prompt in `UserInteraction.add()`

### Legacy Schema Variant

`paper_sorts/get_data.py`, `paper_sorts/add.py`, and `paper_sorts/search.py` use the column name
`bibtext_id` (note: typo — an extra `t`). The `DatabaseConnector`/`PsycopgDB` stack uses `bibtex_id`.
A personal database created with the legacy scripts will have the typo column name.

---

## Control Flow

### Interactive Session (run.py → UserInteraction → DatabaseConnector → PsycopgDB → PostgreSQL)

```
run.py
  └─ argparse: parse -c, --section, -k
  └─ ConfigReader.__init__: open .crypt file → Fernet.decrypt → ConfigParser → db_config dict
  └─ DatabaseConnector.__init__(db_config) → PsycopgDB(db_config)
  └─ UserInteraction.interact(db_connector)
       └─ get_user_input("What do you want to do? 1/2/3/4") → loop
            case "1" → self.search(db_connector)
                 └─ cast(input("1) author  2) title")) → pick path
                 └─ search_by_author / search_by_paper_title
                      └─ db_connector.search_by_author(name)
                           └─ PsycopgDB.fetch_from_db(SQL JOIN query)
                                └─ psycopg2.connect(**db_config) → cursor.execute → fetchall
                      └─ get_user_choice(papers) → disambiguation menu
                      └─ pretty_print_results(bibtex_data, paper)
            case "2" → self.add(db_connector)
            case "3" → self.update(db_connector)
            case "4"|"q" → break
```

### Per-Operation DB Interaction

Each `PsycopgDB` method (`store_in_db`, `fetch_from_db`, `delete_from_db`, `update_db_entry`) opens
a **new connection** per call, commits (or rolls back on error), and closes. There is no persistent
connection or connection pool.

---

## Configuration

### Source

A Fernet-encrypted INI file (typically `../../database.crypt`) decrypted with a key file
(`../../key`). The config layout:

```ini
[postgresql]
dbname = mydb
user   = myuser
password = mypassword
host   = localhost
port   = 5432
```

`ConfigReader` extends `ConfigParser`, decrypts the file with `cryptography.fernet.Fernet`, then
reads the `[postgresql]` section into a `dict`. This dict is passed directly to `psycopg2.connect(**config)`.

### CLI Args (run.py)

```
-c / --config    path to encrypted config  (default: ../../database.crypt)
--section        config file section       (default: postgresql)
-k / --key       path to key file          (default: ../../key)
```

The default paths assume the program is launched from inside `paper_sorts/`; otherwise, pass `-c` and `-k` explicitly.

---

## Install and Run (Legacy)

```bash
# Install dependencies
poetry install

# Run the interactive CLI
poetry run python paper_sorts/run.py -c ../../database.crypt --section postgresql -k ../../key

# Bulk import
poetry run python paper_sorts/get_data.py -l ../../literature_overview.tex -b ../../bib.bib

# Lint
poetry run pylint paper_sorts

# Tests (requires live personal database + seeded data)
poetry run python -m unittest discover tests
```

---

## Known Limitations and Quirks

1. **Identical author names**: Two distinct people with the same `"Last, First"` string are stored as one author. This is by design (current behaviour) and documented as a limitation.

2. **Dual schema variants**: The legacy `get_data.py`/`add.py`/`search.py` modules use `bibtext_id` (typo); the `DatabaseConnector`/`PsycopgDB` stack uses `bibtex_id`. A database may be in either state.

3. **Tests require live personal database**: `tests/test_database_connector.py` opens a real connection using `../../database.crypt` + `../../key` and asserts on specific seeded rows (`"Pino, J."`, `"Wang2021LargeScaleSA"`). It fails on any machine without that exact data. `tests/test_user_interaction.py` is a placeholder that always fails (`assertEqual(True, False)`).

4. **Delete not wired in interactive menu**: `DatabaseConnector.delete_paper_entry_from_database` exists but is not reachable from the `UserInteraction.interact()` loop. The modernization adds a proper delete subcommand.

5. **Per-call connection opens**: Every `PsycopgDB` method opens a fresh `psycopg2` connection, executes, and closes. This avoids connection leaks but means a single "add paper" operation (bib insert + paper insert + N author inserts) opens N+2 separate connections.

6. **Rollback semantics**: `add_entry_to_db` manually tracks partial success and calls `rollback_database_addition` on failure. This deletes any bib/paper rows added before the failure and removes any author-paper links created so far. It does NOT use database-level transactions across all inserts — each `store_in_db` call is its own transaction. A process kill mid-operation can leave the database inconsistent.

7. **BibTeX key uniqueness**: The `bib.bibtex_id` PRIMARY KEY and the `bib.bibtex` UNIQUE constraint enforce uniqueness at the DB level. Bulk import skips existing keys rather than overwriting.

8. **Default paths assume CWD**: The default `-c ../../database.crypt` and `-k ../../key` paths assume the CLI is launched from inside `paper_sorts/`, not from the repo root.
