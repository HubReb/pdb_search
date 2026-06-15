# Architecture: paper_sorts (Legacy Stack)

> **Purpose of this document**: Reverse-engineer the pre-modernization codebase
> (`paper_sorts/` flat layout) so that the modernization has a written baseline.
> This document describes the *legacy* stack as it existed before the
> 001-modernize-stack refactor. It is the acceptance reference for User Story 2:
> the rebuilt system must reproduce everything described here.

---

## 1. Purpose

`paper_sorts` is a personal, offline CLI tool for storing and searching academic
publication metadata. A user maintains a local PostgreSQL database of papers they
have read or plan to read. For each paper the system stores:

- Title
- Authors (one or more, in "Last, First" form)
- A one-sentence summary ("contents")
- The full BibTeX source string and its key

The tool allows the user to search, add, update, and delete entries interactively,
and to bulk-import entries from a LaTeX literature-overview `.tex` file paired with
a `.bib` file.

There is no web interface, no API, no multi-user mode, and no network exposure.

---

## 2. User Journeys

### 2.1 Search by Author
1. User starts `python paper_sorts/run.py` (with `-c` and `-k` for config/key paths).
2. Main menu: user enters `1` → "Search the database".
3. Sub-menu: user enters `1` → "Search by author".
4. User types an author name ("Last, First" form).
5. System queries `authors_id` → `authors_papers` → `papers` and returns all papers
   by that author.
6. If multiple papers: user chooses one from a numbered list.
7. System fetches the BibTeX string from `bib` and pretty-prints: title, authors,
   summary, BibTeX entry.

### 2.2 Search by Title
1. User selects `2` at sub-menu → "Search by paper title".
2. User types a title (exact match).
3. System queries `papers` joined to `authors_id` via `authors_papers`.
4. If multiple papers share the title: user picks one from a numbered list.
5. System fetches BibTeX and pretty-prints result.

### 2.3 Add a New Entry (Inline)
1. User enters `2` → "Add an entry".
2. Prompted for: authors (comma-separated "Last, First"), title, BibTeX key,
   then either a `.bib` file path or inline BibTeX text, then summary.
3. System inserts rows into `bib`, `papers`, `authors_id` (if new), and
   `authors_papers` in a single connection with commit-per-table-operation.

### 2.4 Update an Existing Entry
1. User enters `3` → "Update an entry".
2. Sub-menu: table to update (`papers` / `bib` / `authors_id` / abort).
3. Column sub-menu (for `papers`: `title` or `contents`).
4. User enters the internal ID of the row to change, then the new value.
5. User confirms (`1/y/yes` to proceed, `2/n/no` to abort).

### 2.5 Delete an Entry
- Implemented internally by `DatabaseConnector.delete_paper_entry_from_database()`.
- Removes the paper from `papers`, its link rows from `authors_papers`, and its
  BibTeX entry from `bib`. Authors in `authors_id` are left in place.

### 2.6 Bulk Import from .tex + .bib
- Run `python paper_sorts/get_data.py -l literature_overview.tex -b bib.bib -c … -k …`
- Creates the four tables if they don't exist, then inserts each paper found.
- Per-paper commit: a failure mid-import leaves already-committed rows intact.

---

## 3. Data Model

### Tables

| Table | Primary Key | Notable Columns |
|-------|-------------|-----------------|
| `papers` | `id` SERIAL | `title TEXT`, `contents TEXT`, `bibtex_id TEXT` FK → `bib.bibtex_id` |
| `bib` | `bibtex_id TEXT` | `bibtex TEXT UNIQUE` |
| `authors_id` | `id` SERIAL | `author TEXT` |
| `authors_papers` | `id` SERIAL | `author_id INT`, `paper_id INT` (no DDL FKs) |

### Relationships

- A Paper has exactly one BibTeX entry (FK from `papers.bibtex_id` → `bib.bibtex_id`).
- A Paper can have many Authors, and an Author can have many Papers (many-to-many via `authors_papers`).
- The `authors_papers` table has **no DDL foreign keys** — this is a known quirk of the original schema and must be preserved.

### Historical Variants

Two column name variants exist across the codebase:
- `bibtex_id` — used by `DatabaseConnector` (the OO stack, `run.py` entry point)
- `bibtext_id` (typo!) — used by the legacy procedural modules (`add.py`, `get_data.py`, `search.py`)

Both variants refer to the same conceptual column. Migration from the typo variant to the canonical name is handled by Alembic revision 002.

### Where to Add a New Field

To add a new field to a paper (e.g. `year: int`):
1. Add the column to `papers` in `DatabaseConnector.create_tables()` (legacy) or create an Alembic migration (modern).
2. Update `DatabaseConnector.add_entry_to_db()` to include the new field.
3. Update `UserInteraction.add()` to prompt for the new field.
4. Update `DatabaseConnector.search_by_title()` / `search_by_author()` to return it.
5. Update `helpers.pretty_print_results()` to display it.

---

## 4. Control Flow

```
User types a command
        ↓
paper_sorts/run.py
  → parse CLI args (argparse)
  → ConfigReader.read(database.crypt, key)   [decrypts Fernet INI]
  → DatabaseConnector.__init__(config)
      → PsycopgDB.__init__(config)            [connects to PostgreSQL]
  → UserInteraction.interact(db_connector)
        ↓
  [match/case on user input]
  → UserInteraction.search(db_connector)
      → db_connector.search_by_author(name)
          → PsycopgDB.fetch_from_db(sql)    [psycopg2 cursor.execute + fetchall]
      → helpers.get_user_choice(results)    [numbered list → user picks one]
      → db_connector.search_for_bibtex_entry_by_id(paper)
      → helpers.pretty_print_results(bib, paper)
  → UserInteraction.add(db_connector)
      → helpers.get_user_input(prompt)      [loops until non-empty]
      → db_connector.add_entry_to_db(...)
          → PsycopgDB.store_in_db(sql)      [INSERT with commit]
  → UserInteraction.update(db_connector)
      → db_connector.update_entry(col, val, table, id)
          → PsycopgDB.update_db_entry(sql, id, val)
  → "4"/q → break
```

---

## 5. Configuration

The legacy system uses a Fernet-encrypted INI file (default path `../../database.crypt`,
key at `../../key`). `ConfigReader` (a `ConfigParser` subclass) decrypts and parses it:

```ini
[postgresql]
host = localhost
port = 5432
dbname = papers
user = myuser
password = mypassword
```

Config is passed as a plain `dict` through the stack. There is no environment-variable
support, no `.env` support, and no multi-source priority chain in the legacy stack.

---

## 6. Install & Run (Legacy)

```bash
pip install poetry
poetry install
# Launch interactive mode:
poetry run python paper_sorts/run.py -c ../../database.crypt --section postgresql -k ../../key
# Bulk import:
poetry run python paper_sorts/get_data.py -l lit.tex -b bib.bib -c ../../database.crypt -k ../../key
# Run tests (requires live personal database):
poetry run python -m unittest discover tests
```

---

## 7. Rollback Semantics

`DatabaseConnector` and `PsycopgDB` open a new `psycopg2.connect()` per operation.

- **Add**: inserts `bib` row, commits; inserts `papers` row, commits; then for each author
  inserts into `authors_id` (if new) and `authors_papers`, committing after each author.
  A failure mid-author leaves the already-committed rows in place. There is no atomic
  single-transaction add. To roll back a failed add the caller must call
  `rollback_database_addition()` explicitly.
- **Update**: single `UPDATE … WHERE …` with commit; rolled back on `DatabaseError`.
- **Delete**: no explicit transaction — the delete query commits immediately.
- **`create_tables()`**: called at startup if tables don't exist; if interrupted, partial
  table creation may leave the schema in an inconsistent state.

---

## 8. Known Limitations

| Limitation | Description |
|------------|-------------|
| Duplicate author names | Two authors with the same "Last, First" string are treated as the same author. No deduplication or disambiguation. |
| Per-class log files | Each class (`UserInteraction`, `DatabaseConnector`, `PsycopgDB`) creates its own file-backed logger (`interaction.log`, `db_connector.log`, `psycopg_logger.log`). Logs accumulate in the working directory. |
| Tests depend on live DB | `test_database_connector.py` opens a real connection and asserts on hardcoded rows (`"Pino, J."`, `"Wang2021LargeScaleSA"`). Cannot run without the personal database. |
| `test_user_interaction.py` always fails | Placeholder `assertEqual(True, False)` — not a real test. |
| `bibtext_id` typo | The legacy procedural modules use a misspelled column name. Code working against a database created by `get_data.py` will not see entries created by `DatabaseConnector`, and vice versa. |
| No pagination | Search results are displayed in full; on a very large corpus this could be unwieldy. |
| Ctrl+C leaves partial state | Interrupting mid-add may leave `bib` and `papers` rows without author links. |
