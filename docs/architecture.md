# Architecture Document: Paper Sorts (Legacy)

**Version**: Pre-modernization baseline | **Date**: 2026-06-14 | **Status**: Reference only — modernization in progress on `001-modernize-stack`

---

## Purpose

Paper Sorts is a personal, offline, CLI tool for managing a literature database. A user can:

- Search for papers by title or author name.
- Add new papers (inline or from a `.bib` file).
- Update paper fields (title, summary, BibTeX entry, author name).
- Delete papers.
- Bulk-import papers from a LaTeX literature overview (`.tex`) paired with a BibTeX file (`.bib`).

The tool stores metadata (title, authors, one-sentence summary, full BibTeX source) in a local PostgreSQL database. It is not a library, not a web service, and not a multi-user system.

---

## User Journeys

### 1. Search by Author

```
User runs pdbsearch
-> Chooses "1) Search the database"
-> Chooses "1) Search by author"
-> Types author name (e.g. "Pino, J.")
-> Tool queries authors_id + authors_papers + papers
-> If found: displays list; user picks one paper
-> Tool fetches bib entry and pretty-prints result
```

### 2. Search by Title

```
User runs pdbsearch
-> Chooses "1) Search the database"
-> Chooses "2) Search by paper title"
-> Types title (exact match)
-> Tool queries papers + authors_id + authors_papers
-> If single result: pretty-prints immediately
-> If multiple results: disambiguation menu (numbered list), user picks one
```

### 3. Add Paper (Inline)

```
User chooses "2) Add an entry"
-> Prompted for: author list (comma-sep), title, BibTeX key, BibTeX entry (inline), summary
-> Tool validates BibTeX key is not already in database
-> Inserts into bib, papers, authors_id (if new), authors_papers
-> On failure: attempts rollback to avoid partial writes
```

### 4. Add Paper (from .bib file)

Same as 3, but user enters a filename instead of the BibTeX string. The file is read and its content inserted.

### 5. Update Entry

```
User chooses "3) Update an entry"
-> Prompted for: table (papers/bib/authors), column, entry identifier, new value
-> Confirmation step: "Change X to Y? 1) Yes 2) No"
-> On yes: executes UPDATE query
```

### 6. Delete Paper

Handled via update flow currently; `delete_paper_entry_from_database` removes from:
- `authors_papers` (unlinks all authors for this paper)
- `authors_id` (removes any authors who now have no papers)
- `papers` (removes the paper row)
- `bib` (removes the BibTeX entry)

### 7. Bulk Import (get_data.py)

```
User runs: python paper_sorts/get_data.py -l literature.tex -b bib.bib
-> Parses .tex to extract title/BibTeX-key/description triples
-> Parses .bib to enrich with full BibTeX string and author list
-> For each entry: checks if bibtex_id exists; inserts if not
-> Commits per-paper; logs warnings for missing bib entries
```

---

## Data Model

Four PostgreSQL tables:

```
papers
  id        SERIAL PRIMARY KEY
  title     TEXT
  contents  TEXT                      -- one-sentence summary
  bibtex_id TEXT                      -- FK -> bib.bibtex_id (via DatabaseConnector)
             OR bibtext_id (sic)      -- typo variant used by get_data.py and add.py

bib
  bibtex_id TEXT PRIMARY KEY          -- or bibtext_id (sic) in legacy variant
  bibtex    TEXT UNIQUE               -- full BibTeX source string

authors_id
  id      SERIAL PRIMARY KEY
  author  TEXT                        -- "Last, First" format

authors_papers
  id         SERIAL PRIMARY KEY
  author_id  INT                      -- no DDL FK (intentional)
  paper_id   INT                      -- no DDL FK (intentional)
```

**Relationships**:
- `papers` -> `bib`: many-to-one (each paper has exactly one BibTeX entry)
- `papers` <-> `authors_id`: many-to-many via `authors_papers` (no DDL FK on link table)

**Known schema variant**: The procedural modules (`add.py`, `get_data.py`) use `bibtext_id` (typo); the OO stack (`database_connector.py`) uses `bibtex_id`. A personal database may have either variant depending on how it was originally populated.

**Known limitation**: Two authors with identical "Last, First" strings are treated as the same author. There is no de-duplication beyond the string value.

---

## Control Flow

### Startup

```
run.py:run()
  -> parse argparse (config path, key path, section)
  -> ConfigReader.__init__(): decrypt Fernet INI -> dict
  -> DatabaseConnector.__init__(): build PsycopgDB wrapper + logger
  -> UserInteraction().interact(database_connector)
```

### Dialog Loop (UserInteraction.interact)

```
while True:
  prompt user for 1-4
  match choice:
    1 -> UserInteraction.search() -> search by author or title
    2 -> UserInteraction.add()
    3 -> UserInteraction.update()
    4/"q" -> break
```

### Persistence Path

```
UserInteraction -> DatabaseConnector -> PsycopgDB -> psycopg2 -> PostgreSQL
```

`PsycopgDB` opens a new connection per query (`connect(**config)`), executes, commits or rolls back, and closes. No connection pooling.

---

## Configuration Approach

`ConfigReader` (`paper_sorts/config_reader.py`) reads a Fernet-encrypted INI file:

```ini
[postgresql]
dbname = papers
user = <user>
password = <password>
host = localhost
port = 5432
```

The file is decrypted with `cryptography.fernet.Fernet` using the key from a separate key file. The resulting dict is passed to `psycopg2.connect(**config)`.

No environment variable support. No `.env` file support. Only the encrypted config file.

---

## Install and Run

```bash
# Install (legacy)
poetry install

# Run interactive CLI
poetry run python paper_sorts/run.py -c ../../database.crypt -k ../../key

# Bulk import
poetry run python paper_sorts/get_data.py -l literature.tex -b bib.bib

# Add single entry from .bib
poetry run python paper_sorts/add.py -b single.bib

# Lint
poetry run pylint paper_sorts

# Tests (requires live personal database)
poetry run python -m unittest discover tests
```

---

## Known Limitations and Quirks

1. **Tests depend on a live personal database**: `tests/test_database_connector.py` opens a real connection using the encrypted config file and asserts on specific seeded rows (`"Pino, J."`, `"Wang2021LargeScaleSA"`). Cannot run on a fresh machine.

2. **`tests/test_user_interaction.py` is always-failing placeholder**: `assertEqual(True, False)` — intentionally fails.

3. **Dual schema variant**: `bibtext_id` (typo) in `add.py`/`get_data.py` vs `bibtex_id` in the OO stack. A database populated by `get_data.py` will not be readable by `database_connector.py` without column renaming.

4. **No rollback on `create_tables`**: If `create_tables()` fails midway, the tables that were created before the failure remain; reruns are idempotent because `CREATE IF NOT EXISTS` is used.

5. **Partial add failure semantics**: `DatabaseConnector.add_entry_to_db` attempts `rollback_database_addition` on failure, but `rollback_database_addition` itself has SQL bugs (e.g. missing closing paren in `DELETE FROM bib WHERE (bibtex_ident=%s`). Full rollback is therefore not guaranteed.

6. **Per-class file loggers**: Each class (`UserInteraction`, `DatabaseConnector`, `PsycopgDB`) creates its own file-backed logger via `create_logger()`. Log files accumulate in the working directory.

7. **`PsycopgDB.update_db_entry` argument order**: `(query, identifier, update_value)` but `cur.execute(query, (update_value, identifier))` — the arguments are swapped at the call site. This means every `UPDATE ... SET col=%s WHERE id=%s` binds `update_value` first, then `identifier`. Callers must be aware.

8. **No explicit quit in search sub-menu**: The search sub-menu does not have a quit/abort option beyond invalid input silently re-prompting.

9. **`cast` returns -1 on non-integer input**: Used to parse menu selections; `-1` is not a valid menu index, causing a re-prompt. This is the intended behaviour.

10. **`get_user_input` loops on empty string**: Pressing Enter re-prompts indefinitely. No timeout.

---

## Where to Add a New Field

To add a new field to the `papers` table (e.g. `doi TEXT`):

1. Add a new column in `DatabaseConnector.create_tables()` (the `CREATE TABLE papers` statement).
2. Add the column to `DatabaseConnector.store_paper_in_db()` and `add_entry_to_db()` INSERT statements.
3. Add a `case "doi":` branch in `DatabaseConnector.__update_papers_table()`.
4. Update `UserInteraction.add()` to prompt for the new field.
5. Update `pretty_print_results()` in `helpers.py` to display it.
6. Update the same locations in `add.py` and `get_data.py` if those are still in use.
7. If the database already exists, run a manual `ALTER TABLE papers ADD COLUMN doi TEXT;`.
