# Architecture: paper_sorts (Legacy Stack, Pre-Modernization)

**Scope**: This document reverse-engineers the *legacy* flat-layout `paper_sorts/` package as it
existed before the 001-modernize-stack feature.  It is the acceptance reference for User Story 2
(the rebuilt system must do everything described here).

---

## Purpose

`paper_sorts` is an **offline, single-user CLI** tool for managing a personal library of academic
papers.  It stores publication metadata (title, authors, abstract/summary, BibTeX entry) in a
local PostgreSQL database and lets the user search, add, update, and delete entries through an
interactive command-line dialog.

Typical use-case: a researcher running the tool on a personal laptop to keep track of papers read
during travel.

---

## User Journeys

1. **Search by author** — enter an author name; the system lists all papers by that author; the
   user picks one; the system prints its title, authors, summary, and BibTeX entry.

2. **Search by title** — enter a title; if one paper matches, its details are printed; if multiple
   papers match, the user picks from a numbered list; details are printed.

3. **Add entry (inline)** — enter author(s), title, BibTeX key, BibTeX entry text, and a summary;
   all are written to the database in one logical transaction.

4. **Add entry from file** — same as above but the BibTeX text is read from a `.bib` file.

5. **Update entry** — select a table (papers / bib / authors_id), a field, supply the identifier
   of the row, enter the new value, and confirm; the database row is updated.

6. **Delete entry** — supply the title, authors, BibTeX key, and summary of the paper; after a
   confirmation, the paper and all its associations are removed.

7. **Bulk import** — point at a `.tex` literature-overview file and a `.bib` file; all cited
   entries with matching BibTeX records are inserted (one per paper, skipping duplicates).

8. **Quit** — exit the interactive menu without side effects.

---

## Data Model

Four tables, created lazily by `DatabaseConnector.create_tables()` using raw SQL:

### `bib`
| Column | Type | Constraint |
|--------|------|------------|
| bibtex_id | TEXT | PRIMARY KEY |
| bibtex | TEXT | UNIQUE |

The BibTeX citation key and the full BibTeX source string.

### `papers`
| Column | Type | Constraint |
|--------|------|------------|
| id | SERIAL | PRIMARY KEY |
| title | TEXT | |
| contents | TEXT | |
| bibtex_id | TEXT | FK → bib.bibtex_id |

One row per publication.  `bibtex_id` is the user-facing unique identifier.

### `authors_id`
| Column | Type | Constraint |
|--------|------|------------|
| id | SERIAL | PRIMARY KEY |
| author | TEXT | |

One row per unique author name string (in `"Last, First"` form).  Two authors with identical
names are treated as the same person — known limitation.

### `authors_papers`
| Column | Type | Constraint |
|--------|------|------------|
| id | SERIAL | PRIMARY KEY |
| author_id | INT | (no DDL FK) |
| paper_id | INT | (no DDL FK) |

Many-to-many join table between `authors_id` and `papers`.  The absence of DDL foreign keys is
a deliberate original design choice (preserved in the modernized stack).

### Column name variant
The procedural legacy modules (`add.py`, `search.py`, `get_data.py`) used `bibtext_id` (sic —
typo) instead of `bibtex_id`.  The two spellings coexist in different database snapshots.

---

## Control Flow

```
user → CLI dialog (UserInteraction)
          ↓
      DatabaseConnector   (domain operations; SQL strings)
          ↓
      PsycopgDB           (thin psycopg2 wrapper)
          ↓
      PostgreSQL
```

1. **`run.py`** — entry point.  Parses CLI flags (config file, section, key file), decrypts config
   via `ConfigReader`, instantiates `DatabaseConnector`, passes it to `UserInteraction.interact()`.

2. **`UserInteraction`** (`user_interaction.py`) — all `input()` / `print()` calls live here.
   Routes user choices to the appropriate `DatabaseConnector` method.

3. **`DatabaseConnector`** (`database_connector.py`) — high-level domain operations.  Contains all
   SQL strings.  Performs multi-step transactions (add: insert bib → insert paper → insert
   author(s) → link author-paper).  Explicit rollback via SQL DELETE on partial failure.

4. **`PsycopgDB`** (`psycopg_db.py`) — thin wrapper around `psycopg2`.  The only module that
   imports `psycopg2`.  Provides `store_in_db`, `fetch_from_db`, `delete_from_db`,
   `update_db_entry`.

5. **`helpers.py`** — pure utility functions: `create_logger`, `get_user_input`,
   `pretty_print_results`, `get_data` (LaTeX parser), `get_bibtex_information` (BibTeX parser).

6. **`config_reader.py`** — decrypts a Fernet-encrypted INI file; returns the `[postgresql]`
   section as a dict passed to `psycopg2.connect(**config)`.

---

## Configuration

Single source: Fernet-encrypted INI file (`../../database.crypt` by default, decrypted with
`../../key`).  Paths are overridden via `-c` / `-k` CLI flags.

INI format:
```ini
[postgresql]
host = ...
port = ...
dbname = ...
user = ...
password = ...
```

No support for environment variables or `.env` files in the legacy stack.

---

## Install and Run

```bash
# Install dependencies (Poetry)
poetry install

# Run the interactive CLI
poetry run python paper_sorts/run.py -c /path/to/database.crypt --section postgresql -k /path/to/key

# Lint
poetry run pylint paper_sorts

# Run tests (requires a live DB with personal seed data)
poetry run python -m unittest discover tests
```

---

## Known Limitations and Quirks

1. **Two authors with identical names** are treated as the same person (no disambiguation by
   affiliation or ORCID).  Documented; behaviour preserved.

2. **`bibtext_id` typo** — procedural modules use `bibtext_id`; the OO stack uses `bibtex_id`.
   A personal database may have either spelling depending on which code path created it.

3. **Rollback semantics** — `DatabaseConnector.add_entry_to_db` rolls back manually via SQL
   DELETE on partial failure.  A power failure between steps could leave the DB inconsistent.

4. **Tests depend on live personal database** — `test_database_connector.py` connects to the
   developer's personal DB and asserts on specific seeded rows (`"Pino, J."`,
   `"Wang2021LargeScaleSA"`).  A fresh checkout cannot run the suite without this data.

5. **`test_user_interaction.py`** — a placeholder that always fails (`assertEqual(True, False)`).

6. **Logging** — each class creates its own file-backed logger (`db_connector.log`,
   `interaction.log`, etc.).  Log files are created in the current working directory.

7. **No migration tool** — schema is created by `create_tables()` at runtime.  Changing the
   schema requires manually running SQL on existing databases.

8. **Single config source** — only the encrypted INI file is supported; no env vars, no `.env`.
