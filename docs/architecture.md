# Architecture Document: paper_sorts (Legacy Codebase)

**Audience**: Python developer new to the project  
**Purpose**: Reference for US1; acceptance baseline for US2  
**Date**: 2026-06-20

---

## Purpose

`paper_sorts` is an offline, single-user, CLI-only personal publication database.
It lets the user store and retrieve research paper metadata (title, author(s),
one-sentence summary, full BibTeX entry) in a local PostgreSQL database.
Use case: traveling without internet access, searching a personal reading list.

---

## User Journeys

### 1. Search by author
User invokes the CLI → selects "1) Search" → selects "1) Search by author" →
enters an author name → system queries DB → if multiple papers found, user
picks one from a numbered list → system prints title, authors, summary, BibTeX.

### 2. Search by title
Same as above but enters a paper title in the "2) Search by title" path.

### 3. Add a paper (inline)
User selects "2) Add" → enters author(s), title, bibtex key, summary →
chooses to enter BibTeX inline → system inserts into bib + papers + authors_id
+ authors_papers tables in a single connection, with manual rollback on error.

### 4. Add a paper (from .bib file)
Same as above but provides a path to a `.bib` file; the system reads the BibTeX
string from the file.

### 5. Update an entry
User selects "3) Update" → picks table (papers/bib/authors) → picks field →
enters identifier and new value → confirms with y/n → system updates DB.

### 6. Delete a paper
Not yet in the interactive menu of `run.py`, but `DatabaseConnector` exposes
`delete_paper_entry_from_database`. The legacy tests exercise this directly.

### 7. Bulk import (admin)
User runs `python -m paper_sorts.get_data` with a `.tex` and `.bib` file →
system parses LaTeX, extracts titles + BibTeX keys, reads BibTeX from .bib,
inserts all papers in a single connection.

### 8. Add single entry (admin)
User runs `python -m paper_sorts.add` with a `.bib` file and summary → system
inserts one paper.

---

## Data Model

Four tables, no DDL foreign keys on `authors_papers`:

```
bib
  bibtex_id TEXT PK
  bibtex    TEXT UNIQUE

papers
  id         SERIAL PK
  title      TEXT
  contents   TEXT
  bibtex_id  TEXT FK→bib.bibtex_id

authors_id
  id     SERIAL PK
  author TEXT

authors_papers  (many-to-many, no DDL FKs)
  id        SERIAL PK
  author_id INT  (references authors_id.id by convention)
  paper_id  INT  (references papers.id by convention)
```

**Known quirk**: `get_data.py` and `add.py` use column name `bibtext_id` (typo)
while `database_connector.py` uses `bibtex_id` (correct). These two modules
create incompatible schemas on fresh DBs.

---

## Control Flow: CLI Dialog → Domain → DB

```
run.py:run()
  → argparse (--config, --key, --section)
  → ConfigReader(filename, section, key_file)   # decrypts Fernet INI
  → DatabaseConnector(config_dict)
      → PsycopgDB(config_dict)  # wraps psycopg2 (!)
  → UserInteraction().interact(database_connector)
      loop:
        get_user_input(prompt)   # re-prompts on empty
        "1" → self.search(db)
          get_user_choice() / input()
          db.search_by_author() or db.search_by_title()
          pretty_print_results()
        "2" → self.add(db)
          get_user_input() * N
          db.add_entry_to_db()
        "3" → self.update(db)
          get_user_input() * N
          db.update_entry()
        "4"/"q" → break
```

---

## Configuration

`ConfigReader` (subclass of `ConfigParser`) reads an encrypted INI file:
1. Opens the file as bytes.
2. Reads the key file as bytes.
3. Decrypts with `cryptography.fernet.Fernet(key).decrypt(config)`.
4. Parses the decrypted string with `ConfigParser`.
5. Returns a `db_config` dict with the `[postgresql]` section items.

Typical keys: `host`, `port`, `database`, `user`, `password`.

---

## Install / Run

Legacy (Poetry):
```bash
poetry install
poetry run python -m paper_sorts.run
```

Tests (legacy, require live DB):
```bash
python -m pytest tests/
# or
python -m unittest tests/test_database_connector.py
```

---

## Known Limitations and Quirks

1. **Dual column-name bug**: `get_data.py` / `add.py` create `bibtext_id`
   (typo); `database_connector.py` expects `bibtex_id`. Data created by the
   former is invisible to the latter without manual column rename.

2. **No fresh-checkout tests**: `test_database_connector.py` requires
   `../../database.crypt` + `../../key` + pre-seeded rows ("Pino, J.",
   "Wang2021LargeScaleSA"). No contributor without those files can run the suite.

3. **One always-failing placeholder test**: `tests/test_user_interaction.py`
   contains `assertEqual(True, False)` — it always fails.

4. **Driver inconsistency**: `psycopg_db.py` uses `psycopg2`; `get_data.py` and
   `add.py` use `psycopg` (v3). Two different drivers co-exist.

5. **psycopg_db.py opens a connection per query**: Each `store_in_db`,
   `fetch_from_db`, and `delete_from_db` call opens and closes its own
   connection. The rollback in error paths closes without rollback in some
   branches.

6. **Author identity**: Two authors with identical `"Last, First"` strings are
   treated as the same author (single `authors_id` row). This is documented
   behavior, not a bug.

7. **`delete` not in interactive menu**: `user_interaction.py:interact()` has
   options 1–3 + quit; delete requires calling `DatabaseConnector` directly.

8. **Partial-rollback in `add_entry_to_db`**: If author insertion fails after
   bib + paper insertion succeed, rollback deletes bib but uses a broken
   SQL string (`"Delete from bib where (bibtex_ident=%s)"` — mismatched
   column name `bibtex_ident` vs `bibtex_id`), leaving partial data.

9. **SQLi risk**: `sanity_checks` uses an f-string in a SQL query:
   `f"select exists(select * from papers where bibtex_id='{bibtex_ident}');"` —
   not parameterized.

---

## Where to Add a New Field

To add a field (e.g., `year: int`) to the `papers` table:
1. Add `year INTEGER` column to the `CREATE TABLE` statement in
   `DatabaseConnector.create_tables()`.
2. Update `add_entry_to_db` INSERT statement and all callers.
3. Update `search_by_title` and `search_by_author` SELECT statements.
4. Update `pretty_print_results` in `helpers.py`.
5. Update all tests that assert on row structure.
