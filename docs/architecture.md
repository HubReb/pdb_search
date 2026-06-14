# Architecture: Legacy paper_sorts (Pre-Modernization)

> This document describes the **original, pre-modernization** codebase as it
> existed before the 001-modernize-stack refactor. It serves as the acceptance
> reference for User Story 2 (the rebuilt system must preserve all described
> behaviours) and as a record of the existing architecture for future
> contributors reading git history.
>
> For the modernized architecture, see `CLAUDE.md`.

---

## Purpose & Scope

`paper_sorts` is a personal, offline, single-user CLI tool that stores
academic paper metadata in a local PostgreSQL database. The user can:

- Search for papers by title or by author name.
- Add new entries (inline prompts or from a `.bib` file).
- Update existing entries (title, summary, BibTeX, or author name).
- Delete entries.
- Bulk-import from a LaTeX literature overview + `.bib` pair.

There is no web interface, no network exposure, and no multi-user support.

---

## User Journeys

### 1. Search by Author

1. User starts the app (`python paper_sorts/run.py`).
2. Top-level menu prompts: "1) Search / 2) Add / 3) Update / 4) Quit".
3. User picks 1 (Search).
4. Sub-menu prompts: "1) by author / 2) by title".
5. User picks 1 (by author), enters a name.
6. `UserInteraction.search_by_author()` calls `DatabaseConnector.search_by_author(name)`.
7. If multiple papers found, `get_user_choice()` displays a numbered list; user picks one.
8. `DatabaseConnector.search_for_entry_by_specified_paper_information()` fetches the full record.
9. `DatabaseConnector.search_for_bibtex_entry_by_id()` fetches the BibTeX entry.
10. `helpers.pretty_print_results()` prints: title, authors (joined with "and"), summary, bibtex.

### 2. Search by Title

Same flow as "by author" but calls `DatabaseConnector.search_by_title(title)`. If the title
matches more than one paper (e.g. duplicate titles from different authors), a disambiguation
list is shown.

### 3. Add a New Entry

1. `UserInteraction.add()` uses `get_user_input()` for: author list (comma-separated),
   title, bibtex key, bibtex choice (inline or from file), summary.
2. `DatabaseConnector.add_entry_to_db()` runs sanity checks (table exists, key unique),
   then inserts into `bib`, then `papers`, then iterates authors inserting into
   `authors_id` (if new) and `authors_papers`.
3. On any insertion failure the method calls `rollback_database_addition()` to
   undo partial writes before returning False.

### 4. Update an Existing Entry

1. `UserInteraction.update()` prompts: which table (papers / bib / authors), which column,
   which identifier, what new value.
2. A confirmation prompt summarises the change before applying.
3. `DatabaseConnector.update_entry()` dispatches to `__update_papers_table()`,
   `__update_bib_table()`, or `__update_authors_id_column()`.
4. Author rename merges the old author into the new author if the new name already exists.

### 5. Delete an Entry

Not exposed in the original interactive menu (menu only shows Search / Add / Update / Quit),
but `DatabaseConnector.delete_paper_entry_from_database()` exists and is exercised by tests.

### 6. Bulk Import from LaTeX / BibTeX

`get_data.py` and `helpers.py` contain `get_data(tex_file)` and `get_bibtex_information(dict, bib_file)`
which extract `{title: {bibtex_id, contents, bibtex, author}}` from a `.tex` / `.bib` pair.
`DatabaseConnector.add_data_from_dict(data_dict)` then inserts each entry, skipping entries
whose bibtex key already exists (idempotent re-import).

---

## Data Model

Four tables, created lazily by `DatabaseConnector.create_tables()`:

```
papers
  id         SERIAL PRIMARY KEY
  title      TEXT
  contents   TEXT
  bibtex_id  TEXT → FK into bib.bibtex_id

bib
  bibtex_id  TEXT PRIMARY KEY    (the citation key, e.g. "Wang2021LargeScaleSA")
  bibtex     TEXT UNIQUE          (full BibTeX source string)

authors_id
  id         SERIAL PRIMARY KEY
  author     TEXT                 (in "Last, First" format)

authors_papers
  id         SERIAL PRIMARY KEY
  author_id  INT                  (references authors_id.id, no DDL FK)
  paper_id   INT                  (references papers.id, no DDL FK)
```

### Relationships

- **Paper → Bib**: many-to-one. Each paper has exactly one BibTeX entry.
- **Paper ↔ Author**: many-to-many via `authors_papers`. No DDL foreign keys —
  application code handles referential integrity manually.

### Schema Variant

The legacy `add.py` and `get_data.py` scripts (which pre-date the
`DatabaseConnector`) used the column name `bibtext_id` (sic — typo) in both
`bib` and `papers`. The `DatabaseConnector` / `PsycopgDB` stack uses the
correct spelling `bibtex_id`. Databases created by either path are structurally
equivalent but column names differ. The migration in revision 002 normalises
this difference.

### Where to Add a New Field

To add, say, a `publication_year` field to `papers`:
1. Add the column to `DatabaseConnector.create_tables()` DDL.
2. Update `add_entry_to_db()` to accept and insert the new field.
3. Update `update_entry()` / `__update_papers_table()` if the field is updatable.
4. Update `UserInteraction.add()` to prompt for the new field.
5. Update `pretty_print_results()` to display it.

---

## Control Flow: Search by Author (end-to-end)

```
stdin → UserInteraction.search_by_author(db_connector)
          │
          ├─ input("Please enter the author's name: ")
          │
          └─ db_connector.search_by_author(name)
                │
                └─ database_handler.fetch_from_db(
                       "select ... from authors_id INNER JOIN ...",
                       (name,)
                   )
                       │
                       └─ PsycopgDB.fetch_from_db()
                              │
                              ├─ psycopg2.connect(**config)
                              ├─ cur.execute(query, args)
                              ├─ cur.fetchall()
                              └─ return rows

          ← List[Tuple] of matching rows
          │
          ├─ get_user_choice(papers) if multiple
          │
          └─ db_connector.search_for_bibtex_entry_by_id(chosen_paper)
                          │
                          └─ database_handler.fetch_from_db(
                                 "select * from bib where bibtex_id=%s", (paper[3],)
                             )

          ← (bibtex_id, bibtex_string)
          │
          └─ helpers.pretty_print_results(bibtex_data, paper_data)
                 │
                 └─ print(title, authors, summary, bibtex)
```

---

## Configuration

The app is started as:
```bash
python paper_sorts/run.py -c ../../database.crypt --section postgresql -k ../../key
```

`ConfigReader` (a subclass of `configparser.ConfigParser`) reads the encrypted
file, decrypts it with `cryptography.fernet.Fernet`, and parses the `[postgresql]`
section into a dict `{host, port, dbname, user, password}`. That dict is passed
to `DatabaseConnector`, which passes it to `PsycopgDB`, which passes it to
`psycopg2.connect()`.

If the key file is missing or wrong, `Fernet.decrypt()` raises `InvalidToken`,
which propagates as an unhandled exception with a stack trace.

---

## Rollback Semantics

`DatabaseConnector.add_entry_to_db()` is the most complex write path. It operates
in this order:

1. Insert into `bib`.
2. Insert into `papers`.
3. For each author: insert into `authors_id` (if new), insert into `authors_papers`.

If any step after (1) fails, `rollback_database_addition()` is called, which:
- Deletes the `bib` row just inserted (via `delete_from_db`).
- For each author link already created in step (3): calls `rollback_author_addition()`
  which removes the `authors_papers` row and, if the author has no other papers,
  removes the `authors_id` row too.

However: each SQL call goes through `PsycopgDB.store_in_db()` which opens its own
connection, executes, and commits. So writes are committed one-by-one, not in a
single transaction. The "rollback" is therefore a compensating-write approach, not
a true database ROLLBACK. This means a crash between insertions can leave the
database in an inconsistent state — the modernization fixes this by using a single
`with Session(...)` context manager that commits only on full success.

---

## Install / Run (Legacy)

```bash
pip install poetry
poetry install
poetry run python paper_sorts/run.py \
    --config ../../database.crypt \
    --section postgresql \
    --key ../../key
```

Or:
```bash
cd paper_sorts
python run.py -c ../../database.crypt -k ../../key
```

---

## Known Limitations

1. **Identical author deduplication**: Two authors with identical "Last, First"
   strings are treated as the same person. There is no author-ID disambiguation
   beyond the name string.

2. **No CI-friendly test suite**: `tests/test_database_connector.py` requires a
   live developer-local PostgreSQL database seeded with specific rows
   (e.g. "Pino, J.", "Wang2021LargeScaleSA"). It cannot be run on a fresh machine
   or in CI without manual setup. `tests/test_user_interaction.py` is a placeholder
   (`assertEqual(True, False)`) that always fails.

3. **Per-call transaction model**: Each `PsycopgDB` method opens, uses, and closes
   its own connection. Rollback is a compensating-write, not a true DB rollback.
   Partial failures can leave the database inconsistent.

4. **Single log file per class**: Each object (`UserInteraction`, `DatabaseConnector`,
   `PsycopgDB`) creates its own file-backed logger. There is no structured logging
   or log aggregation.

5. **Duplicate modules**: `add.py`, `search.py`, `get_data.py` contain older
   procedural versions of the same functionality. They use `psycopg` v3 (not
   `psycopg2`), column name `bibtext_id` (not `bibtex_id`), and are not wired into
   `run.py`. They are vestiges of earlier development and were never fully removed.

6. **No delete in the interactive menu**: The top-level menu only offers
   Search / Add / Update / Quit. Delete is implemented in `DatabaseConnector` and
   tested but not exposed to the user in the interactive flow.
