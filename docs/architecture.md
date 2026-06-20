# Architecture: paper_sorts (pre-modernization baseline)

**Purpose of this document**: Reverse-engineered reference for the legacy flat-layout
`paper_sorts/` codebase, as it existed before the `001-modernize-stack` refactor.
This document is the acceptance reference for User Story 2 (the rebuilt system must do
everything described here).

---

## What does it do?

`paper_sorts` is a personal, off-line CLI tool that stores academic publication metadata
in a local PostgreSQL database and allows a single user to:

- Search publications by title or author name
- Add a new publication (manually or from a `.bib` file)
- Update an existing publication's title, summary, author, or BibTeX entry
- Delete a publication
- Bulk-import publications from a LaTeX literature-overview file and its `.bib` companion

It is designed for use with an existing personal PostgreSQL database and a Fernet-encrypted
credentials file.  There is no web UI, REST API, or multi-user support.

---

## User Journeys

### Search by title
1. User runs `python paper_sorts/run.py` (or the entry point).
2. Top-level menu is displayed.  User selects `1) Search`.
3. Sub-menu offers `1) Search by author` / `2) Search by title`.
4. User selects `2`, enters the paper title.
5. If one match: paper details are printed (title, authors, summary, BibTeX).
6. If multiple matches (same title): user chooses from a numbered list.
7. If no match: message printed; returns to menu.

### Search by author
1. User selects `1) Search by author` from the search sub-menu.
2. User enters the author's name (e.g. `Pino, J.`).
3. If one paper: printed directly.
4. If multiple papers by that author: numbered list for disambiguation.
5. If not found: message printed.

### Add a publication (inline)
1. User selects `2) Add` from the top-level menu.
2. Prompted for: author(s) (comma-separated), title, BibTeX key.
3. Offered choice: enter BibTeX from a file or type inline.
4. Prompted for summary (one sentence).
5. Entry is persisted: bib, paper, author(s), author–paper links.

### Add a publication (from .bib file)
1. Same as above, but at step 3 the user provides a filename.
2. `pybtex` parses the `.bib` file; authors, bibtex key, and full entry are extracted.

### Update
1. User selects `3) Update`.
2. Prompted for which table to update (`papers` / `bib` / `authors`).
3. Prompted for which column and the row's identifier.
4. New value entered; confirmation required (`y`/`n` or `1`/`2`).
5. Change is committed or aborted.

### Delete
1. (No dedicated delete in the legacy top-level menu — reached via `DatabaseConnector`
   methods exposed to `UserInteraction`. In the modernized version this becomes a
   subcommand.)

### Bulk import
1. User runs `python paper_sorts/get_data.py -l <tex> -b <bib>`.
2. LaTeX file is parsed with `pylatexenc` to extract title–citation-key pairs.
3. `.bib` file is parsed with `pybtex` to get full bibtex + author list per key.
4. Each entry is inserted (paper, bib, authors, links) unless already present.

---

## Data Model

Four PostgreSQL tables:

### `bib`
| Column | Type | Constraints |
|--------|------|-------------|
| `bibtex_id` | TEXT | PRIMARY KEY |
| `bibtex` | TEXT | UNIQUE |

Stores the full BibTeX source string for each publication, keyed by the BibTeX
citation key (e.g. `Wang2021LargeScaleSA`).

### `papers`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | SERIAL | PRIMARY KEY |
| `title` | TEXT | |
| `contents` | TEXT | |
| `bibtex_id` | TEXT | FK → `bib.bibtex_id` |

One row per publication.  `bibtex_id` links to `bib`.

### `authors_id`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | SERIAL | PRIMARY KEY |
| `author` | TEXT | |

One row per unique author name (in `"Last, First"` form).

### `authors_papers`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | SERIAL | PRIMARY KEY |
| `author_id` | INT | (no DDL FK) |
| `paper_id` | INT | (no DDL FK) |

Many-to-many join table.  No DDL foreign keys (quirk of the original schema).

### Relationships

```
papers (N) ──── FK bibtex_id ──── (1) bib
papers (N) ──── paper_id ──── (N) authors_papers ──── author_id ──── (N) authors_id
```

### Historical schema variant (older modules)

`paper_sorts/get_data.py`, `add.py`, and `search.py` use a slightly different
column name: `bibtext_id` (sic, typo) in `papers` and `bib`, and `bibtext`
(no UNIQUE) in `bib`.  These modules were never wired into the main entry point
(`run.py`) and are the "legacy procedural modules" to be deleted in FR-012.

---

## Control Flow

```
run.py
  └─ UserInteraction.interact()
       ├─ search()
       │    ├─ search_by_author()
       │    │    └─ DatabaseConnector.search_by_author()
       │    │         └─ PsycopgDB.fetch_from_db()  ← only psycopg2 caller
       │    └─ search_by_paper_title()
       │         └─ DatabaseConnector.search_by_title()
       ├─ add()
       │    └─ DatabaseConnector.add_entry_to_db()
       │         ├─ sanity_checks()
       │         ├─ PsycopgDB.store_in_db() ×N
       │         └─ __insert_single_author() ×N
       └─ update()
            └─ DatabaseConnector.update_entry()
                 └─ PsycopgDB.update_db_entry() / store_in_db() / fetch_from_db()
```

### Layer responsibilities

| Layer | Class | Responsibility |
|-------|-------|----------------|
| Presentation | `UserInteraction` | `input()`/`print()`, menu loops, format results |
| Domain | `DatabaseConnector` | Business logic, SQL strings, multi-step transactions |
| Persistence | `PsycopgDB` | `psycopg2` connection management, parameterised queries |
| Config | `ConfigReader` | Fernet decrypt → `ConfigParser` → `dict` |
| Helpers | `helpers.py` | `create_logger`, `get_user_input`, `cast`, bibtex/latex parsing |

---

## Configuration Approach

1. `run.py` accepts `-c <config>` (default `../../database.crypt`) and `-k <key>`.
2. `ConfigReader` opens the encrypted file, decrypts with the key (Fernet), parses the
   `[postgresql]` section, and returns a plain dict `{dbname, user, password, host, port}`.
3. That dict is passed into `PsycopgDB.__init__` and used in `psycopg2.connect(**params)`.

**Security quirk**: plaintext credentials exist in memory as a dict (not `SecretStr`);
they are not written to logs by the production code path.

---

## Install / Run

```bash
# Legacy (pre-modernization):
poetry install
poetry run python paper_sorts/run.py \
    -c ../../database.crypt -k ../../key

# Modernized (post-001-modernize-stack):
uv sync --all-extras
uv run pdbsearch --config ../../database.crypt --key ../../key
```

---

## Known Limitations and Quirks

1. **Duplicate author names**: Two authors with identical "Last, First" strings are treated
   as the same author (looked up by name, not ID).  This is a documented limitation.

2. **Legacy column typo**: The older modules (`get_data.py`, `add.py`, `search.py`) use
   `bibtext_id` (typo) instead of `bibtex_id`.  The two sets of modules are never used
   together, so databases created by the procedural modules have a different schema.
   Revision 002 of the Alembic migrations handles the rename.

3. **Rollback on partial add failure**: `DatabaseConnector.add_entry_to_db` manually
   deletes already-inserted rows when an author insertion fails mid-loop
   (`rollback_database_addition`).  This is not a real transaction — it is manual cleanup.
   If `rollback_database_addition` itself fails, the database may be partially written.
   In the modernized version, a proper SQLAlchemy session transaction handles this atomically.

4. **No delete in top-level menu** (legacy): `delete_paper_entry_from_database` exists on
   `DatabaseConnector` but is not surfaced in the `run.py` interactive menu.  Deletion
   was accessible only by calling the class directly (e.g. from a script).  The modernized
   CLI adds `pdbsearch delete` as a proper subcommand.

5. **Per-class log files**: Each class creates its own file-backed logger
   (`db_connector.log`, `interaction.log`, `psycopg_logger.log`).  Log level and filename
   are constructor parameters with hardcoded defaults.  No centralized configuration.

6. **Tests depend on a live database**: `tests/test_database_connector.py` asserts on
   rows that only exist in the original author's personal database.  There is no seeded
   fixture, no ephemeral DB, and the suite will fail on any other machine.
   `tests/test_user_interaction.py` is a placeholder that always fails
   (`assertEqual(True, False)`).

7. **No delete subcommand** in the legacy CLI — the `interact()` menu has
   `1) Search / 2) Add / 3) Update / 4) Quit` only.

---

## Where to Add a New Field

To add a new field (e.g. `journal`) to a paper in the **legacy** codebase:

1. Add a column to the `papers` table in `DatabaseConnector.create_tables()`.
2. Add the column to the INSERT in `DatabaseConnector.store_paper_in_db()` and
   `add_entry_to_db()`.
3. Update the SELECT queries in `search_by_title()` and `search_by_author()` and
   adjust index references (`paper[4]`, etc.) throughout.
4. Update `UserInteraction.add()` to prompt for the new field.
5. Update `helpers.pretty_print_results()` to display it.
6. Update `get_data.py` / `add.py` if the bulk-import path should populate the new field.
7. Run the integration test against a live DB (currently the only test).

In the **modernized** codebase, steps 1–3 are replaced by an Alembic migration + ORM
model change + repository method update.
