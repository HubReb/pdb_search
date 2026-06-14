# Architecture: paper_sorts (Legacy — Pre-Modernization Reference)

**Purpose**: This document describes the paper_sorts application *as it existed before the 001-modernize-stack refactoring*. It serves as the acceptance reference for the modernization: the rebuilt system must support all operations described here.

**Audience**: Python developers unfamiliar with the project.

---

## What Does It Do?

`paper_sorts` is a personal, offline, single-user CLI for managing a bibliography database. The user can:

- Search for papers by title or by author name.
- Add a new paper record (inline or from a `.bib` file).
- Update fields on an existing paper (title, summary, BibTeX entry, author).
- Delete a paper.
- Bulk-import papers from a LaTeX `.tex` + `.bib` file pair (the way the database was originally bootstrapped).

All data is stored in a local PostgreSQL database. There is no web interface, no API, no multi-user support.

---

## Data Model

Four tables:

### `bib`
| Column | Type | Notes |
|--------|------|-------|
| bibtex_id | TEXT (PK) | BibTeX citation key — user-facing unique identifier |
| bibtex | TEXT (UNIQUE) | Full BibTeX source string |

### `papers`
| Column | Type | Notes |
|--------|------|-------|
| id | SERIAL (PK) | Internal paper identifier |
| title | TEXT | Publication title |
| contents | TEXT | Summary / abstract |
| bibtex_id | TEXT | FK → bib.bibtex_id |

### `authors_id`
| Column | Type | Notes |
|--------|------|-------|
| id | SERIAL (PK) | Internal author identifier |
| author | TEXT | Author name in "Last, First" format |

### `authors_papers`
| Column | Type | Notes |
|--------|------|-------|
| id | SERIAL (PK) | — |
| author_id | INTEGER | References authors_id.id (no DDL FK) |
| paper_id | INTEGER | References papers.id (no DDL FK) |

**Relationships**:

- One Paper has exactly one Bib (via bibtex_id FK).
- One Paper has zero or more Authors (via authors_papers many-to-many).
- One Author may be linked to zero or more Papers.
- `authors_papers` has no DDL foreign key constraints (a known limitation — deleting a paper leaves orphan rows in authors_papers).
- Two authors with identical "Last, First" strings are treated as the same person (duplicate not detected).

**Where to add a new field**: Add a column to the `papers` table (or `bib` if BibTeX-related), then update `DatabaseConnector.create_tables()` and the corresponding `add_entry_to_db` / `update_entry` methods.

---

## Three-Layer Control Flow

```
UserInteraction  (paper_sorts/user_interaction.py)
       │  drives via method calls
       ▼
DatabaseConnector (paper_sorts/database_connector.py)
       │  calls via store_in_db / fetch_from_db / etc.
       ▼
PsycopgDB         (paper_sorts/psycopg_db.py)
       │  calls
       ▼
 PostgreSQL via psycopg2
```

### Layer 1 — `UserInteraction` (presentation)

Handles all `input()` / `print()` calls. Presents menus, collects user choices, displays results. Drives a `DatabaseConnector` instance injected at construction. The only layer that touches stdin/stdout.

Main entry: `interact(database_connector)` — presents the top-level menu and dispatches to sub-dialogs.

### Layer 2 — `DatabaseConnector` (domain / service)

Implements all user-visible operations as methods. Contains raw SQL strings. Handles multi-step transactional logic: e.g. `add_entry_to_db` calls `store_paper_in_db` → `__add_paper_to_db` → author insertion — rolling back the whole sequence if any step fails.

Key methods:
- `search_by_author(author)` — returns rows matching author name.
- `search_by_title(title)` — returns rows matching title.
- `add_entry_to_db(title, author, bibtex_id, contents, bibtex)` — full insert pipeline.
- `update_entry(paper_id, field, new_value)` — updates one field.
- `delete_paper_entry_from_database(paper_id)` — removes paper + author links.
- `rollback_database_addition()` — explicit rollback on partial failure.
- `create_tables()` — creates all four tables if they don't exist (called at startup).

### Layer 3 — `PsycopgDB` (persistence)

Thin wrapper around `psycopg2`. Exposes four methods:
- `store_in_db(sql, args)` — INSERT / UPDATE.
- `fetch_from_db(sql, args)` — SELECT → list of rows.
- `delete_from_db(sql, args)` — DELETE.
- `update_db_entry(sql, args)` — UPDATE (duplicate of store_in_db; historical).

This is the only module that imports `psycopg2`. Swapping the driver means changing only this class.

---

## Configuration

At startup, `run.py` reads credentials from a Fernet-encrypted INI file:

```
python paper_sorts/run.py -c ../../database.crypt --section postgresql -k ../../key
```

`ConfigReader` (paper_sorts/config_reader.py):
1. Reads the key file.
2. Decrypts the INI file with `cryptography.fernet.Fernet`.
3. Parses the `[postgresql]` section: `dbname`, `user`, `password`, `host` (implicit localhost), `port` (implicit 5432).
4. Returns a `dict` of credentials passed to `PsycopgDB`.

No environment-variable or `.env` support in the legacy stack.

---

## Install and Run (Legacy)

```bash
poetry install
poetry run python paper_sorts/run.py -c ../../database.crypt --section postgresql -k ../../key
```

The argparse defaults assume the script is launched from inside `paper_sorts/`; pass `-c` and `-k` explicitly otherwise.

---

## Legacy / Duplicate Modules

Three older procedural scripts exist alongside the OO stack:

| Module | Notes |
|--------|-------|
| `paper_sorts/add.py` | Standalone add, uses `psycopg` v3, `bibtext_id` (typo column) |
| `paper_sorts/search.py` | Standalone search, same driver/column variant |
| `paper_sorts/get_data.py` | Standalone bulk import, same variant |

These are **not wired into `run.py`**. They use a different driver (`psycopg` v3 vs `psycopg2`) and a misspelled column name (`bibtext_id` instead of `bibtex_id`). They exist as a historical artefact and are removed in the modernization.

---

## Rollback Semantics

`DatabaseConnector` uses `psycopg2` connections in `autocommit=False` mode (default). Multi-step operations (add paper → add bib → link authors) roll back the entire transaction if any step raises an exception. The `rollback_database_addition()` method calls `PsycopgDB.rollback()` explicitly.

**What happens on a partial add failure**: If inserting the paper row succeeds but the bib insert fails, the paper insert is rolled back. The database is never left with a paper row that has no corresponding bib row. (Exception: if a crash occurs between psycopg2 commits on separate operations — the legacy code sometimes commits between steps rather than in one atomic transaction. This is a known limitation documented in the code.)

---

## Known Limitations

1. **Live-database test dependency**: `tests/test_database_connector.py` opens a real PostgreSQL connection and asserts on developer-local rows ("Pino, J.", "Wang2021LargeScaleSA"). Running on a fresh machine or in CI will fail.
2. **Always-failing placeholder**: `tests/test_user_interaction.py` contains `assertEqual(True, False)` and always fails.
3. **No NOT NULL constraints**: All text columns (title, contents, author) accept NULL — the application does not enforce non-empty at the DB level.
4. **No DDL FKs on authors_papers**: Deleting a paper leaves orphan rows in `authors_papers`.
5. **Duplicate authors**: Two entries with identical "Last, First" strings are treated as the same author. No deduplication detection.
6. **bibtext_id typo**: The legacy `add.py` / `search.py` / `get_data.py` modules use the column name `bibtext_id` (sic). The OO stack uses `bibtex_id`. Running the legacy modules against a database created by the OO stack will fail.
7. **Per-class log files**: Each class creates its own file logger (`db_connector.log`, `interaction.log`) at construction time. Multiple runs append to the same files.
8. **argparse with relative-path defaults**: The default config/key paths assume launch from inside `paper_sorts/`.
