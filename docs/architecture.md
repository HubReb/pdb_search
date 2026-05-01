# Paper Sorts — Architecture (current implementation, pre-modernization)

**Audience**: a Python developer who has never seen the project. After reading this document end-to-end, you should be able to answer:

1. What does this application do?
2. How is the data laid out, and why?
3. Where would I add a new feature, or a new field on a paper?
4. What happens if a partial add fails midway through?

This is the **acceptance reference** for User Story 2 of the modernize-stack feature. The rebuilt CLI must reproduce every flow described here. The document also serves as a **rename map** for Phase 4 of that work — every module name mentioned here is one of the targets of FR-012 (delete the legacy modules) and the `paper_sorts/* → src/paper_sorts/*` migration.

## 1. Purpose

Off-line paper-database searcher. A small, single-user CLI for storing and searching publication metadata in a local PostgreSQL database. The original use case (per the README) is *"traveling by train"* — a mode where no online connection is available to the usual literature-search services. Each entry holds:

- **Title** of the publication
- **Authors** in `"Last, First"` form
- A short, user-written **summary**
- The full **BibTeX** source string

Three operations are supported interactively (search / add / update) plus a delete flow that exists in the code but isn't wired to the top-level menu, plus a one-shot bulk-import flow invoked separately as a script.

The application is **personal-scale**: one user, one local Postgres, no network exposure, no authentication, no concurrent writers. These bounds are codified in the project Constitution under "Stack & Constraints" (Section 2).

## 2. User journeys

All journeys are driven from `python paper_sorts/run.py -c <encrypted-config> -k <key-file>`. After the CLI confirms the database connection, the user sees the **top-level menu**:

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
Your choice:
```

The menu has **four entries**. There is no menu entry for delete or for bulk-import in the current code, even though both exist as functionality (delete is implemented but not surfaced in the menu; bulk-import is a separate script invocation).

### 2.1 Search (option 1)

The search dialog is two-step: pick a method, then provide the query.

```
Search interface
Please choose a method:
1) Search by author
2) Search by paper_information title
```

#### By title

```
Please enter the paper_information title: <title>
```

If a single paper has that title, results render as:

```
title: <title>
authors: <author1> and <author2> and ...
summary: <contents>
bib entry: <bibtex source>
```

If multiple papers share the title, the user is shown a numbered list and asked to pick:

```
Following papers found:
1: title: <title>
2: title: <title>
Choose paper_information to extract:
```

If no paper matches, the message is `Paper was not found in db_connector.`

#### By author

The author name is required in `Last, First` format:

```
Please enter the author's name: <"Last, First">
```

Multi-paper authors trigger the same numbered-list disambiguation. Single matches render with the same pretty-print format above. No-match prints `Author was not found in db_connector.`

### 2.2 Add (option 2)

A linear prompt sequence (`UserInteraction.add`):

```
Please enter the necessary information
Author(s), please provide a , separated list: <a, b, c>
Paper title: <title>
bibtex key: <key>
Do you want to enter the bibtex entry via a separate file?
1) Yes
2) No
Your choice: 1
Enter filename: <path.bib>           # if "Yes"
bib entry: <pasted source>           # if "No"
summary of the paper_information: <one-line summary>
```

The application then:

1. Validates that `bibtex_key` is unique (`DatabaseConnector.sanity_checks`).
2. Inserts the row into `bib`, then `papers`, then upserts each author into `authors_id`, then links each author to the new paper via `authors_papers`.
3. If anything fails after `bib` was written, calls `DatabaseConnector.rollback_database_addition` to delete the partial `bib` row and any `authors_papers` links already created. (The implementation has a typo in the rollback SQL — `Delete from bib where (bibtex_ident=%s` — which would mean rollback fails silently in production. Treat as a known bug the modernization should fix.)

### 2.3 Update (option 3)

Two-step menu, then identifier, then value, then confirmation.

```
Which information do you want to update?
1) papers
2) bib
3) authors
4) abort
Your choice: 1

Which information do you want to update?
1) title
2) contents
3) abort
Your choice: 1

Which entry do you want to update?
Please enter the respective id: <paper_id>

Enter the new information: <new value>

Please verify: You wish to change 'title' of the entry '<id>' to '<value>'.
 Proceed?
1) (Y)es
2) (N)o
Your choice:
```

Notable rules baked into `UserInteraction.update` and `DatabaseConnector.update_entry`:

- The BibTeX **identifier** (the `bibtex_id` column / primary key) is **not** updatable — only the BibTeX *source* (the `bibtex` column) is.
- For author updates, only the author's name string can change; you cannot reassign which author is on which paper via this flow.
- Confirmation accepts `1` / `y` / `yes` for proceed, `2` / `n` / `no` for abort. Anything else aborts and logs an error.
- **Known bug:** the first-step prompt accepts the menu numbers `1`/`2`/`3` *or* the canonical names `papers`/`bib`/`authors`. `UserInteraction.update` accepts both via `match … case "papers" | "1": …`, but it then forwards `table_to_be_updated` to `DatabaseConnector.update_entry(..., table=…)` *without normalising* — and `update_entry`'s dispatch (`match table: case "papers": …`) only knows canonical names. So entering `1` raises `ValueError("Updating table 1 is not supported.")` which `UserInteraction.update` swallows and prints "Could not update entry — please check logs." Practical effect: the legacy CLI's update path only works when the user types the canonical name, never the menu number. Modernisation fixes this by construction (cli/update.py normalises before calling `paper_service.update_field`).

### 2.4 Delete (no menu entry)

The code has `DatabaseConnector.delete_paper_entry_from_database`, which:

1. Resolves the paper's id from its title.
2. Deletes the corresponding `authors_papers` link rows.
3. For each author that is now without any remaining papers, deletes the author row (this cleanup happens **inline in `delete_author_of_list`**, not via the `__delete_author_with_no_papers` method — the latter exists but is not reached on the standard delete path).
4. Deletes the row from `papers`.
5. Deletes the row from `bib` (unconditionally — there is no check for shared `bibtex_id` in the current code).

This entire flow is reachable from code but is *not* wired into the top-level menu. A user cannot delete via the interactive CLI today.

### 2.5 Bulk import from `.tex` + `.bib` (separate script)

`python paper_sorts/get_data.py -l <literature.tex> -b <bibliography.bib> -c <config> -k <key>` runs a one-shot import:

1. Parse the `.tex` file with `pylatexenc.LatexNodes2Text` to extract entries (heuristic: lines containing `*` and `<cit.>` tokens are treated as titles).
2. Match each title against entries in the `.bib` file via pybtex.
3. For each matched record, insert into `bib`, `papers`, `authors_id`, `authors_papers`, committing per-paper.
4. If a title in the `.tex` has no matching `.bib` record, skip it silently.

This flow lives in **legacy procedural style** — it does not go through `DatabaseConnector` / `PsycopgDB`; it makes raw `psycopg` (v3) calls directly. **Crucially, it uses the column name `bibtext_id` (sic), not `bibtex_id`** — see § 7.2.

## 3. Data model

Four PostgreSQL tables, created by `DatabaseConnector.create_tables()` if they don't already exist. The DDL is verbatim:

```sql
CREATE TABLE IF NOT EXISTS authors_papers (
    id SERIAL PRIMARY KEY,
    author_id INT,
    paper_id INT
);

CREATE TABLE IF NOT EXISTS authors_id (
    id SERIAL PRIMARY KEY,
    author TEXT
);

CREATE TABLE IF NOT EXISTS bib (
    bibtex_id text PRIMARY KEY,
    bibtex text,
    UNIQUE (bibtex)
);

CREATE TABLE IF NOT EXISTS papers (
    id SERIAL PRIMARY KEY,
    title TEXT,
    contents TEXT,
    bibtex_id TEXT,
    CONSTRAINT fk_bibtex_id FOREIGN KEY (bibtex_id) REFERENCES bib(bibtex_id)
);
```

Things that are **not** declared at the schema level even though the application enforces them in code:

- No `NOT NULL` constraints anywhere except those implied by `PRIMARY KEY`. `papers.title`, `papers.contents`, `papers.bibtex_id`, `bib.bibtex`, `authors_id.author`, `authors_papers.author_id`, `authors_papers.paper_id` all permit NULL at the SQL level.
- No foreign keys on `authors_papers`. `author_id` and `paper_id` are bare `INT`s; the schema does not stop you from inserting a link to a non-existent author or paper. The application happens to never produce dangling links in normal use, but a manual SQL session or an interrupted script could.
- No `UNIQUE` constraint on `(authors_papers.author_id, authors_papers.paper_id)`. The pair can repeat — and does, when the import flow re-runs over data that already includes some authors.

The single FK that **is** declared is `papers.bibtex_id → bib.bibtex_id`, plus the `UNIQUE` on `bib.bibtex` (so the same BibTeX source string cannot appear twice).

```
bib (bibtex_id PK, bibtex UNIQUE)
 │
 │ 1 : N
 ▼
papers (id PK, title, contents, bibtex_id FK)
 │
 │ N : N (via authors_papers)
 ▼
authors_papers (id PK, author_id, paper_id)  ← no FKs declared
 ▲
 │
authors_id (id PK, author)
```

### 3.1 The legacy `bibtext_id` column variant

`paper_sorts/add.py`, `paper_sorts/search.py`, and `paper_sorts/get_data.py` all use the column name **`bibtext_id`** (sic — note the misspelling), not `bibtex_id`. A personal database that was first populated by `get_data.py` — which is the documented bulk-import path — will have `papers.bibtext_id` and `bib.bibtext_id` rather than the names that the OO `DatabaseConnector` expects. The two schemas are functionally equivalent but textually divergent; the modernization's Alembic Revision 002 (`002_legacy_bibtext_to_bibtex.py`) is the bridge.

## 4. Control flow

The runtime stack is three layers tall.

```
┌──────────────────────────────────────────────────────────────────┐
│  paper_sorts/user_interaction.py  ── UserInteraction              │
│  • All input() / print() in the application live here.            │
│  • Menu loops, prompt strings, dialog branching.                  │
│  • Talks only to DatabaseConnector — never to psycopg.            │
└──────────────────────────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  paper_sorts/database_connector.py  ── DatabaseConnector          │
│  • Hand-written SQL strings as Python str constants.              │
│  • Multi-step transactional logic (add: 4 inserts + rollback).    │
│  • Search query construction with JOINs.                          │
│  • Calls store_in_db / fetch_from_db / delete_from_db on the      │
│    PsycopgDB attribute.                                           │
└──────────────────────────────────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  paper_sorts/psycopg_db.py  ── PsycopgDB                          │
│  • The ONLY module that imports psycopg2.                         │
│  • Each method opens a connection, runs the query, closes.        │
│  • No connection pooling, no async, no caching.                   │
└──────────────────────────────────────────────────────────────────┘
                              ▼
                       PostgreSQL (local)
```

### 4.1 Boundary rule

The strongest architectural rule in the current codebase is **"only `psycopg_db.py` imports `psycopg2`."** The constitution Principle I (v1.1.0) elevates this to a binding rule: replacing the driver MUST be a single-file change. The modernization redefines this rule at the layer level (`src/paper_sorts/db/` is the only place that imports `sqlalchemy`), but the spirit is the same.

### 4.2 Walkthrough: search by title

1. `UserInteraction.interact()` reads `1` from the top-level menu, calls `self.search(db_connector)`.
2. `UserInteraction.search` reads `2` (title sub-menu), calls `self.search_by_paper_title(db_connector)`.
3. `UserInteraction.search_by_paper_title` reads the title string, calls `db_connector.search_by_title(title)`.
4. `DatabaseConnector.search_by_title` constructs a JOIN across `papers`, `authors_papers`, `authors_id`, and calls `self.database_handler.fetch_from_db(query, (title,))`.
5. `PsycopgDB.fetch_from_db` opens a connection, runs `cur.execute(sql.SQL(query), params)`, calls `cur.fetchall()`, closes.
6. `DatabaseConnector` post-processes the rows: if multiple papers share the title, it returns one row per (paper, joined-author-string); otherwise it returns the single row.
7. `UserInteraction.search_by_paper_title` calls `get_user_choice(papers)` if disambiguation is needed, then `db_connector.search_for_bibtex_entry_by_id(chosen_paper)` to fetch the BibTeX, then `pretty_print_results(bibtex_data, chosen_paper)`. **Known crash:** `pretty_print_results` indexes `bibtex_data[1]`, but `search_for_bibtex_entry_by_id` returns a `fetchall()` *list* of rows (one row), not the unpacked tuple — so the print step raises `IndexError` after emitting `title:`/`authors:` and before the bib line. The same bug fires on the search-by-author journey (§4.x). Modernization replaces both `pretty_print_results` and `search_for_bibtex_entry_by_id` so this is fixed by construction post-T020.

Total round-trips to Postgres for a single-match title search: **3** (the JOIN query, the `search_for_entry_by_specified_paper_information` author-lookup query, and the `search_for_bibtex_entry_by_id` bib-fetch query).

### 4.3 Walkthrough: add (with rollback)

1. `UserInteraction.add` collects six pieces of input (authors comma-separated, title, BibTeX key, source-mode flag, BibTeX source or filename, summary).
2. Calls `db_connector.add_entry_to_db(bibtex, authors, key, title, summary)`.
3. `DatabaseConnector.add_entry_to_db`:
   1. `sanity_checks(bibtex_ident)` — verifies `papers` table exists and the BibTeX key isn't already present.
   2. Inserts into `bib`.
   3. Inserts into `papers`. **If this fails**, deletes the just-inserted `bib` row to avoid orphan bibs (this is the path with the SQL-syntax bug: `Delete from bib where (bibtex_ident=%s)` is missing a closing paren and uses the wrong column name).
   4. Loops over the author list:
      - Inserts the author into `authors_id` (or fetches the existing id).
      - Inserts an `authors_papers` link.
      - On failure, calls `rollback_database_addition` which deletes the bib row and any `authors_papers` links already created.

The rollback path is bespoke; SQLAlchemy's transaction wrapper will replace it during modernization with `with session.begin()`.

### 4.4 Logging

Every class instantiates its own logger via `helpers.create_logger(log_file_name, logger_name, level)`. The result: a single application run produces several log files in the working directory (`db_connector_test.log`, `interaction.log`, `psycopg_logger.log`, etc.). This is bespoke and idiosyncratic; the modernization replaces it with one stdlib `dictConfig` driven by `Settings.log_file`.

## 5. Configuration

Database credentials live in a Fernet-encrypted INI file. `paper_sorts/config_reader.py` (`ConfigReader` class) reads two files at startup:

- The encrypted INI itself (`-c <path>` on the CLI; defaults to `../../database.crypt`).
- The decryption key (`-k <path>`; defaults to `../../key`).

After decryption the INI contains:

```ini
[postgresql]
dbname=...
user=...
password=...
host=...           # optional
port=...           # optional
```

`ConfigReader.db_config` ends up as a plain `dict` that's passed straight into `psycopg2.connect(**config)`. There is no validation, no defaulting, no env-var fallback, and no `.env` support.

The default paths (`../../database.crypt`, `../../key`) only resolve if the program is launched from inside `paper_sorts/` — a quirk the README's `python run.py` command implicitly assumes but doesn't document.

## 6. Install and run

```bash
uv sync --extra legacy-baseline   # pulls psycopg2-binary; required since T002 dropped it
uv run python paper_sorts/run.py -c ../../database.crypt --section postgresql -k ../../key
```

The README example writes `python run.py`, which is the wrong path; run from the repo root with the path above.

For the bulk import:

```bash
python paper_sorts/get_data.py -l ../../literature_overview.tex -b ../../bib.bib -c ../../database.crypt -k ../../key
```

Both invocations read CLI args via `argparse`. The application has no console-script entry point in `pyproject.toml`.

## 7. Known limitations and quirks

These are the items a future maintainer most often trips over. The modernization fixes most of them; some are explicit non-goals.

### 7.1 The legacy procedural modules — `add.py`, `search.py`, `get_data.py`

These three files are **older, standalone, procedural versions** of functionality that has since been refactored into the `UserInteraction` / `DatabaseConnector` / `PsycopgDB` stack. They:

- import `psycopg` (v3), not `psycopg2` like the rest of the codebase;
- use the column name `bibtext_id` (sic);
- duplicate `create_logger` and `read_config` independently of the OO stack's equivalents.

They are not wired into `run.py`. Constitution Principle I (v1.1.0) elevated this to "do not extend." The modernization deletes them entirely (FR-012, T026).

### 7.2 Schema variants `bibtex_id` vs `bibtext_id`

A real personal database can have either column name depending on which entry path populated it. `DatabaseConnector.create_tables` writes `bibtex_id`. `add.py`, `search.py`, `get_data.py` all read/write `bibtext_id`. Alembic Revision 002 in the modernization detects the legacy state and renames in place.

### 7.3 No `NOT NULL`, no FKs on `authors_papers`

The schema (§ 3) is far looser than the data the application actually produces. Tightening it at migration time would break existing personal databases that have edge-case rows (NULL titles, dangling links). The modernization preserves the schema verbatim and moves all tightening to the application layer. See `specs/001-modernize-stack/data-model.md` for the application-vs-schema invariant table.

### 7.4 Duplicate `(author_id, paper_id)` rows

The `add` flow inserts one link row per author per paper. Re-running an import that overlaps an already-imported paper will produce duplicate links. There is no `UNIQUE` constraint on the link table to prevent this. Documented quirk; **out of scope** for the modernization to fix.

### 7.5 Mocked-DB tests are forbidden

Constitution Principle II forbids mocking `PsycopgDB` (or, post-modernization, the SQLAlchemy session) in tests. The current `tests/test_database_connector.py` is therefore an integration test against a live Postgres seeded with specific rows (`"Pino, J."`, `"Wang2021LargeScaleSA"`). Running the suite without that database fails — this is by design, not a regression. The modernization's User Story 3 replaces the developer-local-DB dependency with an ephemeral pytest-postgresql fixture.

### 7.6 Placeholder failing test

`tests/test_user_interaction.py` contains `assertEqual(True, False)` and was presumably scaffolded but never written. It always fails. The modernization deletes it.

### 7.7 Per-class log files

Every class builds its own file-backed logger via `create_logger(log_file_name, logger_name, level)`. A single run produces six log files at the CWD. Idiosyncratic but harmless. The modernization replaces this with a single dict-config.

### 7.8 Bespoke rollback in add path

`DatabaseConnector.add_entry_to_db` tracks the partial state of an in-progress add and, on any failure, calls `rollback_database_addition` to clean up. The rollback SQL has a syntax bug (missing closing paren on the bib delete) that would make the rollback fail silently — but in practice the bug isn't exercised because most add failures hit the `sanity_checks` gate before any insert happens. The modernization replaces all of this with a single `session.begin()` block; SQLAlchemy handles the rollback.

### 7.9 Delete is not in the menu

`DatabaseConnector.delete_paper_entry_from_database` is fully implemented but unreachable via the interactive top-level menu. The modernization keeps this unchanged at the menu level (the four-entry menu is preserved verbatim per spec FR-002 / contracts/cli-commands.md § "Why only four options"); delete becomes reachable via the `pdbsearch delete` subcommand instead of via the menu.

## 8. Module map (current → modernized)

This map is the rename guide for Phase 4 of the modernize-stack feature. Reviewers comparing the modernized branch against this branch can use it to trace equivalence.

| Current location (legacy) | Modernized location | Notes |
|---------------------------|--------------------|-------|
| `paper_sorts/run.py` | `src/paper_sorts/cli/app.py` | argparse → Typer; `main()` is the new console-script entry point |
| `paper_sorts/user_interaction.py` `UserInteraction.search*` | `src/paper_sorts/cli/search.py` | rich-prompt-based dialog |
| `paper_sorts/user_interaction.py` `UserInteraction.add` | `src/paper_sorts/cli/add.py` | + `src/paper_sorts/services/paper_service.py::add_paper` |
| `paper_sorts/user_interaction.py` `UserInteraction.update` | `src/paper_sorts/cli/update.py` | + `services/paper_service.py::update_field` |
| (no current entry — exposed only in code) | `src/paper_sorts/cli/delete.py` | newly menu-less but newly reachable as `pdbsearch delete` |
| `paper_sorts/get_data.py` | `src/paper_sorts/services/import_service.py` + `src/paper_sorts/cli/importer.py` | bulk LaTeX/BibTeX import |
| (none) | `src/paper_sorts/cli/migrate.py` | new — wraps `alembic upgrade head` |
| `paper_sorts/database_connector.py` | `src/paper_sorts/services/paper_service.py` + `src/paper_sorts/db/repositories.py` | service layer split from persistence |
| `paper_sorts/psycopg_db.py` | `src/paper_sorts/db/session.py` | SQLAlchemy session / engine factory |
| (hand-written SQL strings) | `src/paper_sorts/db/models.py` | SQLAlchemy 2.x ORM models |
| (runtime `create_tables()`) | `migrations/versions/001_initial_schema.py` | Alembic revision; verbatim DDL |
| `paper_sorts/add.py`, `paper_sorts/search.py`, `paper_sorts/get_data.py` | (deleted in T026) | legacy procedural; FR-012 |
| `paper_sorts/config_reader.py` `ConfigReader` | `src/paper_sorts/config.py` | pydantic-settings v2 + Fernet custom source |
| `paper_sorts/helpers.py::create_logger` | `src/paper_sorts/logging_config.py` | single stdlib `dictConfig` |
| `paper_sorts/helpers.py::get_user_input`, `get_user_choice`, `cast`, `pretty_print_results` | `src/paper_sorts/cli/prompts.py` | rich-backed wrappers; constitution Principle III v1.3.0 |
| `paper_sorts/helpers.py::get_data`, `get_bibtex_information` | `src/paper_sorts/services/import_service.py` | LaTeX / BibTeX parsing logic |
| `paper_sorts/helpers.py::iterate_through_papers` | `src/paper_sorts/services/paper_service.py` (private helper) | ad-hoc result reshaping for the title-search disambiguation list |

## 9. What this document does *not* cover

- Performance characteristics of the existing implementation. There are no benchmarks today; the modernization's User Story 2 / SC-006 establishes a baseline (T007/T008) and the modernized stack must show no regression against it. Until that baseline exists, any performance claim is a guess.
- The Alembic migration plan and the modernized data model. Those live in `specs/001-modernize-stack/contracts/database-schema.md` and `specs/001-modernize-stack/data-model.md`.
- The constitution itself. See `.specify/memory/constitution.md`.

This document is a snapshot of the implementation as of the commit on which it was written. It exists to make the modernization's equivalence claims testable — once the modernized stack is in place, anyone can compare its behaviour to what is described here flow-by-flow.
