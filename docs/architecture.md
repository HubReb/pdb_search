# paper_sorts — Architecture

This document reverse-engineers the **legacy** (pre-modernization) stack as the
acceptance reference for the rebuild (spec 001-modernize-stack, FR-001), then
maps each legacy piece to its modern replacement. A Python developer who has
never seen the project should be able to answer, from this document alone:
*what does it do, how is the data modeled, and where would I add a new field?*

## 1. Purpose

paper_sorts is an **off-line, single-user CLI** that stores publication
metadata — title, authors, one-sentence summary, and the full BibTeX entry — in
a local PostgreSQL database, and lets the owner search, add, update, delete, and
bulk-import entries. It is a personal-use tool (the README cites reading papers
"traveling by train"), not a library or a service. There is no network surface,
no authentication, no multi-user concern.

## 2. User journeys

- **Search**: choose *by author* or *by title*; the tool prints the matching
  paper(s); when several papers share a title, it asks the user to pick one from
  a numbered list, then pretty-prints title / authors / summary / BibTeX.
- **Add**: type the author list, title, BibTeX key, and summary, and either
  paste the BibTeX entry inline or point at a single-entry `.bib` file.
- **Update**: change a paper's title or contents, the BibTeX source, or an
  author's name — each behind a confirmation that summarises the exact change.
- **Delete**: remove a paper and its BibTeX entry, author links, and any author
  left with no remaining papers.
- **Bulk import**: from a LaTeX literature-overview `.tex` plus a matching
  `.bib`, insert every cited entry that has a BibTeX record (one-shot, used to
  bootstrap the database).

## 3. Data model (four tables)

| Table | Columns | Role |
|-------|---------|------|
| `bib` | `bibtex_id` (PK, TEXT), `bibtex` (TEXT, UNIQUE) | the full BibTeX source, keyed by the BibTeX key |
| `papers` | `id` (PK, SERIAL), `title`, `contents`, `bibtex_id` (FK → `bib.bibtex_id`) | one publication record |
| `authors_id` | `id` (PK, SERIAL), `author` (`"Last, First"`) | one credited person |
| `authors_papers` | `id` (PK, SERIAL), `author_id`, `paper_id` | many-to-many link, **no DDL foreign keys** |

Relationships: a **Paper** has exactly one **Bib** (via `papers.bibtex_id`, the
only real FK) and many **Authors** (via the `authors_papers` link). The BibTeX
key is the user-facing unique identifier; `papers.id` is the internal identity.

**Where would I add a new field?** A scalar paper attribute → a new nullable
column on `papers` (declared on the `Paper` ORM model + an Alembic migration). A
new relationship → a new table + link, again model + migration. Do **not** add
NOT NULL (outside PKs), foreign keys on `authors_papers`, or new indexes — that
is the schema-preservation contract (the original DDL is a contract; revision
001 reproduces it verbatim).

## 4. Control flow

### Legacy (pre-modernization)

Three layers, top to bottom:

1. **`paper_sorts/user_interaction.py` — `UserInteraction`**: all CLI dialog
   (`input()`/`print()`), a four-option top menu (Search / Add / Update / Quit)
   and the search/add/update sub-dialogs. The only place stdin/stdout was
   touched.
2. **`paper_sorts/database_connector.py` — `DatabaseConnector`**: high-level
   domain operations (`search_by_author`, `search_by_title`, `add_entry_to_db`,
   `update_entry`, `delete_paper_entry_from_database`) holding the hand-written
   SQL strings and the multi-step transactional logic.
3. **`paper_sorts/psycopg_db.py` — `PsycopgDB`**: a thin wrapper around the
   `psycopg2` driver (`store_in_db` / `fetch_from_db` / `delete_from_db` /
   `update_db_entry`) — the single point that imported the driver.

Supporting legacy modules: `config_reader.py` (Fernet-decrypted INI →
credentials dict), `helpers.py` (`create_logger`, `cast`, `get_user_input`,
`get_user_choice`, `pretty_print_results`, plus LaTeX/BibTeX parsing), and the
older standalone procedural scripts `add.py` / `search.py` / `get_data.py`
(which used `psycopg` v3 and the **`bibtext_id`** typo column — a real schema
divergence the migration must handle).

**What happens if a partial add fails midway?** `DatabaseConnector.add_entry_to_db`
inserts the bib row, then the paper row, then links each author. On an author
failure it calls `rollback_database_addition`, which deletes the bib row and any
author-paper links already created, so the database is not left half-written.
(The legacy rollback SQL had bugs — e.g. `Delete from bib where (bibtex_ident=%s`
— which the modern session-level rollback eliminates.)

### Modern (this rebuild)

Same four conceptual layers, now on mainstream libraries:

- **`cli/` (Typer)** — `app.py` wires subcommands `search`/`add`/`update`/
  `delete`/`import`/`migrate` and, with no subcommand, drops into the four-option
  top menu. `cli/prompts.py` is the single prompt-routing seam (the only module
  allowed to import `rich.prompt`).
- **`services/`** — `paper_service.py` (search/add/update/delete orchestration
  over DTOs) and `import_service.py` (the `.tex`+`.bib` extractor). Pure
  orchestration: no SQL, no rich, no I/O.
- **`db/` (SQLAlchemy 2.x)** — `models.py` (the four declarative models),
  `session.py` (`with_session`: commit/rollback/close), `repositories.py`
  (`PaperRepository`/`AuthorRepository`/`BibRepository` + the `PaperSummary`/
  `PaperCreate` DTOs). The **only** package that imports `sqlalchemy`/`psycopg`.
- **`config.py`** (pydantic-settings, four-source chain) and
  **`logging_config.py`** (stdlib `dictConfig`).

A partial add now fails inside a single `with_session(...)` block, so the
session rollback unwinds the whole unit of work atomically — no bespoke
compensation SQL.

## 5. Configuration

Resolved by `config.py` from four sources, highest priority first: CLI flags
(`--database-url`, `--log-level`, `--log-file`) → environment (`PDBSEARCH_*`) →
`.env` → a **Fernet-encrypted INI** (`--config <path> --key <path>`), which
preserves the legacy encrypted-credentials workflow as one supported source.
A lost or wrong key yields a clear, actionable error — never a stack trace.
Credentials, keys, and encrypted files are never committed or logged.

## 6. Install / run

```bash
uv sync --all-extras
uv run pdbsearch              # interactive top menu
uv run pdbsearch search       # or add / update / delete / import / migrate
uv run pdbsearch --help
```

See `quickstart.md` (in the feature spec) and the README for the full command
and configuration reference. Schema is created and converged via Alembic
(`pdbsearch migrate` / `uv run alembic upgrade head`), not at runtime.

## 7. Known limitations & quirks (carried forward, documented)

- **Duplicate authors**: two people with an identical `"Last, First"` string are
  treated as the same author — no disambiguation. Documented, unchanged.
- **Two historical schemas**: the OO stack used `bibtex_id`; the older
  procedural scripts used the typo `bibtext_id` (and `bibtext`). The `migrate`
  command (Alembic revision 002) converges both onto the canonical names,
  idempotently and with zero data loss (renames move no rows).
- **Bulk-import idempotency**: re-running an import skips BibTeX keys already
  present (BibTeX-key uniqueness), so it never duplicates.
- **BibTeX accents**: LaTeX escapes (e.g. `M{\"u}ller`) round-trip through the
  pybtex parser without corruption when displayed.
- **Orphan cleanup**: deleting a paper (or renaming an author away) removes any
  author row left with no papers, matching legacy behaviour.
