# Architecture (reverse-engineered baseline)

This document is the acceptance reference for the modernization (FR-001): it
captures what the application does, how its layers interact, how data flows, how
it is configured, how it is installed and run, and its known limitations — so
that the rebuilt system can be checked against it path by path. It describes the
**legacy** procedural/OO stack as it stood before modernization, and notes where
the modern stack replaces each piece.

## Purpose

`pdbsearch` is a personal, offline CLI for storing publication metadata (title,
authors, summary, BibTeX entry) in a local PostgreSQL database and searching it
by author or title. It is a single-user tool — not a library or service. The
motivating use case is looking up papers with no internet connection (the README
cites "traveling by train").

## User journeys

1. **Search** — choose *by author* or *by title*; on multiple hits, pick from a
   numbered list; the tool prints title, authors, summary, and the BibTeX entry.
2. **Add** — enter authors (comma-separated `Last, First`), title, BibTeX key,
   then either type the BibTeX entry or point at a `.bib` file; enter a summary;
   the paper is persisted.
3. **Update** — choose a table (papers / bib / authors), a column, the entry id,
   and the new value; confirm; the change is applied.
4. **Delete** — identify a paper, confirm, and it (with its links, orphaned
   authors, and bib row) is removed.
5. **Bulk import** — point at a `.tex` literature overview and its `.bib`; every
   cited entry with a matching record is inserted.
6. **Migrate** — bring an existing personal database onto the canonical schema.

## Data model (four tables)

| Table | Columns | Notes |
|---|---|---|
| `bib` | `bibtex_id` (TEXT PK), `bibtex` (TEXT UNIQUE) | the BibTeX entry, keyed by its key |
| `papers` | `id` (SERIAL PK), `title`, `contents`, `bibtex_id` (FK → `bib`) | a publication; `title`/`contents` nullable |
| `authors_id` | `id` (SERIAL PK), `author` | an author, name in `"Last, First"` form |
| `authors_papers` | `id` (SERIAL PK), `author_id`, `paper_id` | many-to-many link, **no DDL FKs** |

Relationships: a **paper** has exactly one **bib** entry and one or more
**authors**, linked through `authors_papers`. An **author** appears on zero or
more papers. The link table has no foreign keys — the relationship is enforced
only in application code. The user-facing unique identifier is the BibTeX key
(`bibtex_id`); the internal identity is `papers.id`.

**Where would I add a new field?** A new paper attribute is a column on
`papers` plus a field on the `PaperSummary`/`PaperCreate` DTOs and an Alembic
migration under `migrations/versions/`. A new author attribute is a column on
`authors_id` plus the same DTO/migration touch points. No code outside
`db/` constructs SQL, so the blast radius is the model, the migration, the
repository method that reads/writes the column, and the DTO.

## Control flow (CLI dialog → domain → persistence)

Legacy (three layers, top to bottom):

1. `UserInteraction` (`user_interaction.py`) — all `input()`/`print()`; drives a
   `DatabaseConnector` based on menu choices.
2. `DatabaseConnector` (`database_connector.py`) — high-level domain operations;
   holds the hand-written SQL strings and the multi-step transactional logic
   (add paper → add bib → link authors → roll back on partial failure).
3. `PsycopgDB` (`psycopg_db.py`) — a thin wrapper that is the *only* module
   importing the driver (`psycopg2`); exposes `store_in_db`/`fetch_from_db`/
   `delete_from_db`/`update_db_entry`.

Modern (same shape, mainstream libraries):

1. `cli/` (Typer) — `app.py` wires subcommands and the four-option menu; all
   prompts route through `cli/prompts.py`.
2. `services/` — `paper_service.py` (search/add/update/delete) and
   `import_service.py` (tex+bib extraction); pure orchestration over DTOs.
3. `db/` — SQLAlchemy 2.x: `models.py`, `session.py` (`with_session`),
   `repositories.py` (repositories + DTOs). The **only** place importing
   SQLAlchemy/the driver.

A search flows: prompt (`cli/prompts`) → `PaperService.search_by_title` →
`PaperRepository.search_by_title` (parameterised join over the four tables) →
`PaperSummary` DTO → pretty-print. No SQL leaves `db/`; no driver type reaches
the service layer.

### What happens if a partial add fails midway?

Legacy: `DatabaseConnector.add_entry_to_db` inserts the bib row, the paper row,
then links each author; on a failure during author linking it calls
`rollback_database_addition`, which manually deletes the bib row and any
author-paper links created so far. Modern: the entire add runs inside one
`with_session` transaction — an exception rolls the whole unit back
automatically, so no half-written paper persists. Ctrl+C mid-dialog likewise
leaves no partial state (the transaction never commits).

## Configuration

Legacy: a Fernet-encrypted INI file (`database.crypt`) decrypted with a key file
at startup (`ConfigReader` / `read_config`), yielding `[postgresql] dbname=…
user=… password=…`. Modern: `paper_sorts.config.Settings` (pydantic-settings)
resolves four sources, highest priority first — CLI flags > `PDBSEARCH_*` env >
`.env` > the Fernet-encrypted INI (preserved as one source). A missing key or
missing config file yields a clear, actionable error, not a stack trace.

## Install and run

Modern (uv, Python ≥ 3.11):

```bash
uv sync --all-extras
uv run pdbsearch            # interactive four-option menu
uv run pdbsearch search     # or: add / update / delete / import / migrate
uv run pdbsearch --help
```

See [quickstart](../specs/001-modernize-stack/quickstart.md) for configuration
and the full command grammar.

## Known limitations and quirks (preserved)

- **Duplicate authors**: two authors with identical `"Last, First"` strings are
  treated as the same author. Searching either finds both their papers. This is
  inherited behaviour, documented and preserved.
- **Author-list ambiguity on add**: the add prompt splits the author line on
  `", "`, so a single `Last, First` name is indistinguishable from two authors.
  This is the legacy `split(", ")` behaviour, preserved verbatim.
- **Two historical schema spellings**: older procedural modules created the
  schema with a misspelled `bibtext_id` column; the modern `DatabaseConnector`
  lineage used `bibtex_id`. The canonical schema uses `bibtex_id`; the `migrate`
  command (Alembic revision 002) converges a legacy `bibtext_id` database onto
  it idempotently, with zero data loss.
- **Interrupted bulk import**: re-running an import skips already-present BibTeX
  keys (PK uniqueness) rather than duplicating; per-paper commit means a partial
  failure preserves earlier papers.
- **Schema looseness preserved**: `papers.title/contents` and `authors_id.author`
  are nullable, and `authors_papers` has no foreign keys — exactly as the
  original DDL, which the modernization treats as a binding contract (it does not
  add NOT NULL, FKs on the link table, or indexes the original lacked).
