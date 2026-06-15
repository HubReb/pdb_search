# CLI Contract: `pdbsearch`

The CLI is the entire product surface (Constitution Principle III). This
contract defines the command grammar the modern Typer app MUST honour. All
prompts route through `paper_sorts.cli.prompts`; menus are 1-indexed with an
explicit quit/abort; destructive ops confirm with dual-form (numeric + word)
acceptance; failures log detail and surface a short plain-language line.

## Invocation

```
pdbsearch [GLOBAL OPTIONS] [COMMAND] [ARGS]
```

Global options (resolved by `paper_sorts.config.Settings`, priority high→low:
CLI flag > `PDBSEARCH_*` env > `.env` > Fernet INI):

| Option | Meaning |
|---|---|
| `--database-url TEXT` | SQLAlchemy URL (`postgresql+psycopg://…`) |
| `--config PATH` | Fernet-encrypted INI config file |
| `--key PATH` | Fernet key file (decrypts `--config`) |
| `--log-level TEXT` | DEBUG/INFO/WARNING/ERROR (default INFO) |
| `--help` | Show help and exit |

## No-subcommand mode → four-option top-level menu

`pdbsearch` with no subcommand drops into the legacy interactive menu:

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
Your choice:
```

- 1-indexed; option 4 / `q` quits ("Closing connection…").
- Invalid input → "Your input was invalid", re-prompt.
- `import` and `migrate` are **not** on this menu (admin/scripted only).

## Subcommands

### `pdbsearch search`
Interactive: choose `1) by author` / `2) by title`.
- **By title, one match**: print title, authors (`A and B and …`), summary,
  BibTeX entry (the legacy "pretty print").
- **By title, multiple matches**: 1-indexed disambiguation list; out-of-range
  choice re-prompts; chosen paper printed.
- **By author**: same disambiguation when the author has >1 paper.
- Not found → plain-language "not found" message (no stack trace).

### `pdbsearch add`
Prompts in order: authors (comma-separated `Last, First` list), title, BibTeX
key, then "enter BibTeX via file? 1) Yes 2) No":
- **Yes** → filename prompt, read file contents as the BibTeX entry.
- **No** → inline BibTeX entry prompt.
Then summary. Empty input on any required prompt re-prompts (never accepts
empty). Persists the paper; retrievable afterward by both author and title.
Duplicate BibTeX key → plain-language error, logged detail.

### `pdbsearch update`
Menu: `1) papers 2) bib 3) authors 4) abort`.
- **papers** → `1) title 2) contents 3) abort`.
- **bib** → only `bibtex` editable (key immutable; message says so).
- **authors** → only author *name* editable.
Then prompt for the entry id and the new value, then a confirmation that
summarises the exact change:
```
You wish to change '<col>' of entry '<id>' to '<value>'. Proceed?
1) (Y)es  2) (N)o
```
Confirmation accepts `1`/`2`/`y`/`n`/`yes`/`no`. `y` → persist; `n`/abort →
no write. Update error → "Could not update entry — please check logs."

### `pdbsearch delete`
Identify the target paper, summarise it, confirm (dual-form) before removing
the paper, its authorship links, orphaned authors, and its bib row. Abort →
no change.

### `pdbsearch import` (subcommand-only)
```
pdbsearch import --tex PATH --bib PATH
```
Extracts every cited entry with a matching BibTeX record and inserts it.
- Citation key with no `.bib` match → skipped with a logged warning (import
  does not fail).
- Per-paper commit: a mid-run failure preserves earlier papers; rerun skips
  already-present keys.

### `pdbsearch migrate` (subcommand-only)
```
pdbsearch migrate
```
Upgrades a personal database in either historical schema (`bibtex_id` or
legacy `bibtext_id`) to the canonical schema in one action, zero data loss,
idempotent (rerun is a no-op or a clean completion — never half-migrated).
Internally runs Alembic to head.

## Exit & error contract

- Plain-language errors to stdout; full detail (exceptions, driver errors) to
  the log only — never to stdout (Principle III, FR-003, US2-6).
- Ctrl+C mid-dialog exits without leaving the DB in a partial state
  (transactions via `with_session`).
- Missing key / missing encrypted config → clear actionable message, not a
  stack trace.

## Delta vs. legacy CLI (for the architecture doc)

| Legacy | Modern | Behaviour |
|---|---|---|
| `python -m paper_sorts.run` + argparse `--config/--key/--section` | `pdbsearch` + Typer global options | same config inputs, new surface |
| top-level dialog loop in `UserInteraction.interact` | `cli/app.py` four-option menu | identical menu text/semantics |
| `search.py` / `add.py` / `get_data.py` standalone scripts | `search`/`add`/`import` subcommands | same operations, unified entry point |
| per-class `*.log` FileHandlers | one `dictConfig`, RichHandler + optional file | structured, configurable sink |
