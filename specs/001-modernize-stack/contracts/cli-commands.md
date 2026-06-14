# CLI Commands Contract

**Feature**: 001-modernize-stack | **Date**: 2026-06-14

## Entry Point

```
pdbsearch [OPTIONS] [COMMAND]
```

When invoked with no subcommand, drops into the interactive four-option top-level menu.

## Global Options

```
--database-url TEXT    PostgreSQL DSN (overrides all other sources)
--log-level TEXT       Logging level [default: INFO]
--config PATH          Fernet-encrypted INI config file
--key PATH             Key file for decrypting --config
```

## Subcommands

### `pdbsearch search`

Search the database interactively. Prompts user to choose search method.

```
pdbsearch search
```

Interactive flow:
1. Menu: `1) Search by author  2) Search by title  3) (Q)uit`
2. Prompt for author name or title string
3. If multiple results, disambiguation menu (1-indexed, includes abort option)
4. Display: title, authors, summary, BibTeX entry

### `pdbsearch add`

Add a new paper entry interactively.

```
pdbsearch add [--id TEXT]
```

Interactive flow:
1. Prompt: author(s) (comma-separated "Last, First" list)
2. Prompt: paper title
3. Prompt: BibTeX key
4. Menu: `1) Load BibTeX from file  2) Enter BibTeX inline  3) (A)bort`
5. Prompt: summary
6. Confirmation before write

Option `--id TEXT`: skip the title search and add directly (non-interactive use).

### `pdbsearch update`

Update a field of an existing paper, located by search.

```
pdbsearch update [--id TEXT]
```

Interactive flow:
1. Search to locate the paper (same as `search`)
2. Menu: `1) Title  2) Summary  3) BibTeX  4) Author  5) (A)bort`
3. Prompt for new value
4. Confirmation: `You wish to change '{field}' to '{value}'. Proceed? 1) (Y)es  2) (N)o`
5. Accepts: `1`, `y`, `yes` (proceed) or `2`, `n`, `no` (cancel)

### `pdbsearch delete`

Delete a paper entry, located by search.

```
pdbsearch delete [--id TEXT]
```

Interactive flow:
1. Search to locate the paper
2. Display full paper details
3. Confirmation: `Delete '{title}'? 1) (Y)es  2) (N)o`

### `pdbsearch import`

Bulk import papers from a LaTeX + BibTeX file pair.

```
pdbsearch import --tex PATH --bib PATH
```

Options:
```
--tex PATH    Path to the .tex file with \cite{} references  [required]
--bib PATH    Path to the .bib file with BibTeX entries      [required]
```

Behaviour:
- Per-paper commit (partial failures do not roll back previously inserted papers)
- Entries with existing BibTeX key are skipped with a logged warning
- Entries in .tex with no matching .bib record are skipped with a logged warning

### `pdbsearch migrate`

Apply pending Alembic database migrations.

```
pdbsearch migrate [--database-url TEXT]
```

Behaviour:
- Runs `alembic upgrade head`
- Idempotent: safe to run on already-migrated databases
- Handles both legacy schema variants (bibtex_id and bibtext_id column names)
- Admin/scripted operation — NOT in the four-option interactive menu

## Menu Grammar Rules (constitution III)

- All menus are 1-indexed in display
- Every menu includes an explicit abort/quit option
- Destructive operations (update, delete) require a confirmation step summarising the change
- Confirmation accepts `1`/`y`/`yes` (proceed) and `2`/`n`/`no` (cancel)
- Empty input on a required prompt re-prompts until non-empty value given
- All prompts route through `paper_sorts.cli.prompts` — no bare `input()` elsewhere

## Error Behaviour

- Failed database operation: plain-language message to stdout + technical details to logger
- No raw exception or stack trace on stdout
- Config file present but key missing: "Cannot decrypt config: key file not found. Check --key path."
- Ctrl-C mid-dialog: exits without partial database state (context-managed transactions)
