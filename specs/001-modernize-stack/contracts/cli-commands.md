# CLI Command Grammar: pdbsearch

**Feature**: 001-modernize-stack | **Date**: 2026-06-15

## Entry Point

```
pdbsearch [--database-url URL] [--log-level LEVEL] [SUBCOMMAND] [OPTIONS]
```

When invoked with no subcommand, `pdbsearch` drops into an interactive top-level menu with four options.

## Global Options

| Option | Env Var | Default | Description |
|--------|---------|---------|-------------|
| `--database-url` | `PDBSEARCH_DATABASE_URL` | (from config) | PostgreSQL DSN |
| `--log-level` | `PDBSEARCH_LOG_LEVEL` | `INFO` | Logging level |
| `--config` | — | — | Path to Fernet-encrypted INI config |
| `--key` | — | — | Path to Fernet key file |

## Subcommands

### `pdbsearch search`
Interactive search dialog.
- Prompts: search by author OR search by paper title
- On multiple matches: disambiguation numbered list (1-indexed, abort option)
- Output: title, authors, summary, BibTeX entry

```
pdbsearch search
```

### `pdbsearch add`
Add a new paper interactively.
- Prompts: author(s), title, bibtex key, bibtex (inline or from file), summary
- Confirmation before write

```
pdbsearch add
```

### `pdbsearch update`
Update an existing paper field interactively.
- Prompts: table (papers/bib/authors), field, identifier, new value
- Confirmation before write (shows: field, current identifier, new value)

```
pdbsearch update
```

### `pdbsearch delete`
Delete a paper from the database.
- Search first, then confirm deletion with paper summary
- Destructive confirmation required (y/n or 1/2)

```
pdbsearch delete
```

### `pdbsearch import` (admin — not in interactive menu)
Bulk import from LaTeX + BibTeX file pair.

```
pdbsearch import --tex <file.tex> --bib <file.bib>
```

| Option | Required | Description |
|--------|----------|-------------|
| `--tex` | YES | Path to .tex file |
| `--bib` | YES | Path to .bib file |

### `pdbsearch migrate` (admin — not in interactive menu)
Apply Alembic migrations to the database.

```
pdbsearch migrate [--revision HEAD]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--revision` | `head` | Target Alembic revision |

## Interactive Top-Level Menu (no subcommand)

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
Your choice:
```

- 1-indexed; option 4 / `q` exits
- Invalid input re-prompts
- Each option delegates to the corresponding subcommand logic

## Prompt Grammar (constitution Principle III)

1. All prompts route through `paper_sorts.cli.prompts` — no bare `input()` outside that module
2. Numbered menus are 1-indexed in display
3. Every numbered menu includes an explicit abort/quit option as the last item
4. Required fields re-prompt on empty input (no silent accept)
5. Destructive operations present a confirmation summary before executing
6. Confirmation accepts: `1`, `y`, `yes` (proceed) and `2`, `n`, `no` (cancel)
7. Error messages on stdout are plain-language; technical details go to the log file

## Error Handling Contract

| Situation | User sees | Log level |
|-----------|-----------|-----------|
| Paper not found | "Paper not found in database." | INFO |
| Author not found | "Author not found in database." | INFO |
| DB operation failed | "Could not complete operation — see log for details." | ERROR |
| Config error (missing key file) | "Could not decrypt config: key file not found at <path>." | ERROR |
| Duplicate bibtex_id on add | "Entry with BibTeX key '<key>' already exists." | WARNING |
| Ctrl+C mid-dialog | Graceful exit; session rolled back automatically by SQLAlchemy | — |
