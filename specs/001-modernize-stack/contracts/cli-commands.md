# CLI Command Contract

**Feature**: 001-modernize-stack  
**Date**: 2026-06-15

## Entry Point

```
pdbsearch [--database-url URL] [--log-level LEVEL] [SUBCOMMAND]
```

When invoked with **no subcommand**, the CLI drops into the four-option interactive top-level menu.

## Global Options

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--database-url TEXT` | `PDBSEARCH_DATABASE_URL` | (required) | SQLAlchemy DB URL |
| `--log-level TEXT` | `PDBSEARCH_LOG_LEVEL` | `INFO` | Logging level |
| `--config PATH` | `PDBSEARCH_CONFIG_FILE` | none | Fernet-encrypted INI file |
| `--key PATH` | `PDBSEARCH_KEY_FILE` | none | Decryption key file |

## Top-Level Interactive Menu (no subcommand)

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
Your choice:
```

- Input accepted: `1`, `2`, `3`, `4`, `q` (case-insensitive).
- Re-prompts on invalid input.
- `4` / `q` exits cleanly.
- `migrate` and `import` are subcommand-only — not in the interactive menu.

## Subcommands

### `pdbsearch search`
Interactive subcommand. Prompts for search method then search term.

```
Search interface
Please choose a method:
1) Search by author
2) Search by title
3) (Q)uit
Your choice:
```

### `pdbsearch add`
Interactive subcommand. Prompts for author(s), title, BibTeX key, BibTeX entry (inline or file), summary.

### `pdbsearch update`
Interactive subcommand. Prompts for table → column → identifier → new value → confirmation.

Confirmation prompt accepts: `1`/`y`/`yes` (proceed) or `2`/`n`/`no` (abort).

### `pdbsearch delete`
Interactive subcommand. Prompts for BibTeX key, then shows the paper to confirm deletion.

### `pdbsearch import --tex TEX_FILE --bib BIB_FILE`
Batch import. Non-interactive. Commits per paper.

### `pdbsearch migrate`
Applies all pending Alembic migrations. Idempotent. Non-interactive.

## UX Rules (from constitution Principle III)

1. Menus are 1-indexed.
2. Every menu has an explicit abort/quit option.
3. Empty input re-prompts until non-empty.
4. Destructive operations (update, delete) present a confirmation step.
5. Confirmation accepts both numeric and word forms.
6. Raw exceptions never reach stdout — plain-language messages only.
7. All prompts route through `paper_sorts.cli.prompts` module.
