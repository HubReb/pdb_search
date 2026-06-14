# CLI Command Contract

**Feature**: 001-modernize-stack | **Date**: 2026-06-14

## Entry Point

```
pdbsearch [--database-url URL] [--log-level LEVEL] [SUBCOMMAND] [OPTIONS]
```

When invoked with no subcommand, drops into the interactive four-option top-level menu (search / add / update / delete / quit).

`migrate` and `import` subcommands are not in the top-level menu — they are subcommand-only (admin/scripted operations).

## Global Options

| Option | Env var | Default | Description |
|--------|---------|---------|-------------|
| `--database-url` | `PDBSEARCH_DATABASE_URL` | from config | PostgreSQL DSN |
| `--log-level` | `PDBSEARCH_LOG_LEVEL` | `INFO` | Python logging level name |
| `--config` | `PDBSEARCH_CONFIG` | (none) | Path to Fernet-encrypted INI |
| `--key` | `PDBSEARCH_KEY` | (none) | Path to Fernet key file |

## Subcommands

### `pdbsearch search`

Enters interactive search submenu.

Prompts:
1. Search by `(t)itle` or `(a)uthor`? → letter or number choice
2. Enter search term (re-prompts on empty)
3. If multiple results: numbered list with abort option → user picks one
4. Displays: title, authors, summary, BibTeX entry

### `pdbsearch add`

Adds a single paper entry interactively or from a BibTeX file.

Prompts:
1. Enter `.bib` file path, or leave blank to enter manually
2. If manual: title, author(s), BibTeX key, summary (all re-prompt on empty)
3. Confirmation summary before write

### `pdbsearch update`

Updates a field on an existing paper (search-first flow).

Prompts:
1. Search term (title or author)
2. Pick paper from numbered list (with abort)
3. Pick field to update: title / contents / bibtex / author (1-indexed, with abort)
4. Enter new value (re-prompts on empty)
5. Confirmation: `y/n/yes/no/1/2` before write

### `pdbsearch delete`

Deletes a paper (search-first flow).

Prompts:
1. Search term (title)
2. Pick paper from numbered list (with abort)
3. Confirmation: `y/n/yes/no/1/2` before deletion

### `pdbsearch import --tex FILE --bib FILE`

Bulk-imports papers from a `.tex` + `.bib` pair. Not in the interactive menu.

Options:
- `--tex PATH` (required): path to `.tex` file
- `--bib PATH` (required): path to `.bib` file

Behaviour: per-paper commit; skip existing BibTeX keys with logged warning; skip keys with no matching BibTeX record with logged warning.

### `pdbsearch migrate`

Upgrades the personal database schema to the current revision (`alembic upgrade head`). Not in the interactive menu.

Options: none beyond global `--database-url`.

Idempotent: safe to run multiple times.

## Menu Grammar

- Menus are 1-indexed.
- Every menu includes an explicit quit/abort option (e.g. `5) (Q)uit` or `3) abort`).
- Out-of-range input re-prompts without error.
- All prompts route through `paper_sorts.cli.prompts`. Bare `input()` calls outside that module are a violation (Principle III).

## Error Display Contract

- Plain-language message on stdout/stderr: e.g. `"Paper not found."`, `"Database connection failed — check your configuration."`
- Technical detail (exception, SQL error text) logged to the configured logger at ERROR level.
- No raw stack traces or driver error objects reach stdout.

## Confirmation Grammar

Destructive operations (update, delete) accept:
- Numeric: `1` (yes/confirm), `2` (no/abort)
- Word: `y`, `yes`, `n`, `no` (case-insensitive)
