# CLI Command Contract

**Feature**: 001-modernize-stack  
**Date**: 2026-06-14

## Entry Point

```
pdbsearch [--database-url URL] [--log-level LEVEL] [SUBCOMMAND]
```

When invoked with no subcommand, drops into the four-option interactive top-level menu.

## Global Options

| Option | Env var | Description |
|--------|---------|-------------|
| `--database-url` | `PDBSEARCH_DATABASE_URL` | PostgreSQL connection string |
| `--log-level` | `PDBSEARCH_LOG_LEVEL` | Logging level (DEBUG/INFO/WARNING/ERROR) |
| `--config` | — | Path to Fernet-encrypted INI config file |
| `--key` | — | Path to Fernet key file |

## Top-Level Menu (interactive mode)

```
1) Search
2) Add
3) Update
4) Delete
q) Quit
```

Rules:
- 1-indexed numeric input accepted.
- `q` or `Q` accepted for quit.
- Invalid input re-prompts (no crash).

## Subcommands

### `pdbsearch search`

Interactive search submenu:

```
1) Search by title
2) Search by author
3) Back
```

**Search by title**:
- Prompt: `Enter title (or part of title):`
- If no results: `No papers found.`
- If one result: display paper details (title, authors, summary, BibTeX).
- If multiple results: numbered disambiguation list; user selects; out-of-range re-prompts.

**Search by author**:
- Prompt: `Enter author name (Last, First):`
- Displays all matching papers with title and BibTeX key.

### `pdbsearch add`

Interactive add flow:

```
Enter title:
Enter authors (comma-separated, Last First format):
Enter BibTeX key:
Enter summary:
Enter BibTeX entry (or path to .bib file):
```

- All fields required; empty input re-prompts.
- On success: `Paper added successfully.`
- On duplicate BibTeX key: plain-language error, logged at WARNING.

### `pdbsearch update [--id PAPER_ID]`

Interactive update flow:

- If `--id` not provided: prompts for search term to find the paper first.
- Displays current values.
- Submenu:
  ```
  1) Update title
  2) Update contents (summary)
  3) Update BibTeX entry
  4) Update authors
  5) Abort
  ```
- After selection, prompts for new value.
- Confirmation: `Update <field> to "<new_value>"? [y/n]`
- Accepts: `y`, `yes`, `1`, `n`, `no`, `2` (case-insensitive).
- On `n`/`no`/`2`: `Update aborted. No changes made.`

### `pdbsearch delete [--id PAPER_ID]`

Interactive delete flow:

- If `--id` not provided: prompts for search term.
- Displays paper details.
- Confirmation: `Delete "<title>" (id=<id>)? [y/n]`
- On `n`: `Delete aborted.`
- On `y`: deletes paper, all authorship links; BibTeX entry removed if unreferenced.

### `pdbsearch import --tex TEX_FILE --bib BIB_FILE`

Bulk import from LaTeX + BibTeX files:

- Per-paper commits (partial failure leaves inserted papers intact).
- Missing BibTeX record: logged WARNING, entry skipped.
- On completion: `Imported N papers. Skipped M entries.`

### `pdbsearch migrate`

Applies all pending Alembic migrations:

- `alembic upgrade head` equivalent.
- Idempotent: safe to run multiple times.
- On success: `Migration complete.`
- On failure: plain-language error + technical details in log.

## Error Contract

- All errors surface a short plain-language message to stdout.
- Technical details (stack trace, SQL error) go to the log file only.
- No raw exceptions or driver error objects reach stdout.

## Prompt Routing

All user-facing prompts MUST route through `paper_sorts.cli.prompts`. Direct calls to `input()`, `rich.prompt.Prompt.ask`, or `typer.prompt` outside `cli/prompts.py` are violations of constitution Principle III.

## Output Format (paper display)

```
Title: <title>
Authors: <Last, First>; <Last, First>
Summary: <contents>
BibTeX key: <bibtex_id>
BibTeX:
  <bibtex string>
```
