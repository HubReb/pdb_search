# CLI Commands Contract

**Feature**: 001-modernize-stack | **Date**: 2026-06-14

## Entry Point

```
pdbsearch [--database-url URL] [--log-level LEVEL] [SUBCOMMAND]
```

When invoked with no subcommand, drops into the interactive four-option top-level menu. All subcommands can also be invoked directly (for scripting).

## Global Options

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--database-url` | `PDBSEARCH_DATABASE_URL` | (from config) | PostgreSQL DSN |
| `--log-level` | `PDBSEARCH_LOG_LEVEL` | `INFO` | Logging level |
| `--config` | `PDBSEARCH_CONFIG` | `../../database.crypt` | Encrypted config path |
| `--key` | `PDBSEARCH_KEY` | `../../key` | Fernet key file path |

## Subcommands

### `pdbsearch search`

Interactive: prompts for search method (author or title), then the search term. Displays results in Rich pretty-print format. No-results and multiple-results cases both handled interactively.

**Menu**:
```
Search interface
1) Search by author
2) Search by paper title
3) (Q)uit
Your choice:
```

**Output** (one result):
```
title: <title>
authors: <Author1, Author2, ...>
summary: <contents>
bib entry: <full BibTeX string>
```

**Multiple results**: disambiguation menu listing titles (1-indexed, with quit option).

### `pdbsearch add`

Interactive: prompts author list (comma-separated), title, BibTeX key, BibTeX source (inline or from file), summary. Confirmation before writing.

**Prompts**:
```
Author(s) (comma-separated): 
Paper title: 
BibTeX key: 
BibTeX entry — enter inline (1) or from file (2):
  1) Inline
  2) From file
  3) abort
Your choice:
[if 1] BibTeX entry: 
[if 2] BibTeX filename: 
Summary: 
```

**Confirmation**:
```
Add paper "<title>" by <authors>? 
  1) (Y)es
  2) (N)o
Your choice:
```

### `pdbsearch update`

Interactive: prompts which table/column to update, the entry identifier, and the new value. Confirmation before writing.

**Table menu**:
```
Which information do you want to update?
1) papers
2) bib
3) authors
4) abort
Your choice:
```

**Papers column menu**:
```
1) title
2) contents
3) abort
Your choice:
```

**Confirmation**:
```
Change '<column>' of entry '<id>' to '<new value>'?
  1) (Y)es
  2) (N)o
Your choice:
```

Confirmation MUST accept both `1`/`2` and `y`/`n`/`yes`/`no`.

### `pdbsearch delete`

Interactive: prompts for the BibTeX key of the entry to delete. Displays the matching paper. Confirmation before deleting.

**Confirmation**:
```
Delete paper "<title>"?
  1) (Y)es
  2) (N)o
Your choice:
```

### `pdbsearch import`

Subcommand-only (not in top-level interactive menu). Bulk import from a `.tex` + `.bib` file pair.

```
pdbsearch import --tex TEXFILE --bib BIBFILE
```

Skips entries with no matching BibTeX record (logs warning). Commits per-paper.

### `pdbsearch migrate`

Subcommand-only (not in top-level interactive menu). Runs pending Alembic migrations.

```
pdbsearch migrate [--revision HEAD]
```

## UX Rules (from constitution Principle III)

- All menus MUST be 1-indexed.
- Every menu MUST include an explicit quit/abort option.
- Destructive operations (update, delete) MUST show a confirmation step with the exact change before applying it.
- Confirmation MUST accept both numeric (`1`/`2`) and word (`y`/`n`/`yes`/`no`) forms.
- Empty input on any required prompt MUST re-prompt (never silently accept blank).
- Failure messages MUST be plain-language on stdout; technical details go to the log file.
- Raw exceptions, stack traces, and driver error objects MUST NOT reach stdout.

## Prompt Routing Rule (constitution Principle III)

All `input()`, `rich.prompt.Prompt.ask`, and `typer.prompt` calls MUST live in `src/paper_sorts/cli/prompts.py`. No other module under `src/paper_sorts/` may call these directly.
