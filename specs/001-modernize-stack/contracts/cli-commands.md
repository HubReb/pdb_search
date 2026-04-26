# CLI Command Contract

**Feature**: 001-modernize-stack
**Component**: `paper_sorts/cli/`
**Audience**: Anyone writing or testing the modernized CLI; the acceptance reference for spec User Story 2.

This document specifies the externally-visible contract of the CLI. The contract is the surface that the integration tests (US3) and the migration acceptance script (US2) drive.

## Top-level invocation

```text
pdbsearch [--config PATH] [--key PATH] [--database-url URL] [--log-level LEVEL] [SUBCOMMAND ...]
```

| Option            | Source                | Notes                                                                        |
|-------------------|-----------------------|------------------------------------------------------------------------------|
| `--config`        | path                  | Fernet-encrypted INI config. If omitted, falls back to env / `.env`.         |
| `--key`           | path                  | Fernet decryption key. Required iff `--config` is set.                       |
| `--database-url`  | str                   | Direct override; overrides any other source.                                 |
| `--log-level`     | str (DEBUG/INFO/...)  | Defaults to `INFO`.                                                          |

If no subcommand is given, drops into the **interactive top-level menu** (preserves existing UX).

## Top-level interactive menu (default command)

```text
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) Delete an entry          # NEW: now exposed in the menu (FR-002 requires delete)
5) Import from .tex + .bib  # NEW: explicit menu entry; was a separate script before
6) (Q)uit
Your choice:
```

Grammar (constitution Principle III, v1.2.0):
- 1-indexed numeric.
- Mandatory abort/quit at the bottom.
- Empty input re-prompts.
- `q` is accepted in addition to `6`.
- Selecting an option dispatches to the corresponding subcommand's interactive flow, then returns to the menu when that flow completes.

## Subcommand: `search`

### Non-interactive form

```text
pdbsearch search --by author --query "Pino, J."
pdbsearch search --by title --query "Direct speech-to-speech translation with discrete units"
```

### Interactive form (default if `--by` and `--query` not given, or if invoked from the top-level menu)

```text
Search interface
Please choose a method:
1) Search by author
2) Search by paper title
Your choice: 2
Please enter the paper title:
> Direct speech-to-speech translation with discrete units
```

### Behaviour

- If multiple papers match the title or author, present a 1-indexed disambiguation list (preserves `get_user_choice`).
- On a single match, render results as:
  ```
  title: <title>
  authors: <author1> and <author2> and <authorN>
  summary: <contents>
  bib entry: <bibtex>
  ```
- On no match, print plain-language `Author was not found in db_connector.` / `Paper was not found in db_connector.` (preserved verbatim) and return to caller.

### Exit code

`0` on any successful completion (including "no match"). Non-zero only on infrastructure failure (DB unreachable, config invalid).

## Subcommand: `add`

### Non-interactive form

```text
pdbsearch add --bib-file paper.bib --summary "one-sentence summary"
```

### Interactive form (default)

Same prompt sequence as the current `UserInteraction.add` method:

```text
Please enter the necessary information
Author(s), please provide a , separated list:
Paper title:
bibtex key:
Do you want to enter the bibtex entry via a separate file?
1) Yes
2) No
Your choice:
[if 1] Enter filename:
[if 2] bib entry:
summary of the paper_information:
```

### Behaviour

- BibTeX key uniqueness is enforced before any insert. Duplicate key → plain-language error, no insert.
- If the BibTeX file path does not exist → plain-language error, return to caller without partial state.
- All four tables are written within a single SQLAlchemy transaction; partial failure rolls back automatically (replaces the bespoke `rollback_database_addition` logic).

## Subcommand: `update`

Preserves the existing two-step menu:

```text
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
Please enter the respective id: 42

Enter the new information: <new title>

Please verify: You wish to change 'title' of the entry '42' to '<new title>'.
 Proceed?
1) (Y)es
2) (N)o
Your choice:
```

### Behaviour

- Non-existent `id` → plain-language error, no write.
- Updating the BibTeX `bibtex_id` is forbidden (preserves current behaviour: "the bibtex identifier cannot be changed"); only `bibtex` (the source string) can be updated.
- Confirmation accepts `1`/`y`/`yes` (proceed) or `2`/`n`/`no` (abort). Anything else logs an error and aborts.

## Subcommand: `delete`

```text
pdbsearch delete --id 42
```

Or interactive: prompts for the paper id, then a confirmation:

```text
Please verify: You wish to DELETE paper id 42 ('<title>'). This cannot be undone.
1) (Y)es
2) (N)o
Your choice:
```

### Behaviour

- Cascades to `authors_papers` rows for that paper. If any author has no remaining papers afterward, the author row is also deleted (preserves current `__delete_author_with_no_papers` behaviour).
- BibEntry is deleted iff no other paper references it.
- All in one transaction.

## Subcommand: `import`

```text
pdbsearch import <tex-file> <bib-file>
```

Non-interactive bulk import. Reads citations from the `.tex` overview, looks up each in the `.bib` file via pybtex, and inserts one paper per matching entry.

### Behaviour

- Per-paper commit (constitution Principle IV: "bulk import paths MUST commit per-paper, so a partial failure leaves the database in a consistent state recoverable on rerun").
- BibTeX keys already present in the database are skipped with an INFO-level log line (preserves current `add_data_from_dict` behaviour).
- Citations in `.tex` with no matching `.bib` record are skipped with a WARNING log; import continues.

### Exit code

`0` on full success or partial-but-recoverable success. Non-zero only on irrecoverable infrastructure failure.

## Subcommand: `migrate`

```text
pdbsearch migrate
```

Wraps `alembic upgrade head` against the configured database URL. Idempotent: running on an already-migrated database is a no-op with a single INFO line.

### Behaviour

- Creates the schema from scratch on a fresh database.
- Detects the legacy `bibtext_id` column (sic) on a database written by the old `add.py`/`get_data.py` modules and renames it to `bibtex_id` with a data-preserving migration.
- Reports `papers`, `authors_id`, `bib`, `authors_papers` row counts before and after for FR-011 verification (US4).

### Exit code

`0` on success. Non-zero on migration failure; the database is left in its pre-migration state (Alembic transaction wrapping).

## Error and logging contract

- All user-facing errors are plain-language, single-line, written to stderr.
- All technical detail (stack traces, SQL, parameter values) goes to the configured log sink. Default: stdout if no log file is configured, else the configured file. Verbosity controlled by `--log-level`.
- A failed operation never leaves the database in a half-written state — the transaction boundary is per top-level operation.
- `Ctrl+C` mid-dialog cleanly closes the active session and exits with code 130 (standard SIGINT).
