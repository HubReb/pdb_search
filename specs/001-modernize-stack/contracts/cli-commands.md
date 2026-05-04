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
1) (S)earch the database
2) (A)dd an entry
3) (U)pdate an entry
4) (Q)uit
Your choice:
```

Grammar (constitution Principle III, v1.4.0):
- 1-indexed numeric.
- Mandatory abort/quit at the bottom.
- Empty input re-prompts.
- **Single-letter aliases are accepted on every numbered menu in addition to the digit.** Aliases are derived deterministically (parenthesised single-alpha char of the option label wins; otherwise the first alphabetic character of the label, lower-cased), case-insensitive, and unique within a menu. Callers disambiguate collisions by passing an explicit alias for at least one of the colliding options, or by opting an option out (`alias=None`, digit-only on that row).
- Selecting an option dispatches to the corresponding subcommand's interactive flow, then returns to the menu when that flow completes.

### Why only four options

The original `UserInteraction.interact()` menu has exactly these four entries (search / add / update / quit). Modernization preserves the **menu surface** verbatim, even though the **subcommand surface** is richer:

- **Delete** is intentionally absent from the menu. It is reachable as a Typer subcommand (`pdbsearch delete`) but is not a one-keystroke menu choice. The original code likewise does not expose delete in the top-level menu — destructive operations get friction by design. Promoting it would be a UX-surface expansion, which spec FR-002 ("preserve existing CLI feature set") does not authorise.
- **Import** is intentionally absent from the menu. Bulk import from `.tex` + `.bib` is, in the current code, a separate scripted invocation (`python paper_sorts/get_data.py ...`) — not an interactive convenience. Modernization preserves that as `pdbsearch import <tex> <bib>` (a deliberate scripted call), not a menu entry.
- **Migrate** is intentionally absent from the menu — admin/setup operation, subcommand-only (see tasks.md T040 for the same reasoning applied separately).

Adding any of these to the menu would be a separate UX change requiring its own spec.

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
1) Search by (a)uthor
2) Search by (t)itle
Your choice: 2
Please enter the paper title:
> Direct speech-to-speech translation with discrete units
```

The axis labels carry their alias inside parens (`(a)uthor`, `(t)itle`) per the v1.4.0 grammar — the leading-`S` char would otherwise collide between the two options.

### Behaviour

- If multiple papers match the title or author, present a 1-indexed disambiguation list. Title rows on that list opt out of letter-aliasing (every row would otherwise alias to `t`, a collision); the trailing `abort` row keeps its `a` alias.
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

```text
pdbsearch update [--id N]
```

The two-step menu (table → field) is preserved verbatim. The row-identification step diverges by table per spec 002-ux-polish:

* On the **`papers`** table the legacy raw-id prompt is replaced by the interactive search-then-pick dialog (axis → query → optional disambiguation, factored as `cli/search.search_and_pick`). With `--id <N>`, the search step is skipped and the row is fetched directly by id; table, field, and new-value collection still run interactively. No `--table` / `--field` / `--value` flags are provided in this feature.
* On the **`bib`** and **`authors`** tables the legacy `Please enter the respective id` prompt is preserved verbatim — search-by-bibtex-key and search-by-author-row are not part of this feature.

```text
Which information do you want to update?
1) (P)apers
2) (B)ib
3) (A)uthors
4) (q)uit
Your choice: p

Which information do you want to update?
1) (T)itle
2) (C)ontents
3) (q)uit
Your choice: t

Search interface
Please choose a method:
1) Search by (a)uthor
2) Search by (t)itle
Your choice: t

Please enter the paper title: speech

Following papers found:
1) title: Direct speech-to-speech translation with discrete units
2) abort
Choose paper to extract: 1

Enter the new information: <new title>

Please verify: You wish to change 'title' of the paper 'Direct speech-to-speech translation with discrete units' (id 42) to '<new title>'.
 Proceed?
1) (Y)es
2) (N)o
Your choice:
```

The trailing slot on the table-pick and field-pick menus is `(q)uit` (alias `q`), not `abort` — `authors` and `abort` would otherwise both auto-derive `a`, triggering the v1.4.0 collision-rejection rule.

### Behaviour

- The papers-table confirmation summary echoes both the picked paper's title and its id (`'... of the paper '<title>' (id <N>) ...'`) so the user verifies what they recognised in the disambig list while the id remains visible for log/audit traceability.
- The bib/authors confirmation summary keeps the legacy `'... of the entry '<id>' ...'` wording — the user-typed identifier is the only canonical handle on those paths.
- A search query matching no rows shows the existing `Author was not found in db_connector.` / `Paper was not found in db_connector.` message and exits without writing.
- `--id N` against a missing paper prints `Error: no paper with id N.` and exits.
- Updating the BibTeX `bibtex_id` is forbidden (preserves current behaviour: "the bibtex identifier cannot be changed"); only `bibtex` (the source string) can be updated.
- Confirmation accepts `1`/`y`/`yes` (proceed) or `2`/`n`/`no` (abort). Anything else logs an error and aborts.

## Subcommand: `delete`

```text
pdbsearch delete [--id N]
```

Without `--id`, the interactive form runs the same search-then-pick dialog as `update`:

```text
Search interface
Please choose a method:
1) Search by (a)uthor
2) Search by (t)itle
Your choice: t
Please enter the paper title: speech

Following papers found:
1) title: Direct speech-to-speech translation with discrete units
2) abort
Choose paper to extract: 1

Please verify: You wish to DELETE paper id 42 ('Direct speech-to-speech translation with discrete units'). This cannot be undone.
1) (Y)es
2) (N)o
Your choice:
```

With `--id <N>`, the search step is bypassed; the existing non-interactive path runs unchanged. The confirmation is still shown.

### Behaviour

- Cascades to `authors_papers` rows for that paper. If any author has no remaining papers afterward, the author row is also deleted (preserves the cascade-on-delete behaviour in `delete_author_of_list`; note that the `__delete_author_with_no_papers` method exists in the current code but is not on the standard delete path).
- BibEntry is deleted iff no other paper references it.
- All in one transaction.
- A search query matching no rows shows the existing not-found message and exits without deleting.
- `--id N` against a missing paper prints `Error: no paper with id N.` and exits.

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
