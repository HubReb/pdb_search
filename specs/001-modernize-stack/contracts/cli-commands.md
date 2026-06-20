# CLI Contract: pdbsearch Commands

**Feature Branch**: `001-modernize-stack`
**Date**: 2026-06-20

---

## Entry Point

```
pdbsearch [OPTIONS] COMMAND [ARGS]...
```

Invoked with **no subcommand** → drops into the interactive four-option top-level menu (search / add / update / delete).

Global options (available on every subcommand):
- `--database-url TEXT` — overrides `PDBSEARCH_DATABASE_URL` env var
- `--log-level [DEBUG|INFO|WARNING|ERROR]` — overrides `PDBSEARCH_LOG_LEVEL`
- `--config PATH` — path to Fernet-encrypted INI config file
- `--key PATH` — path to Fernet decryption key file

---

## Subcommands

### `pdbsearch search`

Interactive search flow (prompts for search type, then search term).

```
pdbsearch search [--by {title|author}] [--query TEXT]
```

- If `--by` and `--query` are omitted → prompts interactively.
- Multiple matches on same title → disambiguation list (1-indexed, with quit option).
- Output format:
  ```
  title: <title>
  authors: <Author1, First1 and Author2, First2>
  summary: <contents>
  bib entry: <bibtex string>
  ```

---

### `pdbsearch add`

Add a new paper entry interactively or from a `.bib` file.

```
pdbsearch add [--bib-file PATH]
```

Interactive prompts (in order):
1. Author(s) — comma-separated "Last, First" list
2. Paper title
3. BibTeX key
4. BibTeX entry source: `1) From file` / `2) Enter manually`
5. Filename (if from file) or inline BibTeX string
6. Summary (one sentence)

Confirmation before write: `Proceed? 1) (Y)es  2) (N)o`

---

### `pdbsearch update`

Update an existing paper field.

```
pdbsearch update [--id ID] [--table {papers|bib|authors}] [--column COLUMN]
```

Interactive flow (if flags omitted):
1. Select table: `1) papers  2) bib  3) authors  4) abort`
2. Select column (depends on table):
   - papers: `1) title  2) contents  3) abort`
   - bib: bibtex (only editable column)
   - authors: author (only editable column)
3. Enter identifier (paper `id`, author `id`, or `bibtex_id`)
4. Enter new value
5. Confirmation: `Proceed? 1) (Y)es  2) (N)o`

---

### `pdbsearch delete`

Delete a paper and all its associated data (authors, authorship links, bib entry).

```
pdbsearch delete [--id ID]
```

Interactive:
1. Prompt for paper title (or `--id` to skip to confirmation)
2. If multiple matches → disambiguation
3. Show paper summary
4. Confirmation: `Proceed with deletion? 1) (Y)es  2) (N)o`

---

### `pdbsearch import`

Bulk import from a `.tex` + `.bib` pair.

```
pdbsearch import --tex PATH --bib PATH
```

- Skips entries where `bibtex_id` already exists (idempotent).
- Logs skipped entries at INFO level.
- Commits per-paper (partial failure leaves prior entries intact).

---

### `pdbsearch migrate`

Apply Alembic schema migrations.

```
pdbsearch migrate [--target REVISION]
```

- Default target: `head` (latest revision).
- Idempotent: re-running on an up-to-date schema is a no-op.
- Handles both legacy schema variants (detects `bibtext_id` typo column).

---

## UX Invariants (from constitution Principle III)

1. All menus are **1-indexed**.
2. Every menu has an explicit **abort/quit** option.
3. Empty input on a required prompt → **re-prompt** until non-empty.
4. Destructive operations (update, delete) present a **confirmation step** summarising the exact change.
5. Confirmation accepts both `1`/`2` (numeric) and `y`/`n`/`yes`/`no` (word) forms.
6. **All user-facing prompts** route through `paper_sorts.cli.prompts`. Bare `input()`, `Prompt.ask`, or `typer.prompt` anywhere outside `cli/prompts.py` is a violation.
7. Failure paths: logged at ERROR via stdlib logger; user sees a short plain-language message. No raw exceptions or stack traces on stdout.
