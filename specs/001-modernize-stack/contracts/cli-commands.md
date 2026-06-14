# CLI Command Contract

**Feature**: 001-modernize-stack  
**Date**: 2026-06-15

---

## Entry Point

```
pdbsearch [OPTIONS] [COMMAND]
```

When invoked with no subcommand, drops into a 4-option interactive menu (see §Interactive Mode). When invoked with a subcommand, executes it directly and exits.

Global options (available on all subcommands):

| Option | Env Var | Default | Description |
|--------|---------|---------|-------------|
| `--database-url` | `PDBSEARCH_DATABASE_URL` | from config | PostgreSQL DSN |
| `--config` | — | `../../database.crypt` | Fernet-encrypted INI file |
| `--key` | — | `../../key` | Decryption key file |
| `--log-level` | `PDBSEARCH_LOG_LEVEL` | `INFO` | Logging level |

---

## Subcommands

### `pdbsearch search`

Interactive search; prompts for search type and query term.

```
pdbsearch search [--by {title,author}] [--query QUERY]
```

Behavior:
1. If `--by` not given: prompt `1) Search by author / 2) Search by title / 3) Quit`.
2. If `--query` not given: prompt for the search term.
3. If zero results: print "No results found for '<query>'."
4. If exactly one result: display paper details.
5. If multiple results: display numbered list; prompt "Select (1-N) or 0 to abort:"; re-prompt on invalid input.

### `pdbsearch add`

Add a new paper entry.

```
pdbsearch add [--from-bib FILE]
```

Behavior:
1. Prompt for author(s), title, bibtex key, summary.
2. If `--from-bib` given: read bibtex from that file. Otherwise prompt for bibtex string.
3. Confirmation step: show summary; "Confirm add? 1) Yes / 2) No".
4. On success: print "Added '<title>'."
5. On failure: print plain-language error; log technical detail.

### `pdbsearch update`

Update a field of an existing paper.

```
pdbsearch update [--id BIBTEX_ID]
```

Behavior:
1. If `--id` not given: search flow to select paper.
2. Prompt: "Which field? 1) title / 2) contents / 3) bibtex / 4) author / 5) abort".
3. Prompt for new value.
4. Confirmation step: "You will change '<field>' of '<id>' from '<old>' to '<new>'. Proceed? 1) Yes / 2) No".
5. On success: print "Updated."
6. Confirmation accepts: `1`, `y`, `yes` (case-insensitive) for Yes; `2`, `n`, `no` for No.

### `pdbsearch delete`

Delete a paper and its authorship links.

```
pdbsearch delete [--id BIBTEX_ID]
```

Behavior:
1. If `--id` not given: search flow to select paper.
2. Confirmation step: "You will delete '<title>' (<id>). Proceed? 1) Yes / 2) No".
3. Orphan authors (no remaining papers) are also deleted.
4. On success: print "Deleted '<title>'."

### `pdbsearch import`

Bulk-import from a .tex + .bib pair.

```
pdbsearch import TEX_FILE BIB_FILE
```

Behavior:
1. Parse TEX_FILE for citations; parse BIB_FILE for BibTeX entries.
2. For each cited key found in bib: insert paper (per-paper transaction).
3. For each cited key NOT in bib: log warning, skip, continue.
4. On partial failure: already-inserted entries are preserved (per-paper commit).
5. Print summary: "Imported N papers, skipped M."

### `pdbsearch migrate`

Apply Alembic migrations to bring schema up to current head.

```
pdbsearch migrate [--target REVISION]
```

Behavior:
1. Default: `alembic upgrade head`.
2. If `--target` given: upgrade/downgrade to that revision.
3. Idempotent: safe to run on already-migrated databases.
4. On success: print "Migration complete. Current revision: <rev>."
5. On error: print plain-language error; log technical detail; exit non-zero.

---

## Interactive Mode (no subcommand)

When `pdbsearch` is invoked with no arguments, a top-level menu loop runs:

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
Your choice:
```

Menu rules (constitution Principle III):
- 1-indexed.
- Always includes a quit/abort option.
- On invalid input: re-prompt with the same menu.
- `q` / `4` both exit the loop.

The four options correspond to the `search`, `add`, `update`, and `delete` subcommands. `migrate` and `import` are subcommand-only (admin operations; not in the interactive menu).

---

## UX Grammar (Principle III constraints)

- All prompts route through `paper_sorts.cli.prompts` — no bare `input()` elsewhere.
- Empty input on required prompts: re-prompt until non-empty.
- Confirmation accepts numeric (`1`/`2`) AND word (`y`/`n`/`yes`/`no`), case-insensitive.
- Destructive operations (update, delete) MUST show a summary of the change before applying.
- Failure: plain-language message to stdout + technical detail in logger. No raw exceptions on stdout.
