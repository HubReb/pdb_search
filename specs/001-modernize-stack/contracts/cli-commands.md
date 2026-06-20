# CLI Contract: pdbsearch

**Feature**: 001-modernize-stack  
**Date**: 2026-06-20

---

## Entry Point

```
pdbsearch [OPTIONS] COMMAND [ARGS]...
```

When invoked **with no subcommand**, `pdbsearch` drops into the four-option
interactive top-level menu (US2 behavior). `migrate` and `import` are
subcommand-only (admin/scripted operations, not in the interactive menu).

---

## Global Options

| Flag              | Env var                 | Default | Description                          |
|-------------------|-------------------------|---------|--------------------------------------|
| `--database-url`  | `PDBSEARCH_DATABASE_URL`| —       | SQLAlchemy URL (overrides all others)|
| `--log-level`     | `PDBSEARCH_LOG_LEVEL`   | `INFO`  | Logging level (DEBUG/INFO/WARNING…)  |
| `--config`        | —                       | —       | Path to Fernet-encrypted INI file    |
| `--key`           | —                       | —       | Path to Fernet key file              |

---

## Subcommands

### `pdbsearch search`

Interactive search flow. Prompts for method, then term.

```
pdbsearch search [--author AUTHOR] [--title TITLE]
```

- No flags → interactive prompt: `1) Search by author  2) Search by title  3) (Q)uit`
- `--author AUTHOR` → search by author directly
- `--title TITLE` → search by title directly
- Multiple results → disambiguation prompt (numbered list, re-prompts on invalid)
- Output: title, authors, summary, BibTeX entry (rich-formatted)

### `pdbsearch add`

Interactive add flow.

```
pdbsearch add [--bib-file FILE]
```

- Prompts: author(s) (comma-separated), title, bibtex key, summary
- `--bib-file FILE` → reads BibTeX from file; otherwise prompts inline
- Confirmation: shows summary of what will be added, confirms y/n
- On conflict (duplicate bibtex_id): error message, no change

### `pdbsearch update`

Interactive update flow.

```
pdbsearch update
```

- Prompts: which table (papers/bib/authors), which field, identifier, new value
- Confirmation: `You wish to change '<field>' of '<id>' to '<value>'. Proceed? 1) Yes  2) No`
- Accepts `1`/`y`/`yes` and `2`/`n`/`no`

### `pdbsearch delete`

Interactive delete flow.

```
pdbsearch delete
```

- Prompts: search for paper (by title), confirm deletion
- Confirmation: shows paper details, asks `1) Yes  2) No`

### `pdbsearch migrate`

Admin-only. Runs Alembic migrations. Not in interactive menu.

```
pdbsearch migrate [--revision TARGET]
```

- Default: `head` (upgrade to latest)
- Idempotent: safe to rerun

### `pdbsearch import`

Admin-only. Bulk import from LaTeX + BibTeX files. Not in interactive menu.

```
pdbsearch import --tex FILE --bib FILE
```

- Required: `--tex` and `--bib`
- Per-paper commit (partial failure leaves DB consistent)
- Skips entries with no matching BibTeX record (logged warning)

---

## Interactive Top-Level Menu (no subcommand)

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) Delete an entry
5) (Q)uit
Your choice:
```

- Loop continues until user selects 5 or types `q`/`Q`
- Invalid input: re-prompts with "Please choose a valid option"

---

## Prompt Grammar Rules (Principle III)

- All prompts route through `paper_sorts.cli.prompts` — no bare `input()` or
  `typer.prompt` elsewhere in `src/paper_sorts/`
- Menus are 1-indexed, always include an explicit quit/abort option
- Empty input re-prompts (preserves `get_user_input` behavior)
- Confirmation accepts both `1`/`2` (numeric) and `y`/`n`/`yes`/`no` (word)
- Failure messages: plain-language to stdout; technical detail to log file only

---

## Error Messages

| Situation                     | Stdout message                           |
|-------------------------------|------------------------------------------|
| Paper not found               | `Paper not found in database.`           |
| Author not found              | `Author not found in database.`          |
| Duplicate bibtex_id           | `Entry already exists (key: <id>).`      |
| Encrypted config, key missing | `Key file not found: <path>. Check --key option.` |
| DB connection failure         | `Could not connect to the database. Check logs.` |
| Update: column not allowed    | `Cannot update field '<field>'. Check logs.` |
