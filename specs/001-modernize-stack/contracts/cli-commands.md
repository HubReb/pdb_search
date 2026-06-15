# CLI Command & Prompt Contract

Surface: `pdbsearch` (Typer app, console-script entry point). CLI-only (FR-017). All
user-facing prompts route through `src/paper_sorts/cli/prompts.py` (Principle III). Menus are
1-indexed and always carry an explicit abort/quit option.

## Global

```
pdbsearch [--database-url URL] [--log-level LEVEL]
          [--config PATH --key PATH] [SUBCOMMAND ...]
```

- No subcommand → drop into the interactive four-option top-level menu (legacy UX).
- Configuration priority: CLI flags > `PDBSEARCH_*` env > `.env` > Fernet INI.
- `--help` lists subcommands.

## Top-level interactive menu (no subcommand)

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
Your choice:
```

- 1-indexed; option 4 / `q` quits. Invalid input re-prompts. (`migrate` and `import` are
  subcommand-only — deliberately absent from this menu: admin/scripted operations.)

## `pdbsearch search`

Interactive sub-menu:

```
Search interface
1) Search by author
2) Search by paper title
3) abort
Your choice:
```

- **By title**: prompt for title. One match → display the record. Multiple matches → 1-indexed
  disambiguation list; out-of-range selection re-prompts. No match → plain "not found" message.
- **By author**: prompt for `Last, First`. List the author's papers (1-indexed); select one;
  display the record.
- **Display format** (matches legacy pretty-print):
  ```
  title: <title>
  authors: <A and B and C>
  summary: <contents>
  bib entry: <bibtex>
  ```

## `pdbsearch add`

Sequential prompts (each empty input re-prompts until non-empty):

```
Author(s) (comma-separated, "Last, First"):
Paper title:
BibTeX key:
Provide the BibTeX entry from a file?
1) Yes
2) No
Your choice:
  → if Yes: Filename:    (read file)
  → if No:  BibTeX entry:
Summary:
```

Persists the paper, authors, and BibTeX entry; retrievable by author and title afterward.

## `pdbsearch update`

```
Which information do you want to update?
1) papers
2) bib
3) authors
4) abort
Your choice:
```

- `papers` → sub-menu `1) title  2) contents  3) abort`.
- `bib` → only `bibtex` is editable (key immutable).
- `authors` → only the author name is editable.
- Prompt for the entry identifier and the new value, then a **confirmation** summarising the
  exact change:
  ```
  You wish to change '<column>' of entry '<id>' to '<value>'. Proceed?
  1) (Y)es
  2) (N)o
  ```
  Confirmation accepts numeric (`1`/`2`) **and** word (`y`/`n`/`yes`/`no`) forms. `n`/`2` →
  no change written.

## `pdbsearch delete`

Identify the paper, present a confirmation summarising what will be removed (numeric + word
forms), then delete the paper, its BibTeX entry, and its authorship links (dropping authors left
with no papers — legacy behaviour).

## `pdbsearch migrate` (subcommand-only)

```
pdbsearch migrate
```

Upgrades a personal database in either historical schema (`bibtex_id` or legacy `bibtext_id`) to
the canonical schema in one action. Idempotent — rerun is a no-op once converged. Zero data loss
(row counts for papers/authors/authorships/bib match before and after).

## `pdbsearch import` (subcommand-only)

```
pdbsearch import --tex PATH --bib PATH
```

Bulk-import every cited entry that has a matching `.bib` record. Entries whose citation key has
no `.bib` match are skipped with a logged warning (not a hard failure). Commits **per paper** —
a partial failure leaves earlier inserts intact; rerun skips already-imported keys (BibTeX-key
uniqueness).

## Error contract (all commands)

- Failures log full technical detail via the configured stdlib logger.
- Only a short, plain-language message reaches stdout — no raw exceptions, stack traces, or
  driver error objects (Principle III; FR-003).

## Delta vs. legacy

- Bespoke `argparse` + manual dialog loop → Typer subcommands + interactive fallback menu.
- Raw `input()`/`get_user_input` → `cli/prompts.py` helpers (empty-input re-prompt preserved).
- Per-class log files → stdlib `dictConfig` (RichHandler to stdout; optional FileHandler).
