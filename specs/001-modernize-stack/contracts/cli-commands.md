# CLI Command Contract

Entry point: `pdbsearch` (Typer app in `src/paper_sorts/cli/app.py`).
`pdbsearch --help` lists subcommands. Global options apply before the
subcommand and feed the config chain (highest priority source).

## Global options

| Option | Effect |
|--------|--------|
| `--database-url TEXT` | SQLAlchemy URL `postgresql+psycopg://...`; highest-priority config source |
| `--log-level TEXT` | logging level (DEBUG/INFO/...) |
| `--config PATH` | Fernet-encrypted INI config file |
| `--key PATH` | Fernet key file for `--config` |

Config priority (highest first): CLI flags → `PDBSEARCH_*` env → `.env` →
encrypted INI. A missing key for a provided `--config` yields a plain-language
error, not a traceback.

## No-subcommand invocation

`pdbsearch` with no subcommand drops into the legacy interactive top-level menu:

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
Your choice:
```

1-indexed, explicit quit (Principle III). `migrate` and `import` are
**not** in this menu — admin/scripted subcommands only.

## Subcommands

### `pdbsearch search`
Interactive: choose `1) Search by author` / `2) Search by paper title`
(1-indexed; invalid → re-prompt). Then:
- **by title**: 1 match → print directly; >1 match sharing a title →
  1-indexed disambiguation list, out-of-range re-prompts.
- **by author**: prints the author's papers.
Output is the legacy "pretty print": `title`, `authors` (` and `-joined),
`summary`, `bib entry`. Not-found → plain message, logged.

### `pdbsearch add`
Prompts (each re-prompts on empty input): authors (comma-separated `Last, First`
list), title, BibTeX key, then `1) Yes / 2) No` whether to read the BibTeX entry
from a file (`Enter filename:`) or inline, then summary. Persists atomically;
retrievable by both author and title afterward.

### `pdbsearch update`
Menu `1) papers / 2) bib / 3) authors / 4) abort`. For `papers`: `1) title /
2) contents / 3) abort`. For `bib`: only `bibtex` editable (key immutable). For
`authors`: only `author` editable. Prompts for the entry id and new value, then
a confirmation summarising the exact change accepting `1`/`2`/`y`/`n`/`yes`/`no`.
Confirm `n` writes nothing. IDs are never editable.

### `pdbsearch delete`
Identifies the paper, summarises it, confirms (numeric+word), then removes
authorship links, orphaned authors, paper, and bib rows.

### `pdbsearch import`
`pdbsearch import --tex FILE --bib FILE`. Extracts every cited entry that has a
matching `.bib` record; a citation key with no `.bib` match is skipped with a
logged warning (not a hard failure). Commits **per paper** — a partial failure
preserves earlier papers and is re-runnable (BibTeX-key uniqueness skips dupes).
Admin/scripted; absent from the interactive menu.

### `pdbsearch migrate`
`pdbsearch migrate`. Upgrades a personal DB in either historical schema
(canonical `bibtex_id` or legacy `bibtext_id` typo) to the canonical schema in
one action. Idempotent: rerun completes cleanly or no-ops; never half-migrated.
Row counts (papers, authors, authorships, bib) unchanged. Admin/scripted;
absent from the interactive menu.

## Error & logging contract (Principle III, FR-006 #6)

Every failure path: full technical detail to the configured logger; a short,
plain-language message to stdout. Raw exceptions / stack traces / driver error
objects MUST NOT reach stdout.
