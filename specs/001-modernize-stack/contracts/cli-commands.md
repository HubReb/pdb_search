# CLI Command Contracts: pdbsearch

The product surface is a Typer app exposed as the console script `pdbsearch`. Invoked **bare**, it prints a welcome line, connects, and drops into the four-option interactive menu (legacy `UserInteraction.interact` parity). Invoked with a **subcommand**, it runs that operation. `migrate` and `import` are subcommand-only admin/scripted operations and are deliberately absent from the four-option menu.

Global options (resolved by `config.py`, priority CLI > env > `.env` > encrypted INI):

- `--database-url TEXT` — full `postgresql+psycopg://…` URL (highest priority).
- `--config PATH` `--key PATH` — Fernet-encrypted INI + key file (lowest priority source).
- `--log-level [DEBUG|INFO|WARNING|ERROR]` — logging level (default INFO).

## Top-level interactive menu (bare invocation)

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
Your choice:
```

- 1-indexed; explicit quit (`4`/`q`). Invalid input re-prompts (Principle III).
- Selecting Search opens the author/title sub-menu.

## `pdbsearch search`

- Sub-menu: `1) Search by author  2) Search by paper title  3) abort`.
- **By title**: prompt for title → if 0 matches, plain "not found" message; if 1 match, display it; if >1 matches sharing the title, present a 1-indexed numbered disambiguation list with an abort option; out-of-range selection re-prompts.
- **By author**: prompt for author → display matching paper(s); same disambiguation rule when multiple.
- Output (parity with `pretty_print_results`): `title`, `authors` (` and `-joined), `summary`, `bib entry`. Rendered via Rich.
- Failure: plain-language stdout message; technical detail to log.

## `pdbsearch add`

- Prompts: authors (`;`-separated list of `Last, First` names — a `;` delimiter keeps each `Last, First` intact, an improvement over the legacy `, `-split quirk), paper title, bibtex key, then "enter bibtex via file?" (`1) Yes 2) No`) → filename or inline bib entry, then summary.
- Empty input on any required prompt re-prompts (legacy `get_user_input`).
- Persists Paper + Bib + Author links in one transaction. Duplicate bibtex key → plain error, no partial write.
- Retrievable afterward by both author and title (US2 AS3).

## `pdbsearch update`

- Menu: `1) papers  2) bib  3) authors  4) abort`.
  - papers → `1) title  2) contents  3) abort`.
  - bib → only `bibtex` (the identifier is immutable).
  - authors → only the author name.
- Prompt for the target identifier and the new value.
- **Confirmation** summarising the exact change; accepts `1`/`2` and `y`/`n`/`yes`/`no`. `n` → no write (US2 AS4).
- Attempting to update `authors_papers` or any `*_id` column → rejected with a plain message.

## `pdbsearch delete`

- Identify the paper, summarise it, confirm (numeric or word). On confirm, delete the paper, its bib entry, and authorship links; drop any author left with no papers.
- Non-existent paper → plain error (`ValueError` surfaced as a message, not a traceback).

## `pdbsearch import`

- Args: `--tex PATH --bib PATH`.
- Walks the `.tex` for cited entries, matches against the `.bib`, inserts each matched paper. **Per-paper commit** — a mid-run failure preserves already-imported papers (US5 AS3).
- A citation key with no `.bib` match is skipped with a logged warning, not a hard failure (US5 AS2).
- Rerun is safe: existing bibtex keys are skipped, not duplicated (idempotent via bibtex-key uniqueness).

## `pdbsearch migrate`

- Runs Alembic to head against the configured database.
- Converges a legacy `bibtext_id` (sic) schema onto canonical `bibtex_id` with zero row loss; idempotent (US4 / FR-011). Rerun after a successful migration is a no-op.

## Error & exit conventions (all subcommands)

- Raw exceptions, stack traces, and driver error objects MUST NOT reach stdout — they go to the log; the user sees a short plain-language message (Principle III).
- Ctrl+C mid-dialog exits without leaving a partial write (transactions roll back).

## Delta vs. legacy CLI

| Legacy | Modern |
|--------|--------|
| `python paper_sorts/run.py -c … --section … -k …` | `pdbsearch` (bare → menu) or `pdbsearch <subcommand>` |
| argparse global flags only | Typer subcommands + global config options |
| `create_tables()` on first add | `pdbsearch migrate` / Alembic |
| per-class `*.log` files | single dictConfig (Rich stdout + optional file) |
