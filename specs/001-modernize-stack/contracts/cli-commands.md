# Contract: CLI Commands (`pdbsearch`)

The Typer application exposes the subcommands below. Invoked with **no subcommand**, `pdbsearch` drops
into the legacy four-option top-level menu, preserving the interactive UX of the legacy
`UserInteraction.interact`.

## Global options (root callback)

| Option | Source priority | Meaning |
|--------|-----------------|---------|
| `--database-url TEXT` | highest (CLI) | full SQLAlchemy URL, overrides everything |
| `--log-level TEXT` | CLI | DEBUG/INFO/WARNING/ERROR |
| `--config PATH` | CLI | Fernet-encrypted INI path (lowest config source) |
| `--key PATH` | CLI | Fernet key path for `--config` |

Resolution order (highest first): CLI flags > env `PDBSEARCH_*` > `.env` > Fernet-encrypted INI.

## Top-level menu (no subcommand)

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
Your choice:
```
1-indexed, mandatory quit. `q`/`4` exits with "Closing connection...". Invalid input re-prompts.
(`import` and `migrate` are deliberately absent — admin/scripted, subcommand-only.)

## `pdbsearch search`

Interactive. First asks search-by: `1) Search by author` / `2) Search by paper title`.

- **by title**: prompt title → one match prints title/authors/summary/bib; multiple distinct-title
  matches show a 1-indexed disambiguation list (out-of-range re-prompts); no match → plain "not found".
- **by author**: prompt author → resolve papers (disambiguate if several) → print the chosen paper;
  unknown author → plain "not found".

Pretty-print format (parity with legacy `pretty_print_results`):
```
title: <title>
authors: <A and B and C>
summary: <contents>
bib entry: <bibtex source>
```

## `pdbsearch add`

Prompts: author CSV → paper title → bibtex key → "enter bibtex via file? 1) Yes 2) No" → (filename | inline
bib) → summary. Persists bib + paper + author links. Duplicate bibtex key is rejected with a plain
message. Partial failure rolls back (bib row + any author links already created). Empty required input
re-prompts.

## `pdbsearch update`

Prompts: which table `1) papers 2) bib 3) authors 4) abort` → column (for papers: `1) title 2) contents
3) abort`; bib → bibtex; authors → author) → entry id → new value → **confirmation** summarising the
exact change, accepting `1`/`2` **or** `y`/`n`/`yes`/`no`. Abort at any step writes nothing. `_id`
columns refused.

## `pdbsearch delete`

Locate paper (by title), confirm, delete author links (orphan authors removed), delete paper, delete
bib. Confirmation accepts numeric and word forms.

## `pdbsearch import` (subcommand-only)

`pdbsearch import --tex <file.tex> --bib <file.bib>`. Parses the pair, inserts each cited entry that has
a matching `.bib` record, **commits per paper**. A citation key with no `.bib` match is skipped with a
logged warning (not a hard failure). Rerun skips already-imported keys (bib-key uniqueness).

## `pdbsearch migrate` (subcommand-only)

`pdbsearch migrate` runs Alembic `upgrade head` against the configured database. Converges either
historical schema (`bibtex_id` or `bibtext_id`) to canonical in a single action, idempotent, zero data
loss. Rerun is a no-op; mid-run failure leaves the pre-migration state intact.

## Error & UX contract (Principle III)

- Numbered menus 1-indexed with a mandatory abort/quit option.
- Destructive ops (update, delete) confirm with a change summary; confirmation accepts numeric and word forms.
- Failures log full technical detail via the stdlib logger and surface a short plain-language message to
  stdout. Raw exceptions / tracebacks / driver error objects MUST NOT reach stdout.
- All prompts route through `cli/prompts.py` (the sole `rich.prompt` importer).

## Interface-layer coverage (gate G1)

An end-to-end test using Typer's `CliRunner` exercises every subcommand (`search`, `add`, `update`,
`delete`, `import`, `migrate`) through the public entry point, satisfying the interface layer's ≥80%
coverage requirement.
