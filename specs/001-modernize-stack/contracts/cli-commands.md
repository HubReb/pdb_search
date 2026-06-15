# Contract: CLI Commands & Prompt Grammar

The CLI is the entire product surface (Principle III). This contract fixes the
command set, the interactive menus, and the prompt grammar that the
implementation must satisfy. Behaviour is preserved from the legacy
`UserInteraction` dialog (FR-002, SC-002).

## Entry point

`pdbsearch` (console script → `paper_sorts.cli.app:app`). A `typer.Typer` app.

### Subcommands

| Command | Maps to legacy | In top menu? |
|---------|----------------|--------------|
| `pdbsearch search` | `UserInteraction.search` | yes (option 1) |
| `pdbsearch add` | `UserInteraction.add` | yes (option 2) |
| `pdbsearch update` | `UserInteraction.update` | yes (option 3) |
| `pdbsearch delete` | `delete_paper_entry_from_database` flow | — (reachable via search/CLI) |
| `pdbsearch import` | `get_data.py` bulk import | no (admin/scripted) |
| `pdbsearch migrate` | new (FR-011) | no (admin/scripted) |

`pdbsearch --help` lists all subcommands. Global options: `--database-url`,
`--log-level`, `--config <path>`, `--key <path>`, `--log-file`.

## No-subcommand interactive menu

Running `pdbsearch` with no subcommand drops into the legacy top-level menu via
an `invoke_without_command=True` callback:

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
Your choice:
```

- 1-indexed; option 4 is the explicit quit (Principle III). Accepts `q`/`4`.
- Invalid input re-prompts (loop preserved from legacy `interact`).

## Search flow

Sub-menu (legacy `search`):

```
Search interface
Please choose a method:
1) Search by author
2) Search by paper title
3) abort
```

- **By author**: prompt for author name → `services.search_by_author` →
  if multiple results, `prompts.pick_from` disambiguation → `display_paper`.
- **By title**: prompt for title → `services.search_by_title` → if multiple
  papers share the title, `prompts.pick_from` (out-of-range re-prompts) →
  `display_paper`.
- Not found → plain-language message ("Paper/Author not found."), no trace.

`display_paper` format (legacy `pretty_print_results`):

```
title: <title>
authors: <a and b and c>
summary: <summary>
bib entry: <bibtex source>
```

## Add flow

Prompts in order (all via `prompts.ask_nonempty`, empty re-prompts):

1. `Author(s), please provide a , separated list:`
2. `Paper title:`
3. `bibtex key:`
4. Bib source: choice menu `1) Yes 2) No` — enter via file or inline.
   - Yes → `Enter filename:` → read file.
   - No → `bib entry:` inline.
5. `summary of the paper:`

Then `services.add_paper(PaperCreate(...))`. Success/failure → plain message.

## Update flow

Table choice (legacy `update`), 1-indexed with abort:

```
Which information do you want to update?
1) papers
2) bib
3) authors
4) abort
```

- `papers` → column sub-menu `1) title 2) contents 3) abort`.
- `bib` → only `bibtex` is updatable (identifier cannot change).
- `authors` → only the author name is updatable.
- Then prompt for the entry identifier and the new value.
- **Confirmation** (Principle III, dual-form): summarise the exact change, then
  `1) (Y)es 2) (N)o` accepting `1`/`2`/`y`/`n`/`yes`/`no`. `n`/abort → no write.

`services.update_field(table, column, identifier, value)` enforces that IDs are
immutable and that `authors_papers` is not updatable (raises before persistence).

## Delete flow

Search/select the target, present a confirmation summarising the paper to be
removed (dual-form confirm), then `services.delete_paper`. Authors with no
remaining papers are removed (legacy cleanup). Abort → no write.

## `import` (bulk, subcommand-only)

`pdbsearch import --tex <file.tex> --bib <file.bib>`:
`import_service.extract_papers_from_tex_bib` yields one `PaperCreate` per cited
entry that has a matching `.bib` record. Each paper is **committed individually**
(Principle IV / FR-005 AC-3): a key with no `.bib` match is skipped with a
logged warning; a mid-import failure leaves earlier papers persisted; re-running
skips already-present BibTeX keys (idempotent).

## `migrate` (subcommand-only)

`pdbsearch migrate` runs the Alembic upgrade to `head`, converging either
historical schema (`bibtex_id` or the legacy `bibtext_id` typo) onto canonical.
Idempotent: re-running a converged DB is a no-op. Zero data loss (SC-004).

## Prompt grammar (Principle III, binding)

1. **1-indexed** numbered menus in display.
2. Every menu has an explicit **abort/quit** option.
3. **Empty input** on a required prompt re-prompts until non-empty
   (`prompts.ask_nonempty`).
4. **Out-of-range** menu/disambiguation input re-prompts.
5. **Destructive confirmations** accept both numeric (`1`/`2`) and word
   (`y`/`n`/`yes`/`no`) forms.
6. **Errors**: short plain-language message to stdout; full technical detail
   (exceptions, driver errors) only to the configured logger — never to stdout.
7. All prompts route through `cli/prompts.py`; bare `input()` / `rich.prompt` /
   `typer.prompt` elsewhere under `src/paper_sorts/` is a violation.
