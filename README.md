# Off-line Paper Database searcher

Stores paper metadata (title, authors, summary, BibTeX) in a local Postgres DB so you can search it offline (e.g. on a train). Search is by author or title.

Personal use only. Check the logs if something breaks.

## Installation

Packaged with [uv](https://docs.astral.sh/uv/):

```bash
uv sync --all-extras
```

Installs runtime + dev tooling (pytest, ruff, mypy) and registers the `pdbsearch` console script. Full developer setup: `specs/001-modernize-stack/quickstart.md`.

## CLI

```bash
uv run pdbsearch                # interactive menu
uv run pdbsearch <subcommand>   # direct invocation
```

Subcommands: `search`, `add`, `update`, `delete`, `import`, `migrate`. The last three are not in the interactive menu (destructive or scripted).

### Letter aliases

Every numbered menu accepts a letter alongside the digit; aliases are shown in parens on the label.

```
What do you want to do?
1) (S)earch the database
2) (A)dd an entry
3) (U)pdate an entry
4) (Q)uit
Your choice: s
```

The disambiguation list (`Choose paper to extract:`) is digit-only because every title would alias to `t`. The trailing `abort` row keeps its `a`.

## search

```
Search interface
Please choose a method:
1) Search by (a)uthor
2) Search by (t)itle
Your choice: t
Please enter the paper title: <query>
```

Author queries take `${last}, ${first}`. Multiple matches drop into the disambiguation list. Non-interactive: `pdbsearch search --by {author,title} --query <q>`.

## add

```
Please enter the necessary information
Author(s), please provide a , separated list: ${authors}
Paper title: Fancy new paper
bibtex key: new_key
Do you want to enter the bibtex entry via a separate file?
1) (Y)es
2) (N)o
Your choice: 1
Enter filename: bibfile.bib
summary of the paper_information: [...]
```

Flags: `--bib-file <path>` and `--summary <text>` skip the matching prompts.

## update

Two-step table/field picker, then a row-identification step. For `papers`, the row is picked via the same search dialog as `pdbsearch search`. No need to know the id.

```
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
2) title: Speech recognition baselines for low-resource languages
3) abort
Choose paper to extract: 1

Enter the new information: Direct speech-to-speech translation, revised

Please verify: You wish to change 'title' of the paper 'Direct speech-to-speech translation with discrete units' (id 42) to 'Direct speech-to-speech translation, revised'.
 Proceed?
1) (Y)es
2) (N)o
Your choice: y
```

Confirmation shows title (to confirm the pick) and id (so it's in your shell history to grep later).

`bib` and `authors` keep the legacy raw-id prompt (`Please enter the respective id:`).

`pdbsearch update --id <N>` skips the search step on the papers table. No `--table` / `--field` / `--value` flags; those remain interactive.

## delete

Same flow as `update`:

```
$ pdbsearch delete
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
Your choice: y
Deleted paper id 42.
```

`pdbsearch delete --id <N>` skips the search step.

Cascade, in one transaction: `authors_papers` rows are dropped, then any author left with no remaining papers, then the bib row if no other paper still references it.

## import

```bash
pdbsearch import <paper.tex> <refs.bib>
```

Inserts cited papers from the pair, one transaction per paper. Re-runs are idempotent: keys already in the database are logged and skipped. End-of-run summary reports `inserted` / `skipped` / `warned` counts.

## Config

Connection sources, highest precedence first:

1. **CLI flags** — `--database-url`, `--log-level`, etc.
2. **Env vars** — `PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`, `PDBSEARCH_LOG_FILE`.
3. **`.env`** at the project root (same keys).
4. **Fernet-encrypted INI** (`--config <path> --key <path>`):

   ```ini
   [postgresql]
   dbname=your_dbname
   user=your_dbuser
   password=your_dbuser_password
   ```

   The key file holds a single Fernet key.

Full setup including `pdbsearch migrate`: `specs/001-modernize-stack/quickstart.md`.
