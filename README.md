# Off-line Paper Database searcher

A small, bare-bones application to add publication metadata to a postgresql database for later querying in case no online connection
is available to enable querying one of the freely available online resources, e.g. traveling by train.

The database can be searched by either author or publication title.
If the respective entry has previously been added to the database, a search returns:
* paper title
* author
* small summary
* bibtex entry


*Note:* This application was created for only personal usage and its construction reflects that. If you encounter
any problems in your setup, consult the logs.

## Installation

The project is packaged with [uv](https://docs.astral.sh/uv/). With uv installed, run:

```bash
uv sync --all-extras
```

This installs the runtime dependencies plus the dev tooling (pytest, ruff, mypy). The console script `pdbsearch` is registered automatically. See `specs/001-modernize-stack/quickstart.md` for the full developer setup.

## Interaction

Start the interactive CLI:

```bash
uv run pdbsearch
```

This drops into the top-level menu (search / add / update / quit). The non-interactive subcommands are also available — `pdbsearch search`, `pdbsearch add`, `pdbsearch update`, `pdbsearch delete`, `pdbsearch import`, `pdbsearch migrate`. Run `pdbsearch --help` for the full list.

`delete`, `import`, and `migrate` are deliberately absent from the interactive menu (destructive / admin / scripted operations); reach them as subcommands.

### Letter aliases

Every numbered menu accepts a single-letter shortcut in addition to the digit. The alias is rendered with parens on the option label so it is discoverable at a glance — type `s` (or `S`) for search, `1` works the same. Both are valid for the lifetime of the menu.

There is one menu where the title rows are deliberately digit-only because every title would alias to `t`: the disambiguation list (`Choose paper to extract:`). On that menu, type a digit for a row, or `a` for `abort`.

## Search

The top-level dialog is:
```
What do you want to do?
1) (S)earch the database
2) (A)dd an entry
3) (U)pdate an entry
4) (Q)uit
Your choice: s
```
Picking search loads the search dialog:
```
Search interface
Please choose a method:
1) Search by (a)uthor
2) Search by (t)itle
Your choice: t
```
### Search by title

Enter the title (or a substring). If a paper matches, the relevant information is printed.

```
Please enter the paper title:
```
If no paper is found, you are informed. If several papers match, you are presented with a numbered list and asked to pick one.

### Search by author

Enter the author's name. You are then presented with a list of papers that author has (co-)authored and asked
to select one. The name should have the format ```${last name}, ${first name}```.
```
Please enter the author's name:
```

## Add an entry

The program takes you through the steps to add an entry to the database step by step. You are asked
whether you want to provide a file to read the bib entry from or enter the data by hand.

```
Please enter the necessary information
Author(s), please provide a , separated list: ${list_of_authors}
Paper title: Fancy new paper
bibtex key: new_key
Do you want to enter the bibtex entry via a separate file?
1) (Y)es
2) (N)o
Your choice: 1
Enter filename: bibfile.bib
summary of the paper_information: [...]
```

The non-interactive form accepts `--bib-file <path>` and `--summary <text>` to skip the corresponding prompts.

## Update an entry

The program walks you through a table picker, a field picker, and a row-identification step.
For the `papers` table the row is identified via the same search dialog used by `pdbsearch search` —
you no longer have to know the paper's id.

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
```

You are asked to review and verify the change before it is applied. The confirmation echoes both the
title and the id — the title closes the recognition loop, the id stays visible for log/audit traceability:

```
Please verify: You wish to change 'title' of the paper 'Direct speech-to-speech translation with discrete units' (id 42) to 'Direct speech-to-speech translation, revised'.
 Proceed?
1) (Y)es
2) (N)o
Your choice: y
```

For the `bib` and `authors` tables, the legacy raw-id prompt is preserved — those paths still ask `Please enter the respective id:`.

### `--id` for scripting

If you already know the paper's id (e.g. from a script or a prior search), pass `--id <N>` and the
search step is skipped:

```bash
pdbsearch update --id 42
```

`--id` only skips the search step on the papers table. Table, field, and new-value collection remain
interactive — there are no `--table`, `--field`, or `--value` flags.

## Delete an entry

`pdbsearch delete` uses the same search-then-pick affordance:

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

The non-interactive form `pdbsearch delete --id 42` skips the search step.

Cascade behaviour: the `authors_papers` link rows are dropped, any author left with no remaining papers
is removed, and the bib row is deleted only if no other paper still references it. The whole sequence
runs in a single transaction.

## Bulk import

`pdbsearch import` reads a `.tex` + `.bib` pair and inserts the cited papers one at a time. Run
`pdbsearch import --help` for the flag list.

# Config

The database connection can come from any of four sources, in priority order (highest first):

1. **CLI flags** — `--database-url`, `--log-level`, etc.
2. **Environment variables** — `PDBSEARCH_DATABASE_URL`, optionally `PDBSEARCH_LOG_LEVEL`, `PDBSEARCH_LOG_FILE`.
3. **`.env` file** at the project root (same keys as the env vars).
4. **Fernet-encrypted INI** for sensitive deployments — pass `--config <path>` and `--key <path>`. The INI is the same shape as before:

   ```ini
   [postgresql]
   dbname=your_dbname
   user=your_dbuser
   password=your_dbuser_password
   ```

   The key file holds a single Fernet key, generated once and kept in a relatively safe location.

See `specs/001-modernize-stack/quickstart.md` for full setup, including how to seed the database with `pdbsearch migrate`.
