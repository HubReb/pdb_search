# Off-line Paper Database searcher

A small, bare-bones application to add publication metadata to a PostgreSQL
database for later querying when no online connection is available to reach one
of the freely available online resources — e.g. while traveling by train.

The database can be searched by either author or publication title. If the entry
has previously been added, a search returns:

* paper title
* author(s)
* a small summary
* the BibTeX entry

*Note:* this application was created for personal use and its construction
reflects that. If you hit a problem in your setup, consult the logs.

## Installation

Dependencies are managed with [uv](https://docs.astral.sh/uv/) (Python ≥ 3.11):

```bash
uv sync --all-extras
```

## Interaction

Run with no subcommand for the interactive top-level menu, or call a subcommand
directly:

```bash
uv run pdbsearch                 # interactive menu (Search / Add / Update / Quit)
uv run pdbsearch search
uv run pdbsearch add             # add a paper (inline or --bib-file)
uv run pdbsearch update
uv run pdbsearch delete
uv run pdbsearch import --tex literature_overview.tex --bib bib.bib
uv run pdbsearch migrate         # converge an existing database onto the schema
uv run pdbsearch --help
```

## Search

The top-level menu:

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
```

Choose 1 to load the search dialog:

```
Search interface
Please choose a method:
1) Search by author
2) Search by paper title
3) abort
```

### Search by title

Enter the title; if a paper of that name exists, its information is shown. If no
paper is found, you are told. If several papers share that title, you are asked
to pick one from a numbered list.

### Search by author

Enter the author's name as `${last name}, ${first name}`. You are presented with
the matching papers and, if more than one, asked to select one.

## Add an entry

The program walks you through the fields step by step, including whether to read
the BibTeX entry from a file or enter it inline:

```
Author(s), please provide a , separated list: Lee, Ann, Pino, J.
Paper title: Fancy new paper
bibtex key: NewKey2026
Do you want to enter the bibtex entry via a separate file?
1) Yes
2) No
3) abort
Enter filename: bibfile.bib
summary of the paper: ...
```

You can also skip the prompt and pass the file directly:
`uv run pdbsearch add --bib-file bibfile.bib`.

## Update an entry

The program walks you through the update, then asks you to confirm the exact
change before applying it:

```
Which information do you want to update?
1) papers
2) bib
3) authors
4) abort
Your choice: 1
Which information do you want to update?
1) title
2) contents
3) abort
Your choice: 1
Which entry do you want to update? Please enter the respective id/key/name: 42
Enter the new information: the new title
the new title
You wish to change 'title' of entry '42' to 'the new title'.
Proceed? 1) (Y)es  2) (N)o
```

Use [search](#search) to find a paper's id. The confirmation accepts both
numeric (`1`/`2`) and word (`y`/`n`/`yes`/`no`) answers.

## Configuration

Settings are resolved from four sources, highest priority first:

1. **CLI flags** — `--database-url`, `--log-level`, `--log-file`.
2. **Environment** — `PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`, …
3. **`.env`** file in the working directory.
4. **Fernet-encrypted INI** — `--config ${config} --key ${key}`.

The encrypted INI keeps the legacy credentials workflow; its `[postgresql]`
section is of the form:

```
[postgresql]
host=your_host
port=5432
dbname=your_dbname
user=your_dbuser
password=your_dbuser_password
```

A lost or wrong key produces a clear error rather than a stack trace.

## Development

```bash
uv run ruff check .          # lint
uv run ruff format --check . # format check
uv run mypy src              # type-check
uv run pytest                # tests against an ephemeral PostgreSQL
```

The test suite spins up an ephemeral PostgreSQL via `pytest-postgresql` from the
host's `pg_ctl` — no personal database, no `database.crypt`, and no `key` file
required.
