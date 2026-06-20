# Off-line Paper Database searcher

A small, bare-bones application to add publication metadata to a PostgreSQL database for later querying
when no online connection is available to reach one of the freely available online resources — e.g.
while traveling by train.

The database can be searched by either author or publication title. If the entry has previously been
added to the database, a search returns:

* paper title
* author(s)
* a short summary
* the BibTeX entry

*Note:* This application was created for personal use and its construction reflects that. If you run
into problems in your setup, consult the logs.

## Installation

Dependencies are managed with [uv](https://docs.astral.sh/uv/) (Python >= 3.11):

```bash
uv sync --all-extras
```

## Running

Start the interactive CLI:

```bash
uv run pdbsearch
```

`pdbsearch --help` lists the subcommands (`search`, `add`, `update`, `delete`, `import`, `migrate`).
Each can also be run directly, e.g. `uv run pdbsearch search`.

### Configuration

The database connection is resolved from four sources, highest priority first:

1. CLI flags: `--database-url`, `--log-level`,
2. environment variables prefixed `PDBSEARCH_` (e.g. `PDBSEARCH_DATABASE_URL`),
3. a `.env` file,
4. a Fernet-encrypted INI file: `--config <path> --key <path>`.

```bash
uv run pdbsearch --database-url postgresql+psycopg://user:pass@localhost/paper_sorts
uv run pdbsearch --config ../../database.crypt --key ../../key
```

The encrypted-INI form is the legacy credential workflow, preserved as one supported source. A
lost/missing key file produces a clear, actionable error rather than a stack trace.

## Search

With no subcommand, `pdbsearch` shows the top-level menu:

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) abort
```

Choose 1 to load the search dialog:

```
Search interface — please choose a method:
1) Search by author
2) Search by paper title
3) abort
```

### Search by title

Enter the title; if a paper of that name exists, its information is shown. If no paper is found, you are
told so. If several papers share the title, you are presented with the list and asked to choose one.

### Search by author

Enter the author's name in the format `Last, First`. You are presented with a list of papers that
author has (co-)authored and asked to select one.

## Add an entry

The program walks you through adding an entry step by step. You are asked whether to read the BibTeX
entry from a file or enter it by hand.

```
Author(s), please provide a comma-separated list: Doe, Jane
Paper title: Fancy new paper
bibtex key: Doe2026Fancy
Enter the bibtex entry via a separate file?
1) Yes
2) No
3) abort
Your choice: 1
Enter filename: bibfile.bib
summary of the paper: ...
```

## Update an entry

The program walks you through updating a single entry, then asks you to confirm the exact change before
applying it (the confirmation accepts both `1`/`2` and `y`/`n`/`yes`/`no`):

```
Which information do you want to update?
1) papers
2) bib
3) authors
4) abort
Your choice: 1
Which column do you want to update?
1) title
2) contents
3) abort
Your choice: 1
Which entry? Please enter its id: 42
Enter the new information: the new title
You wish to change 'title' of entry '42' to 'the new title'.
Proceed? 1) (Y)es  2) (N)o
```

You can find an entry's id with the [search](#search) functionality.

## Delete an entry

`pdbsearch delete` locates a paper by title, confirms, and removes the paper, its BibTeX entry, and its
author links (dropping authors left with no papers).

## Bulk import

`pdbsearch import --tex literature_overview.tex --bib bib.bib` imports every cited entry that has a
matching BibTeX record, committing per paper so a partial run leaves a consistent database.

## Migrating an existing database

`pdbsearch migrate` upgrades a personal database to the modern schema in a single, idempotent action,
converging either historical schema variant with zero data loss.

## Config file format

If you use the encrypted-INI source, the decrypted configuration should be of the form:

```
[postgresql]
host=localhost
port=5432
dbname=your_dbname
user=your_dbuser
password=your_dbuser_password
```

Keep the encrypted file and its key out of version control.

## Development

```bash
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy src
uv run pytest
```

The test suite provisions an ephemeral PostgreSQL via `pytest-postgresql` from the host's `pg_ctl`; no
personal database or credentials are required on a fresh checkout.
