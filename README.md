# Off-line Paper Database searcher

A small, bare-bones application to add publication metadata to a PostgreSQL
database for later querying when no online connection is available to reach one
of the freely available online resources — for example, traveling by train.

The database can be searched by either author or publication title. If the entry
has previously been added, a search returns:

* paper title
* author
* small summary
* bibtex entry

*Note:* This application was created for personal use only and its construction
reflects that. If you hit any problem in your setup, consult the logs.

## Installation

Dependencies are managed with [uv](https://docs.astral.sh/uv/) (Python ≥ 3.11):

```bash
uv sync --all-extras
```

## Interaction

Run the CLI with no subcommand to drop into the interactive menu:

```bash
uv run pdbsearch
```

Or run a single operation directly:

```bash
uv run pdbsearch search          # search by author or title
uv run pdbsearch add             # add an entry
uv run pdbsearch update          # update an entry
uv run pdbsearch delete          # delete an entry
uv run pdbsearch import --tex lit.tex --bib refs.bib   # bulk import
uv run pdbsearch migrate         # apply migrations / converge a legacy schema
uv run pdbsearch --help          # list subcommands
```

### Configuration

Settings are resolved, highest priority first, from:

1. CLI flags — `--database-url`, `--log-level`, `--config`, `--key`,
2. environment variables prefixed `PDBSEARCH_` (e.g. `PDBSEARCH_DATABASE_URL`),
3. a `.env` file,
4. a Fernet-encrypted INI file (`--config database.crypt --key key`).

The encrypted config should be used if it contains sensitive information such as
a password; keep the key file in a relatively safe location. A missing key file
produces a clear error rather than a stack trace.

## Search

The interactive menu:

```
Welcome! Connected to the database.
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

Enter the title; if a paper of that name exists, its information is shown. If
several papers share the title, you are presented with a numbered list and asked
to choose one.

### Search by author

Enter the author's name in `Last, First` form. You are presented with the matching
paper(s) and, when there is more than one, asked to select one.

## Add an entry

The program walks you through the fields. You are asked whether to read the bib
entry from a file or enter it by hand. Authors are entered as a `;`-separated list
of `Last, First` names, so a single `Last, First` author stays intact:

```
Author(s) — ';'-separated list of 'Last, First' names: Doe, Jane; Roe, R.
Paper title: Fancy new paper
bibtex key: newkey
Enter the bibtex entry via a separate file?
1) Yes
2) No
3) abort
bib entry / filename: ...
summary of the paper: ...
```

## Update an entry

The program walks you through the steps to update a single field. Updates are
confirmed before they are applied:

```
Which information do you want to update?
1) papers
2) bib
3) authors
4) abort
```

For a paper you then choose `title` or `contents`. You identify the row by its id
(papers), bibtex key (bib), or author name (authors); use the search functionality
to find a paper id. The change is summarised and confirmed (accepting `1`/`2` or
`y`/`n`) before it is written.

## Config

Your encrypted INI should be of the form:

```
[postgresql]
host=localhost
port=5432
dbname=your_dbname
user=your_dbuser
password=your_dbuser_password
```

## Tests

```bash
uv run ruff check .
uv run mypy src
uv run pytest
```

The test suite spins up an ephemeral PostgreSQL via `pytest-postgresql` from the
host's `pg_ctl`, so it needs no personal database, encrypted config, or key file.

For a description of the legacy (pre-modernization) stack, see
[`docs/architecture.md`](docs/architecture.md).
