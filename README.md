# Offline Paper Database Searcher

A small, personal CLI for storing publication metadata in a local PostgreSQL
database and searching it offline — handy when no online resource is reachable
(say, on a train). For each paper it keeps the title, authors, a short summary,
and the full BibTeX entry, searchable by author or by title.

> Personal-use tool, not a library or a service. If something goes wrong, the
> plain-language message tells you what happened and the full detail is in the
> logs.

## Install

Dependencies are managed with [uv](https://docs.astral.sh/uv/) (Python ≥ 3.11):

```bash
uv sync --all-extras        # runtime + dev dependencies
```

## Run

```bash
uv run pdbsearch            # interactive top-level menu (Search / Add / Update / Quit)
uv run pdbsearch --help     # list subcommands
uv run pdbsearch search     # search by author or title
uv run pdbsearch add        # add an entry (inline or from a .bib file)
uv run pdbsearch update     # update title / contents / bibtex / author
uv run pdbsearch delete     # delete an entry
uv run pdbsearch import --tex lit.tex --bib refs.bib   # bulk import
uv run pdbsearch migrate    # upgrade an existing personal DB to the new schema
```

Invoked with no subcommand, `pdbsearch` drops into the interactive menu:

```
What do you want to do?
1) Search the database
2) Add an entry
3) Update an entry
4) (Q)uit
```

`import` and `migrate` are admin/scripted subcommands and are deliberately not in
that menu.

## Configure

Configuration sources, in priority order (highest first):

1. CLI flags: `--database-url`, `--log-level`, `--config`, `--key`
2. Environment: `PDBSEARCH_*` (e.g. `PDBSEARCH_DATABASE_URL`)
3. `.env` file
4. Fernet-encrypted INI file: `--config <path> --key <path>`

```bash
uv run pdbsearch --database-url postgresql+psycopg://user:pw@localhost/papers search
# or
export PDBSEARCH_DATABASE_URL=postgresql+psycopg://user:pw@localhost/papers
uv run pdbsearch search
```

The encrypted-INI workflow is preserved as the lowest-priority source, so an
existing encrypted config keeps working. Its section looks like:

```ini
[postgresql]
dbname=your_dbname
user=your_dbuser
password=your_dbuser_password
```

A missing key file for a provided `--config` produces a clear, actionable error
rather than a traceback.

## Search

Choose *by author* or *by paper title*. A title search that matches a single
paper prints it directly; if several papers share a title you are asked to pick
one from a numbered list. Each result shows the title, the authors (joined with
` and `), the summary, and the BibTeX entry. A search with no match prints a
plain message.

## Add

You are prompted for the authors, title, BibTeX key, then whether to read the
BibTeX entry from a file or type it inline, and finally a summary. Empty input is
re-prompted. The paper, its BibTeX record, and its author links are written
together; if any step fails, nothing is left half-written.

## Update

Pick a table (`papers` / `bib` / `authors`), then the field to change (IDs are
never editable, and the BibTeX key is immutable), then the row identifier and the
new value. The change is summarised and you confirm it (accepting `1`/`2` or
`y`/`n`/`yes`/`no`); declining writes nothing.

## Delete

Identify a paper by title, review the summary, and confirm. Deleting removes the
authorship links, any authors left with no papers, and the paper and BibTeX rows.

## Migrate an existing database

If you already have a personal database from an earlier version (either the
`bibtex_id` schema or the older `bibtext_id` typo schema), run:

```bash
uv run pdbsearch migrate
```

It is idempotent and preserves every row — paper, author, authorship, and BibTeX
counts are unchanged.

## Develop

```bash
uv run ruff check src tests        # lint
uv run ruff format --check src     # format check
uv run mypy src                    # type-check (strict on src/)
uv run pytest                      # suite (ephemeral PostgreSQL via pytest-postgresql)
```

The tests need no personal database or credentials: `pytest-postgresql` spins up
an ephemeral PostgreSQL from the host `pg_ctl` and the suite seeds it from
`tests/fixtures/seed_papers.py`.

A reverse-engineered description of the pre-modernization design lives in
[`docs/architecture.md`](docs/architecture.md).
