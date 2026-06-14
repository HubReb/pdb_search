# Off-line Paper Database Searcher

A small, bare-bones CLI for storing publication metadata in a local PostgreSQL database
and querying it offline — for example when traveling by train.

Each entry stores: paper title, author(s), a one-sentence summary, and the full BibTeX record.
The database can be searched by author or title.

*Note*: This tool is for personal use only. If you encounter issues, consult the log output.

---

## Installation

Requires Python >= 3.11 and a local PostgreSQL installation (for `pg_ctl`).

```bash
git clone <repo-url>
cd paper-sorts
uv sync --all-extras
```

---

## Usage

### Interactive menu

```bash
uv run pdbsearch
```

Presents a four-option menu: Search / Add / Update / Quit.

### Subcommands

```bash
uv run pdbsearch search           # search by author or title
uv run pdbsearch add              # add a new entry
uv run pdbsearch update           # update an existing entry
uv run pdbsearch delete           # delete an entry
uv run pdbsearch import --tex literature.tex --bib refs.bib   # bulk import
uv run pdbsearch migrate          # apply database migrations (run once on first use)
```

---

## Configuration

Configuration is loaded in priority order (highest first):

1. CLI flags: `--database-url`, `--log-level`, `--config`, `--key`
2. Environment variables: `PDBSEARCH_DB_HOST`, `PDBSEARCH_DB_PORT`, `PDBSEARCH_DB_NAME`, `PDBSEARCH_DB_USER`, `PDBSEARCH_DB_PASSWORD`
3. `.env` file in the working directory
4. Fernet-encrypted INI file (same format as before):

```bash
uv run pdbsearch --config path/to/database.crypt --key path/to/key
```

The encrypted config file uses the `[postgresql]` section with `dbname`, `user`, `password`, `host`, `port`.

---

## Search

The search interface prompts for a method and then a query:

```
Search interface
Please choose a method:
1) Search by author
2) Search by paper title
3) (Q)uit
```

Search by title: exact match. If multiple papers share a title, a numbered disambiguation
menu is shown.

Search by author: exact match in `"Last, First"` format. All papers by that author are
listed for selection.

---

## Add an Entry

The add dialog walks through each field:

```
Author(s) — comma-separated 'Last, First' list: Vaswani, Ashish, Shazeer, Noam
Paper title: Attention Is All You Need
BibTeX key: Vaswani2017AttentionIA
How do you want to provide the BibTeX entry?
1) Load BibTeX from file
2) Enter BibTeX inline
3) (A)bort
Summary of the paper: Proposes the Transformer architecture.
```

---

## Update an Entry

Search to locate the paper, then select the field to update:

```
Which information do you want to update?
1) papers (title / summary)
2) bib (BibTeX entry)
3) authors (author name)
4) (A)bort
```

A confirmation step summarises the change before it is applied:

```
Proceed?
1) (Y)es
2) (N)o
```

---

## Migration (First-Time Setup or Upgrading)

If you have an existing personal database, run the migration command once to apply
the canonical schema:

```bash
uv run pdbsearch migrate --database-url "postgresql://user:pass@localhost/mydb"
```

This is idempotent. Both legacy schema variants (`bibtex_id` and `bibtext_id`) are
handled automatically.

---

## Development

```bash
uv run ruff check src tests          # lint
uv run ruff format --check src tests # format check
uv run mypy src                      # type check
uv run pytest                        # run the full test suite
uv run pytest -m "not benchmark"     # skip benchmark tests
```

The test suite creates its own ephemeral PostgreSQL instance. No personal database
or configuration files are required.
