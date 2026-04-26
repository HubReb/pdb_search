# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Off-line paper-database searcher: a CLI that stores publication metadata (title, authors, summary, BibTeX) in a local PostgreSQL database and lets the user search/add/update entries. Personal-use tool — not a library or service.

## Commands

Dependencies are managed with Poetry (Python ^3.10):

```bash
poetry install                              # install deps
poetry run python paper_sorts/run.py \      # start the interactive CLI
    -c ${config} --section ${section} -k ${key_file}
poetry run pylint paper_sorts               # lint (pylint is a project dep)
poetry run python -m unittest discover tests  # run tests
poetry run python -m unittest tests.test_database_connector.DataBaseTest.test_search_by_author  # single test
```

Note: the README's `python run.py` is wrong — the entry point lives at `paper_sorts/run.py`. The default paths in argparse (`../../database.crypt`, `../../key`) assume the program is launched from inside `paper_sorts/`; otherwise pass `-c` and `-k` explicitly.

## Tests require a live database

`tests/test_database_connector.py` is an **integration test**, not a unit test. It opens a real PostgreSQL connection using `../../database.crypt` + `../../key` and asserts on specific seeded rows (e.g. `"Pino, J."`, `"Wang2021LargeScaleSA"`). Running the suite without that database + that data will fail with connection or assertion errors — this is expected, not a regression you introduced. `tests/test_user_interaction.py` is a placeholder (`assertEqual(True, False)`) and intentionally fails.

## Architecture

Three layers, top-to-bottom:

1. **`paper_sorts/user_interaction.py` — `UserInteraction`**: All CLI dialog (`input()`/`print()`). Drives a `DatabaseConnector` based on user choices. The only place stdin/stdout should be touched.
2. **`paper_sorts/database_connector.py` — `DatabaseConnector`**: High-level domain operations (`search_by_author`, `add_entry_to_db`, `update_entry`, `delete_paper_entry_from_database`, `rollback_database_addition`). Contains the SQL strings and the multi-step transactional logic (e.g. add paper → add bib → link authors → rollback on partial failure).
3. **`paper_sorts/psycopg_db.py` — `PsycopgDB`**: Thin wrapper around `psycopg2` exposing `store_in_db`, `fetch_from_db`, `delete_from_db`, `update_db_entry`. Per the docstring, this is the *only* module that imports psycopg2 — if the driver is ever swapped, this is the single point of change. Preserve that boundary: do not call psycopg2 from `DatabaseConnector`.

Supporting modules:

- `paper_sorts/config_reader.py` — decrypts a Fernet-encrypted INI file at startup to get DB credentials. Config layout is `[postgresql] dbname=… user=… password=…`.
- `paper_sorts/helpers.py` — pure functions used across layers: `create_logger` (every class builds its own file-backed logger via this), `cast` (safe int parse, returns -1), `get_user_input`/`get_user_choice`, `pretty_print_results`, plus latex/bibtex parsing for bulk import.

### Database schema

Four tables, created lazily by `DatabaseConnector.create_tables()`:

- `papers(id, title, contents, bibtex_id → bib.bibtex_id)`
- `bib(bibtex_id PK, bibtex)`
- `authors_id(id, author)`
- `authors_papers(id, author_id, paper_id)` — many-to-many link

A paper is identified internally by `papers.id`; the BibTeX key (`bibtex_id`) is the user-facing unique identifier and is the FK target from `papers` into `bib`.

### Legacy / duplicate modules — read before editing

`paper_sorts/add.py`, `paper_sorts/search.py`, and `paper_sorts/get_data.py` are **older, standalone, procedural versions** of functionality that has since been refactored into the `UserInteraction` / `DatabaseConnector` / `PsycopgDB` stack. They:

- import `psycopg` (v3) instead of `psycopg2` — different driver,
- use the column name `bibtext_id` (sic) instead of `bibtex_id` used by the OO stack,
- duplicate a `create_logger` and `read_config` independently of `helpers.py` / `ConfigReader`.

They are not wired into `run.py`. When fixing or extending behaviour, change the OO stack — do not edit the legacy scripts unless the task is explicitly to remove or reconcile them. If you find yourself porting logic *from* these files, double-check column names against the live schema in `DatabaseConnector.create_tables()`.

## SpecKit

`.specify/` contains SpecKit templates and `memory/constitution.md` (ratified 2026-04-26, current v1.1.0). The constitution defines four binding principles — Code Quality, Testing Standards, User Experience Consistency, Performance Requirements — and rules out a few things that come up naturally (mocking psycopg in DB tests; extending the legacy `add.py`/`search.py`/`get_data.py` modules; adding connection pools/caches/async drivers). The performance principle is framed as "no measurable regression vs. the current baseline" rather than absolute numbers — there's no benchmark behind any specific bound, so refactors are evaluated against measured baseline. Read the constitution before generating a plan or making non-trivial changes. The README's instruction "read the current plan" refers to a SpecKit plan that may or may not exist in the workspace at any given time; check `.specify/` before assuming one is available.
