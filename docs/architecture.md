# Architecture (reverse-engineered legacy stack)

This document captures the application **as it was before the T026 modernization**
(the legacy flat-layout `paper_sorts/` package). It is the acceptance reference
for the rebuild (spec FR-001 / US1): the modernized system must do everything
described here. For the modern stack, see the repository `README.md` and
`CLAUDE.md`.

## Purpose

An offline, single-user CLI for a personal database of academic papers. It stores
each paper's title, summary, authors, and full BibTeX source in a local
PostgreSQL database and lets the owner search, add, update, delete, and bulk-import
entries. It is a personal tool, used "while traveling by train" — not a library
or service.

## User journeys

1. **Search** — by author name or by paper title. On multiple title matches the
   user disambiguates from a numbered list. The result prints title, authors,
   summary, and BibTeX entry.
2. **Add** — the user types authors, title, BibTeX key, and summary, and either
   pastes a BibTeX entry inline or points at a `.bib` file.
3. **Update** — change a paper's title or contents, a bib entry's BibTeX string,
   or an author's name, behind a confirmation step.
4. **Delete** — remove a paper and its dependent rows.
5. **Bulk import** — load a whole `.tex` literature overview plus its `.bib`
   file in one shot (`get_data.py`).

## Data model (four tables)

- `papers(id, title, contents, bibtex_id → bib.bibtex_id)` — a paper; `id` is the
  internal key, `bibtex_id` the user-facing unique key and FK into `bib`.
- `bib(bibtex_id PK, bibtex UNIQUE)` — the full BibTeX source per key.
- `authors_id(id, author)` — an author, name in `"Last, First"` form.
- `authors_papers(id, author_id, paper_id)` — the many-to-many link, with **no
  foreign keys** by design.

Relationships: a paper has exactly one bib entry and many authors; an author has
many papers. Two authors with identical `"Last, First"` strings are treated as
one (a documented limitation).

## Control flow (three layers)

```
run.py → UserInteraction (CLI dialog, input()/print())
       → DatabaseConnector (domain operations + hand-written SQL strings)
       → PsycopgDB (thin psycopg2 wrapper: store/fetch/delete/update)
       → PostgreSQL
```

- The presentation layer (`UserInteraction`) owns all stdin/stdout.
- The domain layer (`DatabaseConnector`) holds the SQL and the multi-step
  transactional logic (e.g. add paper → add bib → link authors, with a manual
  rollback path on partial failure).
- The driver layer (`PsycopgDB`) is the only place that imports the database
  driver; swapping drivers was meant to be a single-file change.

**Partial-add failure**: `add_entry_to_db` inserts the bib row, then the paper,
then links each author; if an author link fails it calls
`rollback_database_addition`, deleting the bib row and any links already made, to
avoid leaving the database half-written.

## Configuration

Database credentials live in a Fernet-encrypted INI file (`database.crypt`)
decrypted at startup with a key file (`key`). `ConfigReader` reads the
`[postgresql]` section into a dict of connection parameters. CLI flags
(`-c/--config`, `--section`, `-k/--key`) override the defaults
(`../../database.crypt`, `../../key`), which assume the program is launched from
inside `paper_sorts/`.

## Install / run (legacy)

Dependencies were managed with Poetry (Python ^3.10). The CLI was launched via
`python paper_sorts/run.py -c <config> --section <section> -k <key>`. Tests were
bare `unittest` and depended on a live developer-local database seeded with
specific rows (`Pino, J.`, `Wang2021LargeScaleSA`).

## Known limitations and quirks

- **Schema typo split**: the OO stack used `bibtex_id`; the older procedural
  modules (`add.py`, `search.py`, `get_data.py`) used `bibtext_id` (sic) and the
  `psycopg` v3 driver instead of `psycopg2`. A personal DB could be in either
  schema.
- **Duplicate-author collapse**: identical `"Last, First"` names are one author.
- **Developer-local tests**: the suite required a personal database and a
  hand-curated dataset; a placeholder test (`assertEqual(True, False)`) always
  failed.
- **Launch-directory sensitivity**: the default config paths only resolve when
  run from inside `paper_sorts/`.
- **Author entry split**: the interactive add split the author input on `", "`,
  which mis-split a single `"Last, First"` author into two.
