# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` (worktree branch `rep/001-OH3`) | **Date**: 2026-06-19 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-modernize-stack/spec.md`

## Summary

Reverse-engineer the legacy offline paper-database CLI (flat-layout `paper_sorts/`, hand-written
psycopg2 SQL strings, bespoke `argparse` + `input()` dialog loop, `unittest`, Poetry, encrypted-INI
config) and rebuild it on a mainstream stack with **no observable change in user-facing behaviour**.
The modern stack: a `src/`-layout package `src/paper_sorts/`, **SQLAlchemy 2.x** (typed ORM) over the
**psycopg v3** driver isolated to a `db/` persistence layer, **Alembic** migrations replacing the
runtime `create_tables()`, a **Typer** CLI replacing the argparse + manual menu loop, **pydantic-settings**
config with a four-source priority chain (CLI > env > `.env` > Fernet-encrypted INI), **uv**/PEP-621
packaging, and a **pytest** suite that provisions an ephemeral PostgreSQL per session via
**pytest-postgresql** — no developer-local database. The legacy flat modules are deleted once their
behaviour is covered (FR-012). The constitution's four principles are honoured; the conflicting
v1.x references named in FR-016 were already amended in the in-worktree constitution (v1.3.0-b2-hardened),
whose three mechanical gates (per-layer coverage, executing baseline benchmark, doc-currency token scan)
are first-class acceptance items here.

## Technical Context

**Language/Version**: Python ≥ 3.11 (FR-015; constitution Stack & Constraints). Dev host runs 3.14; floor pinned at 3.11 in `pyproject.toml`.
**Primary Dependencies**: SQLAlchemy 2.x, Alembic, psycopg[binary] v3, Typer, rich, pydantic-settings v2, cryptography (Fernet), pybtex, pylatexenc.
**Storage**: PostgreSQL only — the existing four tables (`papers`, `bib`, `authors_id`, `authors_papers`), schema preserved verbatim (no new NOT NULL outside PKs, no FKs on `authors_papers`, no new indexes).
**Testing**: pytest + pytest-postgresql (ephemeral PG off host `pg_ctl`), pytest-cov for per-layer coverage, Typer `CliRunner` for the interface layer.
**Target Platform**: Linux CLI, single-user, offline.
**Project Type**: Single project, layered CLI (presentation → service → persistence → config).
**Performance Goals**: No measurable regression vs. legacy baseline on interactive ops (search-by-title, search-by-author, add, update, delete) on a personal-library-sized dataset; a baseline benchmark harness must exist and execute (Principle IV gate G2).
**Constraints**: Offline, single-user, CLI-only (FR-017). No connection pools beyond SQLAlchemy default, no async drivers, no caches (Principle IV). Driver/ORM imports isolated to `db/` (Principle I). Plaintext creds/keys never committed or logged.
**Scale/Scope**: Personal corpus (tens to low-hundreds of papers). Target ≥30% reduction in project-authored LOC under the package vs. legacy ~2 000 (SC-005).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Constitution in force: **v1.3.0-b2-hardened** (this worktree). Four principles + three mechanical
merge-blocking gates. FR-016's amendments are already reflected in the constitution text (ruff not
pylint, pytest not unittest, prompts routed through `cli/prompts`, layer-level driver isolation), so
no `/speckit-constitution` run is required in this re-run — the conflicting references the spec calls
out no longer exist in the live document. (Recorded for SC-007.)

| Principle | Touched? | Compliance approach | Gate status |
|-----------|----------|---------------------|-------------|
| I. Code Quality (NON-NEGOTIABLE) | Yes | `ruff check` + `ruff format --check` clean; full type hints + docstrings; **only `db/` imports `sqlalchemy`/`psycopg`**; services depend on DTOs. **G3 doc-currency**: an automated test asserts `README.md`/`CLAUDE.md` contain none of `Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB`. | PASS (by construction) |
| II. Testing Standards | Yes | Real ephemeral PG via pytest-postgresql; **no mocking** of session/repos/driver in persistence tests; seed data co-located in `tests/fixtures/seed_papers.py`; pure helpers (prompts, config) unit-tested for empty/malformed/success. **G1 per-layer coverage**: each of `db/`, `services/`, `cli/`, `config.py` independently ≥80%; interface layer covered end-to-end via Typer `CliRunner` over every subcommand. No always-failing placeholders. | PASS |
| III. UX Consistency | Yes | All prompts route through `cli/prompts.py` (sole `rich.prompt` importer); 1-indexed menus with mandatory abort/quit; destructive ops confirm with summary accepting numeric **and** word forms; failures log + plain-language stdout, never raw tracebacks. | PASS |
| IV. Performance | Yes | Parameterised queries + joins over the existing four-table schema; no new tables/indexes/denormalisation; context-managed sessions (`with_session`) closed deterministically; bulk import commits per-paper. **G2 baseline benchmark**: `tests/benchmarks/` harness exists, executes (not permanently skipped), records a baseline for all five interactive ops. | PASS |

No violations requiring waivers → Complexity Tracking left empty.

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output (cli-commands.md, db-repositories.md)
├── checklists/
│   └── requirements.md  # pre-existing
└── tasks.md             # Phase 2 (/speckit-tasks)
```

### Source Code (repository root)

```text
src/paper_sorts/
├── __init__.py
├── config.py                 # pydantic-settings Settings, four-source priority chain
├── logging_config.py         # single dictConfig (RichHandler + optional FileHandler)
├── cli/
│   ├── __init__.py
│   ├── app.py                # Typer app; wires subcommands; no-subcommand → top-level menu
│   ├── prompts.py            # ONLY module allowed to import rich.prompt
│   ├── search.py             # `search` subcommand + interactive search flow
│   ├── add.py                # `add` subcommand
│   ├── update.py             # `update` subcommand
│   ├── delete.py             # `delete` subcommand
│   ├── importer.py           # `import` subcommand (subcommand-only)
│   └── migrate.py            # `migrate` subcommand (subcommand-only)
├── services/
│   ├── __init__.py
│   ├── paper_service.py      # search_by_title/author, add_paper, update_field, delete_paper
│   └── import_service.py     # extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]
└── db/
    ├── __init__.py
    ├── models.py             # 4 ORM models (papers, bib, authors_id, authors_papers)
    ├── session.py            # engine factory + with_session(...) ctx manager
    └── repositories.py       # PaperRepository/AuthorRepository/BibRepository + pydantic DTOs

migrations/                   # Alembic
├── env.py
├── script.py.mako
└── versions/
    ├── 001_*.py              # verbatim port of legacy DDL (canonical bibtex_id schema)
    └── 002_*.py              # legacy bibtext_id → bibtex_id converger (idempotent)

tests/
├── conftest.py               # postgresql_proc, ephemeral_db_url, seeded session fixtures
├── fixtures/
│   ├── seed_papers.py        # SEED_PAPERS canonical dataset (co-located assertions)
│   ├── sample.bib
│   └── literature_overview.tex
├── test_db_repositories.py
├── test_services.py
├── test_cli.py               # Typer CliRunner over every subcommand (interface coverage)
├── test_config.py
├── test_prompts.py
├── test_migrations.py        # both schema variants → canonical, idempotency
├── test_doc_currency.py      # G3 forbidden-token scan of README.md / CLAUDE.md
└── benchmarks/
    ├── __init__.py
    ├── conftest.py
    ├── bench_baseline.py      # G2 records baseline for the 5 interactive ops (executes)
    └── baseline.json

pyproject.toml                # PEP 621, uv, hatchling, ruff/mypy/pytest config
alembic.ini
docs/architecture.md          # FR-001 reverse-engineered legacy architecture doc
README.md                     # migrated, doc-currency clean
```

**Structure Decision**: Single-project `src/`-layout, four layers as named above. Database-driver and
SQLAlchemy imports are physically confined to `src/paper_sorts/db/`; services consume pydantic DTOs only;
`cli/` consumes services only; `cli/prompts.py` is the sole `rich.prompt` importer. This mirrors the
constitution's Principle I isolation rule and makes per-layer coverage (G1) measurable by directory.

## Complexity Tracking

> No constitution violations. No waivers. Table intentionally empty.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| — | — | — |
