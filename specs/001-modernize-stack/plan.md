# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-15 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-modernize-stack/spec.md`

## Summary

Rebuild the off-line paper-database CLI on mainstream Python frameworks while
preserving every user-facing behaviour and the exact four-table PostgreSQL
schema. The bespoke glue (`PsycopgDB` raw-SQL wrapper, `DatabaseConnector`
hand-written SQL, `argparse` dialog loop, `ConfigReader`, per-class
`FileHandler` loggers, `unittest` suite coupled to a developer-local DB) is
replaced by: SQLAlchemy 2.x ORM + repository layer (persistence), a thin
service layer (domain orchestration over pydantic DTOs), a Typer CLI
(presentation) with subcommands and a four-option top-level menu, Alembic
migrations (schema versioning + legacy-variant convergence),
pydantic-settings (four-source config chain), `logging.config.dictConfig`
with a RichHandler, and a `pytest` + `pytest-postgresql` real-DB suite seeded
from a co-located fixture. Packaging moves from Poetry to uv/hatchling with a
`src/paper_sorts/` layout. A reverse-engineered architecture document
(`docs/architecture.md`) is the acceptance baseline. The constitution is
amended in lockstep where this work contradicts its v1.3.0 text.

## Technical Context

**Language/Version**: Python 3.11 (raised from 3.10 per FR-015; min that
SQLAlchemy 2.x + pydantic v2 + Typer support comfortably)
**Primary Dependencies**: SQLAlchemy 2.x (ORM + Core), psycopg v3 (binary
driver), Alembic (migrations), Typer (CLI), Rich (output + RichHandler
logging), pydantic v2 + pydantic-settings v2 (config), pybtex (BibTeX
parse/serialize), pylatexenc (LaTeX → text for `.tex` import), cryptography
(Fernet, encrypted-config source)
**Storage**: PostgreSQL only (psycopg v3). Four tables, schema-preserved:
`papers`, `bib`, `authors_id`, `authors_papers`.
**Testing**: pytest + pytest-postgresql (ephemeral PG from host `pg_ctl`);
pytest-cov for coverage; real DB, no mocking the session/repositories/driver.
**Target Platform**: Linux CLI, single user, local PostgreSQL. CLI-only
(FR-017) — no web/REST/TUI/GUI surface.
**Project Type**: Single-project CLI tool, src-layout package.
**Performance Goals**: No measurable regression vs. the current baseline on a
personal-library-sized dataset (Constitution IV, SC-006). Interactive ops
(search/add/update/delete a single paper) measured by wall-clock on the same
seeded fixture; a bench harness records a baseline for comparison.
**Constraints**: Offline, single-user; no connection pooling beyond
SQLAlchemy defaults, no async driver, no caches, no read replicas
(Constitution IV). Sessions context-managed and closed deterministically.
Schema-preservation contract: no new NOT NULL outside PKs, no FKs on
`authors_papers`, no indexes beyond the original DDL.
**Scale/Scope**: ~dozens–hundreds of papers (personal corpus). Project Python
LOC under `src/paper_sorts/` must drop ≥30 % vs. the current ~2 000 (SC-005),
with mainstream-library imports replacing bespoke code.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

The feature touches all four principles. Several v1.3.0 clauses are written
*assuming this modernization is already done* (they reference
`src/paper_sorts/db/`, ruff, pytest, `cli/prompts.py`). FR-016 + the spec
Assumptions mandate amending any conflicting text via `/speckit-constitution`
in the same change set. The relevant conflicts live in the **Stack &
Constraints** and **Development Workflow & Quality Gates** sections, not the
four principles themselves:

| Principle / Section | Status | Notes |
|---|---|---|
| I. Code Quality | **Upheld** (already amended in v1.3.0) | ruff + ruff format, full type hints + docstrings, driver/ORM isolated to `db/`. This plan implements exactly what the principle already describes. |
| II. Testing Standards | **Upheld** (already amended) | pytest + pytest-postgresql real DB; no mocking the session/repos/driver; seed data co-located in `tests/fixtures/`. |
| III. UX Consistency | **Upheld** (already amended) | All prompts route through `cli/prompts.py`; 1-indexed menus with explicit quit; dual-form confirmations; plain-language errors + logged detail. |
| IV. Performance | **Upheld** | Parameterised queries + joins over the existing four tables; no new index/table/denormalisation; context-managed sessions; per-paper commit on bulk import. Bench harness records a baseline (SC-006). |
| §Development Workflow & Quality Gates | **Amend** | Still says "MUST pass `pylint paper_sorts` and the unittest suite" and references `DatabaseConnector.create_tables()`. These contradict FR-009/FR-010/FR-005. Amend to ruff + pytest + Alembic. |
| §Stack & Constraints | **No change needed** | v1.3.0 already states uv/hatchling, psycopg v3, SQLAlchemy 2.x isolated to persistence, pydantic-settings four-source chain. |

**Gate result**: PASS, conditional on the `/speckit-constitution` amendment of
the **Development Workflow & Quality Gates** section (pylint→ruff,
unittest→pytest, `create_tables()`→Alembic). No principle is relaxed or
removed, so the bump is MINOR (1.3.0 → 1.4.0). Tracked as task T0xx in the
Foundational phase; SC-007 requires it complete before merge. No Complexity
Tracking violations: no new table, index, FK, pool, cache, or async driver is
introduced.

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output (cli-commands.md)
└── tasks.md             # Phase 2 output (/speckit-tasks)
```

### Source Code (repository root)

```text
pyproject.toml               # PEP 621 + uv + hatchling; deps + ruff/mypy/pytest config
uv.lock                      # reproducible lock
alembic.ini                  # Alembic config (script_location = migrations)
docs/
└── architecture.md          # FR-001 reverse-engineered baseline of the legacy stack

src/paper_sorts/
├── __init__.py
├── config.py                # pydantic-settings Settings, four-source priority chain
├── logging_config.py        # dictConfig: RichHandler (stdout) + optional FileHandler
├── cli/
│   ├── __init__.py
│   ├── app.py               # Typer app; wires subcommands; four-option top-level menu
│   ├── prompts.py           # ONLY module allowed to import rich.prompt / do input()
│   ├── search.py            # `pdbsearch search`
│   ├── add.py               # `pdbsearch add`
│   ├── update.py            # `pdbsearch update`
│   ├── delete.py            # `pdbsearch delete`
│   ├── importer.py          # `pdbsearch import` (subcommand-only)
│   └── migrate.py           # `pdbsearch migrate` (subcommand-only)
├── services/
│   ├── __init__.py
│   ├── paper_service.py     # search_by_title/author, add_paper, update_field, delete_paper
│   └── import_service.py    # extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]
└── db/
    ├── __init__.py
    ├── models.py            # 4 ORM models (declarative, SQLAlchemy 2.x typed)
    ├── session.py           # engine factory + with_session(...) context manager
    └── repositories.py      # PaperRepository/AuthorRepository/BibRepository + DTOs

migrations/
├── env.py                   # Alembic environment (reads Settings.database_url)
├── script.py.mako
└── versions/
    ├── 001_*.py             # verbatim port of legacy DDL (canonical schema)
    └── 002_*.py             # converge legacy bibtext_id variant onto canonical

tests/
├── conftest.py              # postgresql_proc, ephemeral_db_url, seeded session fixtures
├── fixtures/
│   ├── seed_papers.py       # SEED_PAPERS canonical dataset (co-located w/ assertions)
│   ├── sample.bib           # BibTeX fixture (accents/escapes round-trip)
│   └── literature_overview.tex + bib.bib  # bulk-import fixture pair
├── benchmarks/
│   ├── bench_baseline.py    # interactive-op timing harness (SC-006)
│   └── baseline.json        # recorded baseline
├── test_repositories.py     # CRUD + search, real DB
├── test_paper_service.py    # service orchestration over a real DB
├── test_import_service.py   # tex+bib extraction
├── test_migration.py        # legacy→canonical convergence, idempotency
├── test_config.py           # four-source priority, Fernet source, lost-key error
└── test_cli.py              # Typer CliRunner end-to-end over the seeded DB
```

**Structure Decision**: Single-project CLI, src-layout. Four layers map 1:1
to directories: `cli/` (presentation) → `services/` (domain) → `db/`
(persistence) + `config.py`/`logging_config.py` (cross-cutting). The driver
and ORM imports are confined to `db/`; services depend only on DTOs exposed by
`db/repositories.py`. The legacy flat `paper_sorts/` package is deleted once
the modern package covers its behaviour (FR-012).

## Complexity Tracking

> No Constitution violations requiring justification.

No new table, index, FK, connection pool, cache, async driver, or
non-CLI surface is introduced. The single governance change (amend the
Development Workflow & Quality Gates section pylint→ruff, unittest→pytest,
`create_tables()`→Alembic) is mandated by FR-016 and is a documentation
sync, not a complexity addition.
