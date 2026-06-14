# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-14 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `specs/001-modernize-stack/spec.md`

## Summary

Modernize the legacy flat-layout `paper_sorts/` package (Poetry, psycopg2/psycopg mix, hand-written SQL, unittest, argparse, per-class log files) onto a mainstream Python 2026 stack: `src/paper_sorts/` src-layout, uv/PEP 621, SQLAlchemy 2.x + psycopg v3, Alembic migrations, Typer CLI, pydantic-settings config, pytest + pytest-postgresql, ruff linting. All user-visible behaviour is preserved; the legacy procedural modules are removed once fully superseded.

## Technical Context

**Language/Version**: Python ≥ 3.11 (raised from 3.10 per FR-015 and constitution Stack & Constraints)
**Primary Dependencies**: SQLAlchemy 2.x, Typer, Alembic, pydantic-settings v2, psycopg v3 (binary), pybtex, pylatexenc, rich, cryptography, ruff, mypy, pytest, pytest-postgresql
**Storage**: PostgreSQL (local). Four-table schema: `papers`, `bib`, `authors_id`, `authors_papers`.
**Testing**: pytest + pytest-postgresql (ephemeral DB per session). No mocking of SQLAlchemy session or DB driver.
**Target Platform**: Linux CLI, offline, single-user
**Project Type**: CLI tool (personal utility)
**Performance Goals**: No measurable regression vs. current implementation on equivalent interactive ops (search, add, update, delete) on personal-library-sized dataset.
**Constraints**: Offline only. No connection pooling beyond SQLAlchemy default. No async drivers. No web/API surface.
**Scale/Scope**: Single user, personal library (~100–1000 papers). No multi-tenant concerns.

## Constitution Check

| Principle | Impact | Status |
|-----------|--------|--------|
| I. Code Quality | ruff replaces pylint; src-layout; type hints; persistence isolation to `db/` | ✅ Upheld — constitution v1.3.0 already names ruff |
| II. Testing Standards | pytest + pytest-postgresql; no mocking; per-layer ≥80% coverage | ✅ Upheld — constitution v1.3.0 already names pytest-postgresql |
| III. UX Consistency | All prompts routed through `cli/prompts.py`; 1-indexed menus; abort options; confirmation steps | ✅ Upheld |
| IV. Performance | Non-regression criterion; benchmark harness under `tests/benchmarks/`; must execute (not permanently skipped) | ✅ Upheld — baseline recorded and harness runs |
| Doc-currency gate | README.md and CLAUDE.md must not contain `Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB` after FR-012 | ✅ Will enforce in T026 (docs update task) |
| Per-layer coverage gate | Each layer ≥80%: db/, services/, cli/, config.py | ✅ Covered by test tasks |
| Baseline-benchmark gate | `tests/benchmarks/` harness must execute (not permanently skipped) | ✅ Covered by T046 task |

**No waivers required.** Constitution v1.3.0 is already aligned with the modernization targets.

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   └── cli-commands.md
└── tasks.md             # Phase 2 output (/speckit-tasks command)
```

### Source Code (repository root)

```text
src/
└── paper_sorts/
    ├── __init__.py
    ├── config.py              # pydantic-settings Settings; four-source priority chain
    ├── logging_config.py      # dictConfig; RichHandler + optional FileHandler
    ├── cli/
    │   ├── __init__.py
    │   ├── app.py             # Typer app; wires subcommands; top-level menu (no subcommand)
    │   ├── search.py          # search subcommand (title + author)
    │   ├── add.py             # add subcommand
    │   ├── update.py          # update subcommand
    │   ├── delete.py          # delete subcommand
    │   ├── importer.py        # import subcommand (tex+bib bulk import)
    │   ├── migrate.py         # migrate subcommand (Alembic upgrade head)
    │   └── prompts.py         # ALL user-facing prompts; sole importer of rich.prompt
    ├── db/
    │   ├── __init__.py
    │   ├── models.py          # SQLAlchemy ORM models (Paper, Bib, Author, AuthorPaper)
    │   ├── repositories.py    # PaperRepository, AuthorRepository, BibRepository + DTOs
    │   └── session.py         # engine factory; with_session() context manager
    └── services/
        ├── __init__.py
        ├── paper_service.py   # search_by_title, search_by_author, add_paper, update_field, delete_paper
        └── import_service.py  # extract_papers_from_tex_bib() → Iterator[PaperCreate]

migrations/
├── env.py
├── script.py.mako
└── versions/
    └── 001_initial_schema.py  # Verbatim port of DatabaseConnector.create_tables() DDL

tests/
├── conftest.py                # postgresql_proc fixture; ephemeral_db_url; engine; session
├── fixtures/
│   └── seed_papers.py         # SEED_PAPERS constant used by conftest seeding
├── test_repositories.py       # integration: PaperRepository, AuthorRepository, BibRepository
├── test_services.py           # integration: paper_service + import_service
├── test_cli.py                # CLI layer: Typer CliRunner covering all subcommands
├── test_config.py             # unit: Settings, four-source priority, encrypted INI path
├── test_migrations.py         # integration: Alembic upgrade/downgrade, both legacy schemas
└── benchmarks/
    ├── bench_baseline.py      # baseline recorder (runs against seeded ephemeral DB)
    └── baseline.json          # recorded baseline (committed)

pyproject.toml                 # PEP 621; uv; hatchling; [project.scripts] pdbsearch
uv.lock                        # generated by uv
docs/
└── architecture.md            # US1 deliverable: reverse-engineered architecture doc
```

**Structure Decision**: Single src-layout project (Option 1). The `src/` prefix enforces import isolation (avoids accidental imports of uninstalled package). Matches constitution and common 2026 Python practice.

## Complexity Tracking

No constitution violations to justify. The migration task (T014/T015) handles both historical column-name variants (`bibtex_id` vs `bibtext_id`) with conditional column detection in Alembic, which is more complex than a straight `upgrade head` but directly required by FR-011 and the edge-case list.

## Implementation Phases

### Phase T001–T005: Project skeleton & tooling
Set up pyproject.toml (uv, PEP 621, hatchling, ruff, mypy, pytest-postgresql), src-layout skeleton, logging_config.py, config.py.

### Phase T006–T010: Persistence layer (db/)
ORM models, repositories (CRUD + search), session manager, Alembic init + migration 001 (verbatim DDL port).

### Phase T011–T015: Service layer (services/)
paper_service.py (search_by_title, search_by_author, add_paper, update_field, delete_paper), import_service.py (tex+bib parser wrapping pybtex/pylatexenc).

### Phase T016–T025: CLI layer (cli/)
prompts.py, app.py (top-level menu + subcommand wiring), individual subcommand modules (search, add, update, delete, importer, migrate).

### Phase T026–T030: Docs & legacy removal
architecture.md, README/CLAUDE.md update, remove legacy modules (paper_sorts/ flat layout), doc-currency gate test.

### Phase T031–T045: Test suite
conftest.py + fixtures, test_repositories, test_services, test_cli, test_config, test_migrations.

### Phase T046: Benchmark harness
tests/benchmarks/bench_baseline.py + baseline.json recording.
