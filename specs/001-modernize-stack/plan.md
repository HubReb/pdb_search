# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-17 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-modernize-stack/spec.md`

## Summary

Reverse-engineer the legacy flat-layout `paper_sorts/` package (hand-written
psycopg2 SQL, bespoke `argparse` dialog loop, `unittest`, Poetry, encrypted-INI
config) and rebuild it on mainstream frameworks with no observable change to
CLI behaviour. Target stack: **src-layout** `src/paper_sorts/` package on
**SQLAlchemy 2.x** (driver `psycopg` v3), **Typer** CLI, **Alembic** migrations,
**pydantic-settings** config with the four-source priority chain, **uv** +
**hatchling** packaging, **ruff** lint/format, and **pytest** with
**pytest-postgresql** real-DB integration tests. A one-shot `migrate` command
upgrades either historical schema (`bibtex_id` or the legacy `bibtext_id` typo)
to the canonical schema; a per-paper `import` command preserves the LaTeX/BibTeX
bulk-import path. FR-016 conflicts in the constitution are amended via
`/speckit-constitution` as part of this work, and the b2-hardened mechanical
gates (G1 per-layer coverage, G2 executing baseline benchmark, G3 doc-currency
forbidden-token check) are satisfied by concrete deliverables, not waived.

## Technical Context

**Language/Version**: Python 3.11+ (FR-015; raises the legacy 3.10 minimum)
**Primary Dependencies**: SQLAlchemy 2.x, psycopg v3 (binary), Alembic, Typer,
  rich (Typer's renderer, used for prompts via `cli/prompts.py`),
  pydantic-settings v2, pydantic v2, cryptography (Fernet config source),
  pybtex (BibTeX parse/round-trip), pylatexenc (LaTeX→text for `.tex` import)
**Storage**: PostgreSQL only; four-table schema preserved verbatim
  (`papers`, `bib`, `authors_id`, `authors_papers`)
**Testing**: pytest + pytest-postgresql (ephemeral PG per session via host
  `pg_ctl`), pytest-cov for the per-layer coverage gate, Typer `CliRunner` for
  the interface layer
**Target Platform**: Linux CLI, single-user offline
**Project Type**: Single-project CLI tool (src-layout package)
**Performance Goals**: No measurable regression vs. legacy baseline on
  interactive operations (search-by-title/author, single add/update/delete),
  measured on the seed fixture (Principle IV; SC-006)
**Constraints**: Persistence-driver imports isolated to `db/`; prompts routed
  through `cli/prompts.py`; deterministic session close; no pooling/async/cache;
  schema-preservation contract (no new NOT NULL outside PKs, no FK on
  `authors_papers`, no indexes beyond the original PKs)
**Scale/Scope**: Personal-library dataset (tens–hundreds of papers); ~2300
  legacy LOC to be replaced with ≥30% fewer project-authored lines (SC-005)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Constitution version in this worktree: **1.3.0-b2-hardened** (experiment
worktree only). Four principles + three mechanical gates (G1/G2/G3).

| Principle | Touched? | Plan disposition |
|-----------|----------|------------------|
| I. Code Quality (NON-NEGOTIABLE) | Yes | All `src/` code typed + docstringed; `ruff check`/`ruff format --check` clean. Driver isolation: only `db/` imports `sqlalchemy`/`psycopg`. **G3 doc-currency**: an automated test asserts `README.md`/`CLAUDE.md` contain none of `Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB`. |
| II. Testing Standards | Yes | pytest + pytest-postgresql; no mocking of session/repos/driver in persistence tests; seed fixture co-located and referenced. **G1 per-layer coverage**: each of `db/`, `services/`, `cli/`, `config.py` independently ≥80%, measured per-layer (not aggregate); interface layer via `CliRunner` over every subcommand. |
| III. UX Consistency | Yes | All prompts route through `cli/prompts.py`; 1-indexed menus with explicit abort/quit; update/delete confirmation accepting numeric+word forms; plain-language errors to stdout, detail to logs. |
| IV. Performance | Yes | Parameterised queries + joins over the existing four tables; no new index/table/denormalisation; context-managed sessions. **G2 baseline benchmark**: an *executing* harness under `tests/benchmarks/` records a baseline for the five interactive ops — not permanently skipped. |

**FR-016 amendments (made via `/speckit-constitution` in this change set).** The
constitution's *body principles* (I–IV) already reference the modern stack
(ruff, pytest, `src/paper_sorts/db`, `cli/prompts.py`), but two legacy
references remain in the workflow section and MUST be amended so the final state
has no silent deviation:

1. **Development Workflow & Quality Gates** still says *"Every change MUST pass
   `pylint paper_sorts` and the unittest suite"* → amend to `ruff check src tests`
   + `pytest`.
2. **Development Workflow & Quality Gates** still says *"Schema changes MUST
   update `DatabaseConnector.create_tables()`"* (a deleted legacy method) →
   amend to "land as Alembic migrations under `migrations/versions/`".

These are PATCH-level wording corrections that align stale workflow text with
the already-modern principle bodies; performed via `/speckit-constitution`, not
edited silently (SC-007).

**Gate result**: PASS — no unjustified violation. No Complexity Tracking entry
required (no new table/index/denormalisation; the four-table schema is
preserved).

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── cli-commands.md
│   └── repository-api.md
├── checklists/
│   └── requirements.md  # (pre-existing)
└── tasks.md             # Phase 2 output (/speckit-tasks)
```

### Source Code (repository root)

```text
src/paper_sorts/
├── __init__.py
├── config.py                 # pydantic-settings Settings, four-source chain
├── logging_config.py         # dictConfig (RichHandler + optional FileHandler)
├── cli/
│   ├── __init__.py
│   ├── app.py                # Typer app; wires subcommands; no-subcommand menu
│   ├── prompts.py            # ONLY module allowed to import rich.prompt
│   ├── search.py             # `search` subcommand + interactive search flow
│   ├── add.py                # `add` subcommand (inline / from .bib)
│   ├── update.py             # `update` subcommand
│   ├── delete.py             # `delete` subcommand
│   ├── importer.py           # `import` subcommand (bulk .tex + .bib)
│   └── migrate.py            # `migrate` subcommand (legacy → canonical)
├── services/
│   ├── __init__.py
│   ├── paper_service.py      # search_by_title/author, add/update/delete
│   └── import_service.py     # extract_papers_from_tex_bib -> Iterator[PaperCreate]
└── db/
    ├── __init__.py
    ├── models.py             # 4 ORM models (papers, bib, authors_id, authors_papers)
    ├── repositories.py       # PaperRepository/AuthorRepository/BibRepository + DTOs
    └── session.py            # engine + with_session(...) context manager

migrations/
├── env.py
├── script.py.mako
└── versions/
    ├── 001_initial_schema.py # verbatim port of original DDL
    └── 002_converge_legacy.py# bibtext_id (sic) → bibtex_id convergence

tests/
├── conftest.py               # postgresql_proc, ephemeral_db_url, seeded session
├── fixtures/
│   └── seed_papers.py        # canonical SEED_PAPERS dataset (co-located)
├── test_repositories.py      # persistence layer (real DB)
├── test_paper_service.py     # service layer
├── test_import_service.py    # bulk-import service
├── test_migrate.py           # both historical schemas → canonical, idempotent
├── test_config.py            # four-source priority + Fernet source
├── test_cli.py               # CliRunner over every subcommand (interface layer)
└── benchmarks/
    ├── bench_baseline.py     # executing baseline benchmark (G2)
    └── baseline.json         # recorded baseline

pyproject.toml                # PEP 621 + uv + hatchling + ruff + pytest config
alembic.ini
docs/architecture.md          # FR-001 reverse-engineered architecture doc
README.md                     # migrated (G3 forbidden-token clean)
CLAUDE.md                     # migrated (G3 forbidden-token clean)
```

**Structure Decision**: Single-project src-layout CLI. The four-layer split
(presentation `cli/`, domain `services/`, persistence `db/`, configuration
`config.py`) is the architecture the constitution's per-layer coverage gate
(G1) and driver-isolation rule (Principle I) are written against, so it is not
optional — it is the contract.

## Complexity Tracking

> No constitution violations require justification. The four-table schema is
> preserved unchanged (no new table, index, denormalisation, FK, or NOT NULL),
> so Principle IV's "explain why baseline-parity cannot otherwise be met" clause
> is not triggered. Table intentionally empty.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| (none) | — | — |
