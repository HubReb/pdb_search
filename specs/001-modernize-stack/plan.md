# Implementation Plan: Modernize the Stack

**Branch**: `001-modernize-stack` | **Date**: 2026-06-17 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-modernize-stack/spec.md`

## Summary

Rebuild the offline paper-database CLI on mainstream Python frameworks while preserving 100% of user-facing behaviour. The bespoke `argparse` + manual dialog loop, hand-written SQL strings in `PsycopgDB`/`DatabaseConnector`, lazy `create_tables()`, Fernet-only `ConfigReader`, per-class file loggers, and `unittest` suite are replaced by:

- **Typer** CLI (`src/paper_sorts/cli/`) with subcommands `search / add / update / delete / import / migrate` plus a four-option top-level menu when invoked bare.
- **SQLAlchemy 2.x** ORM + repository classes isolated to `src/paper_sorts/db/`, with pydantic DTOs (`PaperSummary`, `PaperCreate`) crossing into the service layer so services never touch ORM types.
- A pure **service layer** (`src/paper_sorts/services/`) holding domain operations and the bulk-import extractor.
- **Alembic** migrations under `migrations/versions/`: revision 0001 a verbatim port of the original DDL (canonical `bibtex_id`), revision 0002 converging the legacy `bibtext_id` (sic) typo schema onto the canonical one — idempotent, satisfying FR-011 / US4.
- **pydantic-settings** config (`config.py`) with the four-source priority chain (CLI > env `PDBSEARCH_*` > `.env` > Fernet-encrypted INI), keeping the encrypted-config workflow as one supported source.
- **stdlib logging** via a single `dictConfig` (`logging_config.py`): RichHandler to stdout + optional FileHandler, replacing per-class `*.log` files.
- **uv / PEP 621 / hatchling** packaging; **ruff** lint+format; **pytest** + **pytest-postgresql** real-DB suite with co-located seed fixtures; **coverage** per-layer; an executing **baseline benchmark** harness.

The legacy flat-layout `paper_sorts/` is deleted once the modern stack covers its behaviour (FR-012). A reverse-engineered architecture document (US1, FR-001) captures the pre-modernization stack as the acceptance reference.

## Technical Context

**Language/Version**: Python ≥ 3.11 (raised from 3.10 per FR-015 / constitution Stack & Constraints)
**Primary Dependencies**: SQLAlchemy 2.x, Alembic, Typer, Rich, pydantic + pydantic-settings v2, psycopg v3 (binary), cryptography (Fernet), pybtex, pylatexenc
**Storage**: PostgreSQL only (driver psycopg v3); four-table schema preserved verbatim (`papers`, `bib`, `authors_id`, `authors_papers`)
**Testing**: pytest + pytest-postgresql (ephemeral PG from host `pg_ctl`), pytest-cov for per-layer coverage; Typer `CliRunner` for the interface layer
**Target Platform**: Linux/commodity workstation, single-user offline CLI
**Project Type**: Single-project src-layout CLI application
**Performance Goals**: No measurable regression vs. the legacy implementation on interactive ops (search-by-title/author, single add/update/delete), measured by a recorded baseline benchmark (constitution Principle IV gate)
**Constraints**: Offline, single-user; no connection pools/caches/async drivers; sessions context-managed and deterministically closed; driver/ORM isolated to `db/`; prompts routed only through `cli/prompts.py`; README.md & CLAUDE.md must contain no forbidden legacy tokens after FR-012
**Scale/Scope**: Personal-library-sized dataset (tens–hundreds of papers); ~2 300 legacy LOC under `paper_sorts/`, target ≥ 30% reduction (SC-005)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Constitution version in effect in this worktree: **1.3.0-b2-hardened** (experiment line; three extra mechanical merge-blocking gates G1/G2/G3).

| Principle | Touched? | How the plan satisfies it |
|-----------|----------|---------------------------|
| **I. Code Quality (NON-NEGOTIABLE)** | Yes | Full type hints + docstrings on every public symbol; `ruff check` + `ruff format --check` clean; persistence isolation — only `db/` imports `sqlalchemy`/`psycopg`; services use repositories + DTOs. **G3 doc-currency gate**: after FR-012 removal, README.md & CLAUDE.md must not contain `Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB`; enforced by an automated test (`tests/test_doc_currency.py`). |
| **II. Testing Standards** | Yes | pytest under `tests/`, real-DB integration via pytest-postgresql, no mocking the session/repositories/driver. Seed data co-located in `tests/fixtures/seed_papers.py`. No always-failing placeholder. **G1 per-layer coverage gate**: db/, services/, cli/, config.py each ≥ 80%; interface layer satisfied by an end-to-end `CliRunner` test over every subcommand. |
| **III. UX Consistency** | Yes | All prompts route through `cli/prompts.py` (sole permitted importer of `rich.prompt`); 1-indexed menus with explicit abort/quit; update & delete confirmations accepting `1`/`2`/`y`/`n`/`yes`/`no`; failures logged + plain-language stdout message, no tracebacks. |
| **IV. Performance** | Yes | Parameterised queries + joins over the existing four tables; no new index/table/denormalisation; sessions context-managed via `with_session`; no pool/cache/async. **G2 baseline-benchmark gate**: `tests/benchmarks/` harness exists and executes (records a baseline for the five interactive ops) — not permanently skipped. |

**FR-016 / SC-007 (constitution amendment)**: The constitution body (Principles I–IV, Stack & Constraints) is already expressed in the modern vocabulary (ruff, pytest, SQLAlchemy session isolation, uv, pydantic-settings, `cli.prompts`). The only stale references are in the *Development Workflow & Quality Gates* section (`pylint paper_sorts`, `unittest`, `DatabaseConnector.create_tables()`). These are reconciled via `/speckit-constitution` as part of this work so no silent deviation remains — recorded in research.md §R10.

**Initial gate result**: PASS (no waivers required; no Complexity Tracking entries). Re-evaluated post-design at end of Phase 1 — still PASS.

## Project Structure

### Documentation (this feature)

```text
specs/001-modernize-stack/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/
│   └── cli-commands.md  # Phase 1 output — subcommand contracts
└── tasks.md             # Phase 2 output (/speckit-tasks)
```

### Source Code (repository root)

```text
src/paper_sorts/
├── __init__.py
├── config.py                 # pydantic-settings Settings, four-source priority
├── logging_config.py         # single dictConfig (Rich stdout + optional file)
├── cli/
│   ├── __init__.py
│   ├── app.py                # Typer app; wires subcommands; bare-invocation menu
│   ├── prompts.py            # ONLY module allowed to import rich.prompt
│   ├── search.py
│   ├── add.py
│   ├── update.py
│   ├── delete.py
│   ├── importer.py
│   └── migrate.py
├── services/
│   ├── __init__.py
│   ├── paper_service.py      # search_by_title/author, add_paper, update_field, delete_paper
│   └── import_service.py     # extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]
└── db/
    ├── __init__.py
    ├── session.py            # with_session(...) context manager
    ├── models.py             # four ORM models
    └── repositories.py       # PaperRepository/AuthorRepository/BibRepository + DTOs

migrations/
├── env.py
├── script.py.mako
└── versions/
    ├── 0001_baseline_schema.py
    └── 0002_converge_legacy_bibtext.py

tests/
├── conftest.py               # postgresql_proc, ephemeral_db_url, seeded_db fixtures
├── fixtures/
│   ├── __init__.py
│   └── seed_papers.py        # SEED_PAPERS canonical dataset
├── benchmarks/
│   ├── __init__.py
│   └── bench_baseline.py     # executing baseline harness (G2)
├── test_repositories.py      # persistence layer (real DB)
├── test_paper_service.py     # service layer
├── test_import_service.py
├── test_config.py            # configuration layer
├── test_prompts.py           # prompts unit tests
├── test_cli.py               # interface layer end-to-end via CliRunner
├── test_migrations.py        # rev 0001 + 0002 idempotent converge (US4)
└── test_doc_currency.py      # G3 forbidden-token gate

docs/
└── architecture.md           # US1 / FR-001 reverse-engineered legacy description

pyproject.toml, uv.lock, alembic.ini, README.md, CLAUDE.md
```

**Structure Decision**: Single-project src-layout. The four architectural layers map to `cli/` (presentation), `services/` (domain), `db/` (persistence), and `config.py` (configuration) — matching the four coverage buckets of constitution Principle II G1 and the driver-isolation rule of Principle I. Tests are flat under `tests/` per pytest defaults, with seed data and benchmarks in dedicated subpackages.

## Complexity Tracking

> No constitution violations — no waivers required. Table intentionally empty.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| — | — | — |
