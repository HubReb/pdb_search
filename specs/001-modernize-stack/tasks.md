# Tasks: Modernize the Stack

**Input**: Design documents from `/specs/001-modernize-stack/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/cli-commands.md

**Tests**: INCLUDED. The spec mandates a real-DB integration suite (US3, FR-008, FR-009) and
persistence-layer coverage ≥ 80 % (SC-008). Persistence tests run against ephemeral PostgreSQL;
the SQLAlchemy session/repositories/driver are never mocked (Principle II).

**Organization**: Tasks are grouped by user story. US2 is the MVP. The build order is Setup →
Foundational → US3 harness (so later stories are testable) → US2 (MVP) → US4 → US5 → US1 →
Polish. US1 (architecture doc) is authored late because it documents the *legacy* system and is
independent of the new code; it can be written any time but is grouped before Polish.

## Path Conventions

Single project, src-layout: `src/paper_sorts/`, `tests/`, `migrations/`, `docs/` at repo root.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project skeleton, dependency manifest, tooling.

- [X] T001 Replace the Poetry `pyproject.toml` with a PEP 621 manifest at `pyproject.toml`: hatchling build backend, `requires-python = ">=3.11"`, src-layout package discovery (`src/paper_sorts`), `[project.scripts] pdbsearch = "paper_sorts.cli.app:app"`, runtime deps (sqlalchemy>=2, psycopg[binary], alembic, typer, rich, pydantic-settings, cryptography, pybtex, pylatexenc) and a `dev`/`all-extras` group (pytest, pytest-postgresql, pytest-cov, ruff, mypy).
- [X] T002 [P] Configure tooling in `pyproject.toml`: `[tool.ruff]` (lint + format), `[tool.mypy]` strict on `src/`, `[tool.pytest.ini_options]` (testpaths=tests).
- [X] T003 Create the src-layout package skeleton: `src/paper_sorts/__init__.py`, `src/paper_sorts/cli/__init__.py`, `src/paper_sorts/services/__init__.py`, `src/paper_sorts/db/__init__.py`.
- [X] T004 Run `uv sync --all-extras` and confirm the environment resolves (lockfile written).

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Persistence layer, config, logging, and session management that every story needs.
**⚠️ MUST complete before any user story phase.**

- [X] T005 Implement `src/paper_sorts/db/models.py`: SQLAlchemy 2.x declarative `Paper`, `Bib`, `AuthorId`, `AuthorPaper` mapping the four legacy tables verbatim (no new columns, no FKs on `authors_papers`, only `papers.bibtex_id → bib.bibtex_id`, `bib.bibtex` UNIQUE) per data-model.md.
- [X] T006 Implement `src/paper_sorts/db/session.py`: engine factory from a database URL and a `with_session(engine)` context manager that commits on success and rolls back on exception (deterministic close — Principle IV). This is the only place a `Session` is opened.
- [X] T007 Implement `src/paper_sorts/db/repositories.py`: pydantic DTOs `PaperSummary` and `PaperCreate`, plus `PaperRepository`, `AuthorRepository`, `BibRepository` with parameterised joins for search-by-title / search-by-author, CRUD, and the duplicate-author / orphan-author cleanup semantics. Repositories accept a `Session`; they expose DTOs, never ORM types.
- [X] T008 [P] Implement `src/paper_sorts/config.py`: pydantic-settings `Settings` with the four-source priority chain (CLI > `PDBSEARCH_*` env > `.env` > Fernet-encrypted INI custom source). Secrets never logged. A clear error when the key file is missing.
- [X] T009 [P] Implement `src/paper_sorts/logging_config.py`: single `logging.config.dictConfig` (RichHandler to stdout, optional FileHandler), one `setup_logging(level)` entry point.
- [X] T010 [P] Implement `src/paper_sorts/cli/prompts.py`: the ONLY module permitted to import `rich.prompt` / call `input`. Helpers: non-empty re-prompt, 1-indexed menu choice with abort, dual-form (numeric+word) yes/no confirmation, numbered disambiguation with out-of-range re-prompt.
- [X] T011 Scaffold Alembic: `alembic.ini` (script_location=migrations) and `migrations/env.py` that reads the URL from `Settings`; `migrations/script.py.mako`.

---

## Phase 3: User Story 3 — Reproducible Test Suite Without Developer-Local State (P2)

**Goal**: A fresh checkout runs the suite against an ephemeral PostgreSQL with co-located seed
data and no developer-local DB. Built first so all later stories are testable.

**Independent test**: `git clone && uv sync && uv run pytest` passes on a machine that never had
the personal database.

- [X] T012 [US3] Author `tests/fixtures/seed_papers.py` with `SEED_PAPERS` — a canonical dataset co-located with tests, including the BibTeX-accent edge case (`\"o`, `\&`, `{Pino}`) and a duplicate-title pair for disambiguation coverage.
- [X] T013 [US3] Implement `tests/conftest.py`: `postgresql_proc` + `ephemeral_db_url` session fixtures (pytest-postgresql off host `pg_ctl`), an `engine` fixture that runs `alembic upgrade head` against the ephemeral DB, and a `seeded_session` fixture that loads `SEED_PAPERS`.
- [X] T014 [P] [US3] Add `tests/fixtures/sample.tex` + `tests/fixtures/sample.bib` (bulk-import pair, incl. one cited key with no matching `.bib` record) and `tests/fixtures/single.bib` (single-entry add).

---

## Phase 4: User Story 2 — Modernized Codebase, Same User-Facing Behavior (P1) 🎯 MVP

**Goal**: The rebuilt CLI delivers search-by-title, search-by-author, add (inline + from `.bib`),
update (title/contents/bibtex/author), and delete — same prompts/outputs/data as legacy — on the
mainstream stack.

**Independent test**: Run the scripted dialog through every CLI path against seeded data; each
path produces equivalent-or-improved output vs. legacy.

- [X] T015 [US2] Create Alembic revision `migrations/versions/001_*.py`: create the four canonical tables verbatim (the legacy DDL ported), with `bib.bibtex` UNIQUE and the `fk_bibtex_id` FK. Downgrade drops them.
- [X] T016 [US2] Implement `src/paper_sorts/services/paper_service.py`: `search_by_title`, `search_by_author`, `add_paper(PaperCreate)`, `update_field(table: Literal[...], column, value, identifier)` (match/case with `assert_never(table)`), `delete_paper(...)`. Pure orchestration over repositories + DTOs; no SQL, no rich, no I/O.
- [X] T017 [P] [US2] Write `tests/test_repositories.py`: real-DB tests for search joins, CRUD, duplicate-author and orphan-author cleanup, referencing `SEED_PAPERS`.
- [X] T018 [P] [US2] Write `tests/test_paper_service.py`: real-DB tests for search/add/update each field/delete and the abort-on-`update_field` of `authors_papers`.
- [X] T019 [US2] Implement `src/paper_sorts/cli/search.py`: interactive search sub-menu (by author / by title / abort), disambiguation on multiple title matches, legacy pretty-print display, plain "not found" messages.
- [X] T020 [US2] Implement `src/paper_sorts/cli/add.py`: sequential prompts (authors, title, key, bibtex inline-or-file, summary), persist via the service; empty-input re-prompt preserved.
- [X] T021 [US2] Implement `src/paper_sorts/cli/update.py`: table → column sub-menus, identifier + new-value prompts, dual-form confirmation summarising the exact change before applying; `n` writes nothing.
- [X] T022 [US2] Implement `src/paper_sorts/cli/delete.py`: identify paper, dual-form confirmation, delete paper + bib + authorship links via the service.
- [X] T023 [US2] Implement `src/paper_sorts/cli/app.py`: Typer app wiring search/add/update/delete (and migrate/import once those exist); on no subcommand, drop into the legacy four-option menu; call `setup_logging` once; load `Settings`; surface plain-language errors (no stack traces to stdout).
- [X] T024 [P] [US2] Write `tests/test_prompts.py`: unit tests for `cli/prompts.py` — empty-input re-prompt, menu parse + abort, dual-form confirmation, out-of-range disambiguation re-prompt.
- [X] T025 [P] [US2] Write `tests/test_config.py`: unit tests for the four-source priority chain and the Fernet source (incl. missing-key clear error).

---

## Phase 5: User Story 4 — One-Shot Migration of Existing Personal Database (P2)

**Goal**: A single `pdbsearch migrate` converges either historical schema (`bibtex_id` or legacy
`bibtext_id`) onto canonical with zero data loss; idempotent.

**Independent test**: Snapshot row counts (papers/authors/authorships/bib) before and after a
migrate on each variant; counts match; rerun is a no-op.

- [X] T026 [US4] Create Alembic revision `migrations/versions/002_*.py`: detect the legacy `bibtext_id`/`bibtext` columns and rename to canonical `bibtex_id`/`bibtex`; idempotent (no-op once converged). Downgrade documented.
- [X] T027 [US4] Implement `src/paper_sorts/cli/migrate.py`: `migrate` subcommand running `alembic upgrade head` against the configured DB; plain success/failure messaging.
- [X] T028 [US4] Write `tests/test_migration.py`: real-DB tests — fresh-DB upgrade creates the four tables; a seeded legacy-`bibtext_id` DB converges with matching row counts; rerun is idempotent.

---

## Phase 6: User Story 5 — Bulk Import from LaTeX/BibTeX Preserved (P3)

**Goal**: `pdbsearch import --tex --bib` inserts every cited entry with a matching `.bib` record;
unmatched keys are skipped with a logged warning; commits per paper.

**Independent test**: Import the fixture pair with N entries; verify N papers + authors + bib
entries; an unmatched key is skipped, not fatal; a rerun does not duplicate.

- [X] T029 [US5] Implement `src/paper_sorts/services/import_service.py`: `extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]` (pylatexenc decode + pybtex parse, `Last, First` authors), skipping cited keys with no matching `.bib` record (logged warning).
- [X] T030 [US5] Implement `src/paper_sorts/cli/importer.py`: `import` subcommand consuming the iterator and persisting per paper (per-paper commit; already-present keys skipped).
- [X] T031 [P] [US5] Write `tests/test_import_service.py`: real-DB tests — N entries imported, unmatched key skipped, rerun does not duplicate (BibTeX-key uniqueness).

---

## Phase 7: User Story 1 — Reverse-Engineered Architecture Documentation (P1)

**Goal**: A single architecture document captures the legacy system's purpose, journeys, data
model, control flow, configuration, install/run, and known limitations — the acceptance
reference for the modernization.

**Independent test**: A Python dev who has never seen the project answers "what does it do? data
model? where to add a field?" from the doc alone.

- [X] T032 [US1] Author `docs/architecture.md`: reverse-engineered description of the legacy stack (CLI dialog → `DatabaseConnector` → `PsycopgDB` → PostgreSQL), the four tables and relationships, the `bibtex_id` vs `bibtext_id` variants, config/Fernet flow, rollback semantics on partial add, install/run, and known limitations (duplicate `Last, First` authors treated as one, etc.).

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Remove legacy, amend the constitution, refresh docs, prove the gates.

- [X] T033 Remove the legacy flat-layout package: delete `paper_sorts/` (add.py, search.py, get_data.py, database_connector.py, psycopg_db.py, helpers.py, config_reader.py, run.py, user_interaction.py, __init__.py) and the legacy `tests/test_database_connector.py` / `tests/test_user_interaction.py` (FR-012) once coverage is in the new suite.
- [X] T034 Amend the constitution via `/speckit-constitution` to v1.3.1 (PATCH): sweep the stale `pylint paper_sorts`, `unittest`, `helpers.get_user_input`, and `DatabaseConnector.create_tables()` references in Development Workflow & Quality Gates to ruff/pytest/`cli/prompts.py`/Alembic (FR-016, SC-007).
- [X] T035 [P] Rewrite `README.md` for the modern stack (uv install, `pdbsearch` subcommands, config priority chain, link to `docs/architecture.md` and quickstart).
- [X] T036 [P] Rewrite `CLAUDE.md` to describe the modern architecture (layers, `db/`-only driver isolation, `cli/prompts.py` rule, test/seed layout, SpecKit pointer).
- [X] T037 Run the full gate set and fix any findings: `uv run ruff check src tests`, `uv run ruff format --check src tests`, `uv run mypy src`, `uv run pytest` (and `--cov` to confirm SC-008 ≥ 80 % on the persistence layer). Confirm SC-005 (≥ 30 % LOC reduction under the package vs. the ~2 000 legacy lines).

---

## Dependencies & Execution Order

- **Setup (T001–T004)** → blocks everything.
- **Foundational (T005–T011)** → blocks all user stories.
- **US3 (T012–T014)** → the test harness; precedes the test tasks of every later story.
- **US2 (T015–T025)** 🎯 MVP → depends on Foundational + US3. T015 (rev 001) precedes service/CLI tests.
- **US4 (T026–T028)** → depends on US2's rev 001 (T015) for the canonical target.
- **US5 (T029–T031)** → depends on Foundational + US3; uses the service/repo layer from US2.
- **US1 (T032)** → independent (documents legacy); can run any time, grouped before Polish.
- **Polish (T033–T037)** → last: legacy removal after the new suite is green; constitution amend; gates.

### Story independence

- US3 is infrastructure consumed by US2/US4/US5 tests.
- US2 is the MVP and standalone deliverable.
- US4 and US5 are additive and independently testable on top of US2.
- US1 is a pure documentation deliverable, independent of the code.

## Parallel Execution Examples

- Setup: T002 ∥ (after T001).
- Foundational: T008 ∥ T009 ∥ T010 (distinct files) after T005–T007.
- US2 tests: T017 ∥ T018 ∥ T024 ∥ T025 (distinct files) once their targets exist.
- Polish docs: T035 ∥ T036.

## Implementation Strategy

1. **MVP = Setup + Foundational + US3 + US2.** This delivers the modernized CLI with the full
   real-DB test suite — the core spec deliverable (SC-002).
2. **Increment 2 = US4** (migration) — unblocks existing-data users.
3. **Increment 3 = US5** (bulk import) — restores the bootstrap path.
4. **US1 + Polish** — architecture doc, legacy removal, constitution amendment, green gates.
