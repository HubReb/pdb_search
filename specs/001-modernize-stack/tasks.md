---
description: "Task list for 001-modernize-stack"
---

# Tasks: Modernize the Stack

**Input**: Design documents from `/specs/001-modernize-stack/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: REQUIRED. The reproducible real-DB test suite is itself a deliverable
(US3, FR-008/FR-009, SC-002/SC-008). Test tasks are included.

**Organization**: Tasks are grouped by user story. The dependency reality of a
modernization is that the persistence/service/CLI layers (built in US2, the MVP)
are prerequisites for the test suite (US3), migration (US4), and bulk import
(US5). US1 (architecture doc) is independent and can land first.

## Path Conventions

Single-project src-layout: package at `src/paper_sorts/`, tests at `tests/`,
migrations at `migrations/`, docs at `docs/`.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project scaffolding, dependency management, tooling, constitution alignment.

- [X] T001 Amend `.specify/memory/constitution.md` via the constitution workflow: in the **Development Workflow & Quality Gates** section replace `pylint paper_sorts` + `unittest suite` with `ruff check`/`ruff format --check` + the `pytest`/`pytest-postgresql` suite, drop the "live development database" caveat, and replace the `DatabaseConnector.create_tables()` schema bullet with "Alembic migrations under `migrations/versions/`". Bump 1.3.0 → 1.3.1 (PATCH) and update the Sync Impact Report. (FR-016, SC-007; research.md R10)
- [X] T002 Rewrite `pyproject.toml` to PEP 621 with the `hatchling` build backend: `[project]` metadata (name `paper-sorts`, `requires-python = ">=3.11"`), runtime deps (sqlalchemy, psycopg[binary], alembic, typer, pydantic, pydantic-settings, pybtex, pylatexenc, cryptography, rich), `[project.optional-dependencies] dev` (pytest, pytest-postgresql, pytest-cov, ruff, mypy), `[project.scripts] pdbsearch = "paper_sorts.cli.app:app"`, `[tool.hatch.build.targets.wheel] packages = ["src/paper_sorts"]`, and `[tool.ruff]`/`[tool.mypy]`/`[tool.pytest.ini_options]` config. (FR-010, FR-015)
- [X] T003 Create the src-layout skeleton: `src/paper_sorts/__init__.py`, `src/paper_sorts/cli/__init__.py`, `src/paper_sorts/services/__init__.py`, `src/paper_sorts/db/__init__.py`, and empty `tests/__init__.py` placeholder removed if present; create `docs/` and `migrations/versions/` directories.
- [X] T004 Run `uv sync --all-extras` to resolve and lock dependencies (produces `uv.lock`); confirm `uv run python -c "import sqlalchemy, typer, pydantic_settings, alembic"` succeeds.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: The persistence, config, and logging substrate every user story builds on. No user story can proceed until these exist.

- [X] T005 [P] Implement `src/paper_sorts/db/models.py`: SQLAlchemy 2.x declarative `Base` and the four models (`Bib`, `Paper`, `Author`, `authors_papers` link table) per data-model.md, honouring the schema-preservation contract (nullable non-PK columns, no FK on the link table, only `papers.bibtex_id → bib.bibtex_id`, `bib.bibtex` UNIQUE; full type hints + docstrings).
- [X] T006 [P] Implement `src/paper_sorts/db/session.py`: `with_session(engine)` `@contextmanager` that yields a `Session`, commits on success, rolls back on exception, always closes (Principle IV). (contracts/repositories.md)
- [X] T007 [P] Implement `src/paper_sorts/config.py`: pydantic-settings v2 `Settings` with the four-source priority chain (CLI > `PDBSEARCH_*` env > `.env` > custom Fernet-encrypted-INI source); expose `database_url`, `log_level`, `log_file`; a clear actionable error on lost/wrong key. (FR-007; research.md R5)
- [X] T008 [P] Implement `src/paper_sorts/logging_config.py`: a single `logging.config.dictConfig` (RichHandler to stdout, optional FileHandler), `configure_logging(level, log_file)` called once at startup. (FR-013; research.md R8)
- [X] T009 Implement DTOs + repositories in `src/paper_sorts/db/repositories.py`: `PaperSummary`, `PaperCreate` (pydantic), and `PaperRepository` (`search_by_title`, `search_by_author`, `add`, `delete`, `get_by_id`), `AuthorRepository.rename`, `BibRepository.update_bibtex` — parameterised joins, authors joined with `" and "`, legacy add/delete/rename/orphan-cleanup semantics. Depends on T005/T006. (contracts/repositories.md, data-model.md)
- [X] T010 Scaffold Alembic: `alembic.ini`, `migrations/env.py` (wired to `db.models.Base.metadata` and the `Settings` database URL), `migrations/script.py.mako`. Depends on T005/T007.
- [X] T011 Write `migrations/versions/001_initial_schema.py`: verbatim port of the legacy DDL (canonical `bibtex_id` schema), with a working `downgrade`. (FR-005; research.md R2)
- [X] T012 Set up `tests/conftest.py` (session-scoped `postgresql_proc` off host `pg_ctl`, `ephemeral_db_url`, an `engine` fixture that runs Alembic `upgrade head` or `metadata.create_all`, and a `seeded_session` fixture) and `tests/fixtures/seed_papers.py` with `SEED_PAPERS` (multi-author paper, a duplicate-title pair, LaTeX-accent bib) plus a small `.tex`/`.bib` fixture pair. (FR-008; Principle II)

**Checkpoint**: persistence + config + logging + migrations + test harness exist.

---

## Phase 3: User Story 2 — Modernized Codebase, Same Behaviour (P1) 🎯 MVP

**Goal**: A user runs the rebuilt CLI and gets search/add/update/delete with the
same prompts, outputs, and data as before, on a mainstream stack.

**Independent test**: Run the scripted dialog over every CLI path (search one/many,
add inline/`.bib`, update title/bibtex, abort at confirm, delete, quit) against
the seeded DB; every path matches or improves on legacy output.

- [X] T013 [P] [US2] Implement `src/paper_sorts/cli/prompts.py` (the ONLY module allowed to import `rich.prompt`): `ask_nonempty`, `ask_choice` (1-indexed, mandatory abort, out-of-range re-prompt), `ask_confirm` (dual numeric/word forms), `pick_from` (disambiguation), `display_paper` (legacy pretty-print). (Principle III; contracts/cli-commands.md)
- [X] T014 [P] [US2] Implement `src/paper_sorts/services/paper_service.py`: `search_by_title`, `search_by_author`, `add_paper`, `delete_paper`, and `update_field` (`Literal["papers","bib","authors_id"]` + `match`/`assert_never`, rejecting ID columns and `authors_papers`). Pure orchestration over DTOs/repos; no SQL/rich/I/O. Depends on T009. (contracts/repositories.md; research.md R12)
- [X] T015 [US2] Implement `src/paper_sorts/cli/search.py`: author/title sub-menu, disambiguation via `pick_from`, `display_paper`, plain-language not-found. Depends on T013/T014.
- [X] T016 [US2] Implement `src/paper_sorts/cli/add.py`: inline or `--bib-file` add flow (parse single bib via pybtex helper), `ask_nonempty` prompts, `services.add_paper`. Depends on T013/T014.
- [X] T017 [US2] Implement `src/paper_sorts/cli/update.py`: table/column menus, identifier + value prompts, dual-form `ask_confirm`, `services.update_field`. Depends on T013/T014.
- [X] T018 [US2] Implement `src/paper_sorts/cli/delete.py`: select target, confirmation summary, `services.delete_paper`. Depends on T013/T014.
- [X] T019 [US2] Implement `src/paper_sorts/cli/app.py`: the `typer.Typer` app wiring `search`/`add`/`update`/`delete` (+ `import`/`migrate` from later phases), `configure_logging` at startup, and the no-subcommand `invoke_without_command=True` callback that drops into the four-option top-level menu (Search/Add/Update/Quit). Depends on T015–T018, T007, T008.
- [X] T020 [P] [US2] Tests `tests/test_repositories.py`: real-DB CRUD + both searches + duplicate-title disambiguation shape + orphan-author cleanup, asserting against `SEED_PAPERS`. (Principle II; SC-008)
- [X] T021 [P] [US2] Tests `tests/test_paper_service.py`: `update_field` accept/reject paths (ID immutability, `authors_papers` rejection, unknown column), add/delete round-trips.
- [X] T022 [P] [US2] Tests `tests/test_prompts.py`: `ask_nonempty` empty→re-prompt, `ask_choice` out-of-range→re-prompt, `ask_confirm` numeric+word forms, malformed input. (Principle II pure-helper coverage)
- [X] T023 [P] [US2] Tests `tests/test_config.py`: env-var override, `.env`, encrypted-INI source, priority order, lost-key actionable error.
- [X] T024 [US2] Tests `tests/test_cli.py`: `typer.testing.CliRunner` over `--help`, search/add/update/delete subcommands and the no-subcommand menu, against the seeded ephemeral DB.

**Checkpoint**: US2 is a self-contained, runnable, tested MVP.

---

## Phase 4: User Story 3 — Reproducible Test Suite, No Local State (P2)

**Goal**: Fresh checkout runs the suite against an ephemeral DB; no `database.crypt`/`key`.

**Independent test**: Clone on a machine that never had the personal DB; the
documented install + test commands pass.

- [X] T025 [US3] Harden `tests/conftest.py` so the suite is fully self-contained: schema built per-session from migrations/`metadata`, seed applied per test or module, deterministic teardown; confirm `uv run pytest` passes with no `database.crypt`/`key` present. (FR-008; SC-003)
- [X] T026 [US3] Add `pytest-cov` config/measurement and confirm persistence-layer statement coverage ≥ 80% (`uv run pytest --cov=paper_sorts`). (SC-008)

**Checkpoint**: suite is reproducible and coverage-gated.

---

## Phase 5: User Story 4 — One-Shot Migration (P2)

**Goal**: A personal DB in either historical schema upgrades in one command, zero data loss, idempotently.

**Independent test**: Seed a DB in the legacy `bibtext_id` schema; run migrate;
row counts (papers/authors/authorships/bib) match exactly; re-run is a no-op.

- [X] T027 [US4] Write `migrations/versions/002_converge_legacy.py`: idempotent rename of `bibtext_id → bibtex_id` (papers, bib) and `bibtext → bibtex` (bib) guarded by `information_schema` probes; no-op on canonical DBs; transactional. (FR-011; research.md R2)
- [X] T028 [US4] Implement `src/paper_sorts/cli/migrate.py` and register it on the Typer app: runs Alembic `upgrade head`. (FR-011; contracts/cli-commands.md)
- [X] T029 [P] [US4] Tests `tests/test_migration.py`: build a legacy-typo schema with seeded rows, run the upgrade, assert canonical columns + exact row-count parity + content spot-checks + idempotent re-run. (SC-004)

**Checkpoint**: migration converges both historical schemas with zero loss.

---

## Phase 6: User Story 5 — Bulk Import from LaTeX/BibTeX (P3)

**Goal**: Import all entries from a `.tex` + `.bib` pair via one command; per-paper commit.

**Independent test**: Run import against a fixture pair with N entries; N papers,
authors, and bib entries are present; an unmatched key is skipped with a warning.

- [X] T030 [US5] Implement `src/paper_sorts/services/import_service.py`: `extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]` (pylatexenc for cited titles+keys, pybtex for authors+source, yield matched entries; unmatched skipped). (contracts/repositories.md; research.md R11)
- [X] T031 [US5] Implement `src/paper_sorts/cli/importer.py` and register it: `import --tex --bib`, commit per paper (partial-failure-safe), log+skip unmatched keys, skip already-present BibTeX keys on re-run. (FR-005 AC-3; Principle IV)
- [X] T032 [P] [US5] Tests `tests/test_import_service.py`: against the fixture `.tex`/`.bib` pair, assert N papers/authors/bib inserted, unmatched key skipped, re-run idempotent (no duplicates).

**Checkpoint**: bulk import preserved with per-paper commit.

---

## Phase 7: User Story 1 — Reverse-Engineered Architecture Doc (P1)

**Goal**: One document captures purpose, journeys, data model, control flow, config, install/run, limitations of the (pre-modernization) system, as the acceptance reference.

**Independent test**: A Python dev who never saw the project answers "what does it
do / what's the data model / where do I add a field" from the doc alone.

- [X] T033 [P] [US1] Write `docs/architecture.md`: reverse-engineer the legacy stack (the `DatabaseConnector`/`PsycopgDB`/`UserInteraction`/`get_data` flow, four-table model + relationships, rollback semantics on partial add, encrypted-config workflow, install/run, known quirks incl. `bibtext_id` typo and duplicate-author limitation) and note the modern mapping. (FR-001; SC-001)

> US1 is sequenced last because the architecture doc is most accurate once the
> reverse-engineering and the modern mapping are both fully understood; it has no
> code dependency and could equally be done first.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Remove legacy, finalize docs/gates.

- [X] T034 Remove the legacy flat layout: delete `paper_sorts/` (add.py, search.py, get_data.py, database_connector.py, psycopg_db.py, helpers.py, config_reader.py, user_interaction.py, run.py, __init__.py) and the legacy `tests/test_*.py`. (FR-012)
- [X] T035 [P] Refresh `README.md` to the modern install/run/config/test instructions (mirror quickstart.md).
- [X] T036 Run the full gate: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy src`, `uv run pytest` — all green. Fix any residual lint/type/test failures. (SC-002, SC-007)

---

## Dependencies & Execution Order

- **Phase 1 (Setup)** → **Phase 2 (Foundational)** are hard prerequisites for everything.
- **Phase 3 (US2, MVP)** depends on Phase 2; it is the spine the other stories extend.
- **Phase 4 (US3)** depends on Phase 3 (needs the code to test) + Phase 2 harness.
- **Phase 5 (US4)** depends on Phase 2 (Alembic/models) and the Typer app from Phase 3.
- **Phase 6 (US5)** depends on Phase 2 (DTOs/repos) and the Typer app from Phase 3.
- **Phase 7 (US1)** is independent (doc only).
- **Phase 8 (Polish)** depends on all code phases (legacy removal must not break imports).

### Parallel opportunities

- Phase 2: T005, T006, T007, T008 are `[P]` (different files, no interdeps).
- Phase 3: T013/T014 `[P]`; the test tasks T020–T023 `[P]` once their targets exist.
- Cross-story: once Phase 3 lands, US4 (T027–T029), US5 (T030–T032), and US1 (T033) can proceed in parallel.

## Implementation Strategy

MVP = Phases 1–3 (US2): a fully runnable, tested, modern CLI with parity. Then
layer US3 (reproducibility/coverage), US4 (migration), US5 (bulk import), and the
US1 doc, finishing with legacy removal and the green-gate sweep.
