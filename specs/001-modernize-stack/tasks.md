# Tasks: Modernize the Stack

**Input**: Design documents from `/specs/001-modernize-stack/`
**Prerequisites**: plan.md ✅, spec.md ✅, research.md ✅, data-model.md ✅, contracts/cli-commands.md ✅

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Establish project structure, packaging, and toolchain before any functional code.

- [ ] T001 Write architecture document for the legacy codebase in `docs/architecture.md` (purpose, user journeys, data model, control flow, config, install/run, known limitations — US1 deliverable)
- [ ] T002 Replace `pyproject.toml`: Poetry → uv/hatchling, Python ≥ 3.11, add all modern dependencies (SQLAlchemy, Alembic, Typer, pydantic-settings, psycopg[binary], pytest, pytest-postgresql, pytest-cov, ruff, mypy)
- [ ] T003 [P] Create `src/paper_sorts/__init__.py` and package skeleton (`cli/`, `services/`, `db/` subdirs with `__init__.py`)
- [ ] T004 [P] Add ruff and mypy config sections to `pyproject.toml` (ruff rules, mypy strict, per-module overrides)
- [ ] T005 [P] Add pytest config to `pyproject.toml` (testpaths, markers: benchmark; addopts with --cov)
- [ ] T006 Create `tests/` directory skeleton: `conftest.py` (empty), `tests/fixtures/` dir, `tests/benchmarks/` dir

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before any user story can be implemented.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T007 Implement `src/paper_sorts/config.py` — pydantic-settings v2 `Settings` model with four-source priority (CLI args > PDBSEARCH_* env > .env > Fernet-encrypted INI custom source)
- [ ] T008 Implement `src/paper_sorts/logging_config.py` — single `logging.config.dictConfig` with RichHandler (stdout) + optional FileHandler; called once at startup from `cli/app.py`
- [ ] T009 Implement `src/paper_sorts/db/models.py` — SQLAlchemy 2.x declarative ORM models: `Bib`, `Paper`, `Author`, `AuthorPaper` (no DDL FKs on `authors_papers`, no NOT NULL beyond PKs — schema preservation contract)
- [ ] T010 Implement `src/paper_sorts/db/session.py` — `get_engine(db_url: str) -> Engine` and `with_session(engine)` context manager (commit on success, rollback on exception, deterministic close)
- [ ] T011 Implement Alembic scaffold: `migrations/env.py`, `migrations/script.py.mako`, `alembic.ini` referencing `PDBSEARCH_DATABASE_URL` env var
- [ ] T012 Write Alembic revision `migrations/versions/001_initial_schema.py` — creates all four tables from canonical DDL; upgrade detects and renames `bibtext_id` → `bibtex_id` if present; downgrade drops tables; idempotent with IF EXISTS guards
- [ ] T013 Implement `src/paper_sorts/db/repositories.py` — `PaperRepository`, `AuthorRepository`, `BibRepository` with pydantic DTOs (`PaperSummary`, `PaperCreate`); all SQL via SQLAlchemy ORM/Core; no raw strings
- [ ] T014 Implement `src/paper_sorts/cli/prompts.py` — all user-facing prompt functions (`ask_text`, `ask_choice`, `ask_confirmation`, `ask_search_method`); only place that may call `rich.prompt.Prompt.ask`; empty-input re-prompt loop; 1-indexed menus with explicit abort option
- [ ] T015 Write `tests/conftest.py` — `postgresql_proc` + `ephemeral_db_url` fixtures (pytest-postgresql off `/usr/bin/pg_ctl`); `seeded_engine` fixture applying Alembic migrations then inserting `SEED_PAPERS`
- [ ] T016 Write `tests/fixtures/seed_papers.py` — `SEED_PAPERS: list[PaperCreate]` constant with ≥3 papers, ≥2 authors each, covering search-by-title (unique title), search-by-title (ambiguous), search-by-author cases
- [ ] T017 Write `tests/test_config.py` — unit tests for `Settings`: env var override, .env file loading, missing key file error message, valid Fernet source (no live DB required)

**Checkpoint**: Foundation ready — all user story implementation can begin.

---

## Phase 3: User Story 1 — Architecture Document (Priority: P1)

**Goal**: Deliver `docs/architecture.md` so a new contributor can understand the legacy system without reading source.

**Independent Test**: A Python developer reads `docs/architecture.md` and can answer: What do the four tables store? What happens on a partial add failure? How does search-by-author flow end-to-end?

- [ ] T018 [US1] Verify `docs/architecture.md` (written in T001) covers all six areas from FR-001: purpose, user journeys, data model (all four tables + relationships + legacy schema variants), control flow (CLI → DatabaseConnector → PsycopgDB → PostgreSQL), configuration (Fernet INI + argparse), install/run, known limitations (identical author dedup, schema variants, developer-local DB dependency in tests)
- [ ] T019 [US1] Add rollback semantics section to `docs/architecture.md`: document exactly what `rollback_database_addition` does and when it fires (spec acceptance scenario 3)

**Checkpoint**: US1 complete — architecture document is the acceptance reference for US2–US5.

---

## Phase 4: User Story 2 — Modernized Codebase, Same CLI Behaviour (Priority: P1) 🎯 MVP

**Goal**: All five interactive flows work via the rebuilt `src/paper_sorts/` stack against an ephemeral seeded DB.

**Independent Test**: Run `uv run pytest tests/test_cli.py tests/test_repositories.py tests/test_services.py` — all pass with 0 personal DB required.

### Service Layer

- [ ] T020 [US2] Implement `src/paper_sorts/services/paper_service.py` — `search_by_title(session, title) -> list[PaperSummary]`, `search_by_author(session, author) -> list[PaperSummary]`, `add_paper(session, paper: PaperCreate) -> bool`, `update_field(session, bibtex_id, table, column, value) -> None` (match/case with `assert_never` exhaustiveness), `delete_paper(session, bibtex_id) -> bool`
- [ ] T021 [US2] Write `tests/test_repositories.py` — integration tests for `PaperRepository`, `AuthorRepository`, `BibRepository` against ephemeral DB: add, search by title (unique + multi), search by author, update each updatable field, delete; assert seed data seeded correctly; NO mocking SQLAlchemy session
- [ ] T022 [US2] Write `tests/test_services.py` — integration tests for all `paper_service.py` functions against ephemeral DB; cover empty-result case, duplicate bibtex_id rejection, partial-add rollback

### CLI Layer

- [ ] T023 [US2] Implement `src/paper_sorts/cli/search.py` — Typer `search` subcommand; calls `paper_service.search_by_title` or `search_by_author`; disambiguation menu via `prompts.ask_choice` when >1 result; `pretty_print` result via prompts module
- [ ] T024 [US2] Implement `src/paper_sorts/cli/add.py` — Typer `add` subcommand; prompts for author(s), title, bibtex key, bibtex (file or inline via `prompts`), summary; calls `paper_service.add_paper`; no bare `input()` calls
- [ ] T025 [US2] Implement `src/paper_sorts/cli/update.py` — Typer `update` subcommand; search-to-locate then field selection menu; confirmation step with `prompts.ask_confirmation`; calls `paper_service.update_field`
- [ ] T026 [US2] Implement `src/paper_sorts/cli/delete.py` — Typer `delete` subcommand; search-to-locate then display + confirmation; calls `paper_service.delete_paper`
- [ ] T027 [US2] Implement `src/paper_sorts/cli/migrate.py` — Typer `migrate` subcommand; calls `alembic upgrade head` programmatically; subcommand-only (not in interactive menu)
- [ ] T028 [US2] Implement `src/paper_sorts/cli/app.py` — Typer app wiring all subcommands; `pdbsearch` entry point drops into four-option interactive menu when invoked with no subcommand; calls `logging_config.setup_logging()` at startup; `migrate` absent from menu
- [ ] T029 [US2] Register `pdbsearch` as script entry point in `pyproject.toml` pointing to `paper_sorts.cli.app:app`
- [ ] T030 [US2] Write `tests/test_cli.py` — Typer `CliRunner` tests for every subcommand: search (unique result, multi-result disambiguation), add (inline bibtex, file bibtex, abort), update (confirm y, confirm n), delete (confirm y, confirm n), migrate; assert no raw exception on stdout; cover CLI layer ≥ 80% independently

**Checkpoint**: US2 complete — `uv run pdbsearch` works against ephemeral test DB with all five flows.

---

## Phase 5: User Story 3 — Reproducible Test Suite (Priority: P2)

**Goal**: `uv sync --all-extras && uv run pytest` passes on a fresh checkout with no developer-local DB.

**Independent Test**: Delete `~/.pgpass`, any `.env`, any personal `database.crypt`; run `uv run pytest`; all tests pass.

- [ ] T031 [US3] Write `tests/test_migration.py` — integration tests for Alembic migrations: apply revision 001 on empty DB (tables created), apply on DB with `bibtext_id` (typo) variant (renamed correctly), apply on already-migrated DB (idempotent), downgrade restores empty DB
- [ ] T032 [US3] Write `tests/test_doc_currency.py` — reads `README.md` and `CLAUDE.md`, asserts none of the forbidden tokens (`Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB`) appear (constitution G3 gate); this test runs in the default suite, not behind a mark
- [ ] T033 [US3] Update `README.md`: replace all legacy-stack instructions (Poetry, psycopg2, argparse invocation) with modern commands (`uv sync`, `uv run pdbsearch`, `uv run ruff check`, `uv run pytest`); ensure no forbidden tokens remain
- [ ] T034 [US3] Update `CLAUDE.md`: replace legacy architecture section with modern src-layout description (cli/ → services/ → db/ layers, constitution reference updated to v1.3.0-b2-hardened)

**Checkpoint**: US3 complete — fresh-checkout test run passes; doc-currency gate is green.

---

## Phase 6: User Story 4 — One-Shot Migration of Existing Database (Priority: P2)

**Goal**: `pdbsearch migrate` upgrades a personal DB from either legacy schema variant with zero data loss.

**Independent Test**: Seed an ephemeral DB in the legacy schema; run the migrate subcommand; assert row counts match before/after and content spot-checks pass.

- [ ] T035 [US4] Extend `tests/test_migration.py` with data-preservation tests: seed ephemeral DB with `bibtex_id` variant, run migration, assert paper/author/bib/authorship counts match; repeat for `bibtext_id` variant; assert migration is idempotent on second run
- [ ] T036 [US4] Verify `migrations/versions/001_initial_schema.py` handles the `bibtext_id` rename in both `bib` and `papers` tables (written in T012) — add explicit test coverage for this path in T035

**Checkpoint**: US4 complete — migration is tested and verified idempotent for both schema variants.

---

## Phase 7: User Story 5 — Bulk Import from LaTeX/BibTeX (Priority: P3)

**Goal**: `pdbsearch import --tex FILE --bib FILE` imports all matching entries with per-paper commit semantics.

**Independent Test**: Create fixture `tests/fixtures/sample.tex` + `tests/fixtures/sample.bib` with 3 entries (one with no matching bib record); run import; assert 2 papers in DB (third skipped with warning).

- [ ] T037 [US5] Implement `src/paper_sorts/services/import_service.py` — `extract_papers_from_tex_bib(tex_path, bib_path) -> Iterator[PaperCreate]`; uses `pylatexenc` + `pybtex`; skips entries with no matching bib record (logged warning); yields one `PaperCreate` per matched entry
- [ ] T038 [US5] Implement `src/paper_sorts/cli/importer.py` — Typer `import` subcommand (`--tex PATH --bib PATH` required options); calls `import_service.extract_papers_from_tex_bib`, then `paper_service.add_paper` per entry (per-paper commit); skips duplicate bibtex keys with warning; subcommand-only (not in interactive menu)
- [ ] T039 [US5] Create test fixtures: `tests/fixtures/sample.tex` and `tests/fixtures/sample.bib` (3 entries, 1 missing from bib)
- [ ] T040 [US5] Write `tests/test_import.py` — integration tests: full import with fixture files (2 of 3 inserted), duplicate-key skip, missing-bib-record skip with logged warning, partial failure leaves prior entries intact

**Checkpoint**: US5 complete — bulk import works with per-paper commit semantics and correct skip behaviour.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Quality gates, legacy removal, coverage enforcement, and final validation.

- [ ] T041 Remove legacy flat-layout modules: `paper_sorts/add.py`, `paper_sorts/search.py`, `paper_sorts/get_data.py`, `paper_sorts/user_interaction.py`, `paper_sorts/database_connector.py`, `paper_sorts/psycopg_db.py`, `paper_sorts/config_reader.py`, `paper_sorts/helpers.py`, `paper_sorts/run.py`, `paper_sorts/__init__.py`; remove `paper_sorts/` directory (FR-012)
- [ ] T042 Remove legacy test stubs: `tests/test_database_connector.py` (live-DB integration test), `tests/test_user_interaction.py` (always-failing placeholder)
- [ ] T043 [P] Write `tests/benchmarks/bench_baseline.py` — timing harness for all five interactive operations (search-by-title, search-by-author, add, update, delete) against seeded ephemeral DB; writes `tests/benchmarks/baseline.json` on `--record-baseline`; on normal run reads baseline.json and asserts no operation exceeds 2× baseline; marked `@pytest.mark.benchmark` (NOT permanently skipped — constitution G2 gate)
- [ ] T044 [P] Run `uv run ruff check src tests` and `uv run ruff format --check src tests`; fix all findings
- [ ] T045 [P] Run `uv run mypy src`; fix all type errors (strict mode)
- [ ] T046 Run `uv run pytest --cov=src/paper_sorts --cov-report=term-missing`; verify each layer independently ≥ 80% line coverage (persistence: `db/`, service: `services/`, interface: `cli/`, config: `config.py`) — constitution G1 gate; fix gaps
- [ ] T047 Run `uv run pytest` (full suite including benchmark and doc-currency tests); confirm all pass; record final commit hash

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 — BLOCKS all user story phases
- **Phase 3 (US1)**: Depends on T001 (in Phase 1) — can start as soon as architecture doc is drafted
- **Phase 4 (US2)**: Depends on Phase 2 completion (T007–T017)
- **Phase 5 (US3)**: Depends on Phase 4 completion (needs modern CLI for README validation)
- **Phase 6 (US4)**: Depends on T012 (Alembic migration revision) — can partially overlap with Phase 4
- **Phase 7 (US5)**: Depends on Phase 4 completion (needs `paper_service.add_paper`)
- **Phase 8 (Polish)**: Depends on Phases 3–7 all complete

### Within Phase 2 (Foundational)

```
T007 (config) → independent
T008 (logging) → independent
T009 (models) → independent
T010 (session) → T009
T011 (alembic scaffold) → T009, T010
T012 (migration rev 001) → T011
T013 (repositories) → T009, T010, T012 (to know schema)
T014 (prompts) → independent (pure prompt functions)
T015 (conftest) → T010, T012, T013, T016
T016 (seed_papers fixture) → T013 (needs PaperCreate DTO)
T017 (test_config) → T007
```

### Within Phase 4 (US2 — critical path)

```
T020 (paper_service) → T013 (repositories)
T021 (test_repositories) → T015, T016
T022 (test_services) → T015, T016, T020
T023–T026 (CLI subcommands) → T014, T020
T027 (migrate subcommand) → T011
T028 (app.py) → T023–T027
T029 (entry point) → T028
T030 (test_cli.py) → T028, T029, T015, T016
```

### Parallel Opportunities

- T003, T004, T005, T006 in Phase 1 can all run in parallel
- T007, T008, T009, T014 in Phase 2 can all run in parallel (independent files)
- T015, T016, T017 in Phase 2 can run after their respective dependencies
- T023, T024, T025, T026 in Phase 4 can run in parallel (independent subcommand files)
- T021, T022 in Phase 4 can run in parallel with T023–T026 (different files)
- T037, T038 in Phase 7 can run in parallel
- T044, T045, T043 in Phase 8 can run in parallel

---

## Implementation Strategy

### MVP First (US1 + US2 Only)

1. Complete Phase 1: Setup (T001–T006)
2. Complete Phase 2: Foundational (T007–T017)
3. Complete Phase 3: US1 Architecture doc (T018–T019)
4. Complete Phase 4: US2 Modern CLI (T020–T030)
5. **STOP and VALIDATE**: `uv run pdbsearch` interactive menu works; `uv run pytest tests/test_cli.py tests/test_repositories.py tests/test_services.py` passes

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. US1 (T018–T019) → Architecture document deliverable
3. US2 (T020–T030) → Working modern CLI (MVP)
4. US3 (T031–T034) → Clean fresh-checkout test run + doc-currency
5. US4 (T035–T036) → Migration verified for both schema variants
6. US5 (T037–T040) → Bulk import working
7. Polish (T041–T047) → Legacy removed, all gates green

---

## Notes

- [P] = different files, no blocking dependencies — can run in parallel
- [USn] = maps task to user story from spec.md for traceability
- Constitution gates are mechanical and merge-blocking: G1 (per-layer coverage), G2 (benchmark executes), G3 (doc-currency)
- Schema preservation: do NOT add NOT NULL outside PKs, do NOT add FKs to authors_papers, do NOT add indexes
- All prompts must route through `src/paper_sorts/cli/prompts.py` — no bare `input()` elsewhere (constitution III)
- Commit after each phase completion or logical group of tasks
- `migrate` and `import` subcommands are NOT in the four-option interactive menu (admin/scripted operations)
