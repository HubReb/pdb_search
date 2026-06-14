# Tasks: Modernize the Stack

**Input**: Design documents from `specs/001-modernize-stack/`  
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/cli-commands.md

**Organization**: Tasks are grouped by user story to enable independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: Which user story this task belongs to (US1–US5)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Replace legacy tooling, create src-layout scaffold, wire Alembic.

- [ ] T001 Replace `pyproject.toml` with PEP 621 / hatchling / uv layout; add all runtime + dev deps (SQLAlchemy 2.x, psycopg[binary], Typer, Alembic, pydantic-settings, pybtex, pylatexenc, cryptography, rich, pytest, pytest-postgresql, pytest-cov, mypy, ruff) in `pyproject.toml`
- [ ] T002 Create `src/paper_sorts/__init__.py`, `src/paper_sorts/cli/__init__.py`, `src/paper_sorts/services/__init__.py`, `src/paper_sorts/db/__init__.py` (empty packages, src-layout skeleton)
- [ ] T003 [P] Configure ruff (`[tool.ruff]`, `[tool.ruff.lint]`) and mypy (`[tool.mypy]`) in `pyproject.toml`; set `src` layout paths
- [ ] T004 [P] Configure pytest (`[tool.pytest.ini_options]`) in `pyproject.toml`; set `testpaths = ["tests"]`, `pythonpath = ["src"]`
- [ ] T005 Initialise Alembic: run `alembic init migrations` to create `migrations/env.py`, `migrations/script.py.mako`, `alembic.ini`; update `env.py` to read DB URL from `paper_sorts.config.Settings` and import ORM metadata

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before any user story can be implemented.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T006 Implement `src/paper_sorts/db/models.py`: four `DeclarativeBase` ORM models (`Paper`, `Bib`, `Author`, `AuthorPaper`) mapping to `papers`, `bib`, `authors_id`, `authors_papers`; honour schema-preservation contract (no new NOT NULL outside PKs, no DDL FK on `authors_papers`)
- [ ] T007 Implement `src/paper_sorts/db/session.py`: `get_engine(url: str) -> Engine` factory and `with_session(url: str)` context manager (commit on success, rollback on exception, close deterministically)
- [ ] T008 Implement `src/paper_sorts/db/repositories.py`: `PaperCreate` and `PaperSummary` Pydantic DTOs; `PaperRepository` (search_by_title, search_by_author, add, delete), `AuthorRepository` (find_or_create, update_name), `BibRepository` (add, update, find_by_id)
- [ ] T009 Implement `src/paper_sorts/config.py`: `Settings(BaseSettings)` with `db_host`, `db_port`, `db_name`, `db_user`, `db_password`, `log_level`, `config_file`, `key_file`; env prefix `PDBSEARCH_`; custom `FernetIniSettingsSource` that decrypts the legacy encrypted INI; `database_url` property returning SQLAlchemy URL; priority: CLI flags > env > `.env` > Fernet INI
- [ ] T010 Implement `src/paper_sorts/logging_config.py`: `configure_logging(level: str) -> None` using `logging.config.dictConfig`; RichHandler to stdout + optional FileHandler; called once from `cli/app.py` at startup
- [ ] T011 Implement `src/paper_sorts/cli/prompts.py`: all user-facing prompt helpers (`ask_nonempty`, `ask_choice`, `ask_confirm`, `ask_menu`) wrapping `rich.prompt.Prompt.ask`; enforce non-empty re-prompt, 1-indexed display, confirmation dual-form acceptance; this is the ONLY place in `src/paper_sorts/` that imports `rich.prompt`

**Checkpoint**: Persistence layer, config, logging, and prompt primitives are complete. User story work can begin.

---

## Phase 3: User Story 1 — Architecture Document (Priority: P1)

**Goal**: Produce `docs/architecture.md` documenting the legacy stack so modernization is grounded.

**Independent Test**: A Python developer can answer "What does it do? What is the data model? Where would I add a new field?" after reading only `docs/architecture.md`.

- [ ] T012 [US1] Write `docs/architecture.md`: purpose; user journeys (search by author, search by title, add, update, delete, bulk import); four-table data model (schema, relationships, bibtex_id vs bibtext_id variants); control flow diagram (CLI → DatabaseConnector → PsycopgDB → PostgreSQL); config approach (Fernet INI); rollback semantics; install/run instructions; known limitations (duplicate author name treatment, per-class log files, personal-DB test dependency)

**Checkpoint**: Architecture document complete. US1 acceptance scenarios satisfied.

---

## Phase 4: User Story 2 — Modernized Codebase, Same Behaviour (Priority: P1) 🎯 MVP

**Goal**: Full CLI rebuilt on modern stack; all existing flows work identically.

**Independent Test**: Run scripted dialog through every CLI path (search by title/author, add inline/bib, update each field, abort update, delete, quit) against a seeded test DB. All paths produce equivalent or improved output.

### Implementation

- [ ] T013 [US2] Implement `src/paper_sorts/services/paper_service.py`: `search_by_title(session, title) -> list[PaperSummary]`, `search_by_author(session, author) -> list[PaperSummary]`, `add_paper(session, paper: PaperCreate) -> bool`, `update_field(session, table: Literal["papers","bib","authors_id"], column: str, identifier: str|int, value: str) -> None` using `match/case` with `assert_never`, `delete_paper(session, bibtex_id: str) -> bool`; pure orchestration (no SQL, no rich, no I/O)
- [ ] T014 [US2] Implement `src/paper_sorts/cli/search.py`: Typer `search` subcommand; prompts for method (author/title) via `prompts.ask_menu`; calls `paper_service.search_by_title` / `search_by_author`; pretty-prints results; handles disambiguation when multiple papers returned
- [ ] T015 [US2] Implement `src/paper_sorts/cli/add.py`: Typer `add` subcommand; prompts for authors, title, BibTeX key, BibTeX entry (inline or from `.bib` file), summary; calls `paper_service.add_paper`; all prompts via `cli/prompts.py`
- [ ] T016 [US2] Implement `src/paper_sorts/cli/update.py`: Typer `update` subcommand; prompts for table → column → identifier → new value → confirmation (dual-form); calls `paper_service.update_field`; aborts cleanly on `n`/`no`/`2`
- [ ] T017 [US2] Implement `src/paper_sorts/cli/delete.py`: Typer `delete` subcommand; prompts for BibTeX key; shows paper summary; confirmation step before `paper_service.delete_paper`
- [ ] T018 [US2] Implement `src/paper_sorts/cli/migrate.py`: Typer `migrate` subcommand; calls `alembic upgrade head` programmatically; non-interactive
- [ ] T019 [US2] Implement `src/paper_sorts/cli/app.py`: Typer root app; registers `search`, `add`, `update`, `delete`, `import`, `migrate` subcommands; when invoked with no subcommand drops into four-option interactive menu (search/add/update/quit) via `prompts.ask_menu`; calls `configure_logging` and resolves `Settings` at startup; `pdbsearch` console script entry point
- [ ] T020 [US2] Write integration tests in `tests/test_services.py`: test `search_by_title`, `search_by_author`, `add_paper`, `update_field` (title, contents, bibtex, author), `delete_paper` against ephemeral DB seeded from `tests/fixtures/seed_papers.py`; real DB only, no mocking
- [ ] T021 [US2] Write CLI tests in `tests/test_cli.py`: test every subcommand via Typer's `CliRunner`; covers success paths, abort paths, empty-input re-prompt, invalid menu choice re-prompt
- [ ] T022 [US2] Write persistence tests in `tests/test_repositories.py`: test all repository methods (CRUD, search, disambiguation) against ephemeral DB; assert on seeded row content referencing `seed_papers.py`

**Checkpoint**: Full CLI is functional. All US2 acceptance scenarios pass.

---

## Phase 5: User Story 3 — Reproducible Test Suite (Priority: P2)

**Goal**: `git clone && uv sync --all-extras && uv run pytest` passes on a machine with no personal DB.

**Independent Test**: Run `uv run pytest` on a clean clone. All tests pass without `database.crypt` or `key` file present.

- [ ] T023 [US3] Create `tests/conftest.py`: `postgresql_proc` fixture (host pg_ctl at `/usr/bin/pg_ctl`); `ephemeral_db_url` session-scope fixture; `db_session` fixture that applies Alembic migrations then seeds from `SEED_PAPERS`; tears down after session
- [ ] T024 [US3] Create `tests/fixtures/seed_papers.py`: define `SEED_PAPERS` as a list of `PaperCreate` objects (at least 3 papers, covering: single-author, multi-author, and a paper with LaTeX accents in BibTeX); document which rows each test assertion references

**Checkpoint**: `uv run pytest` passes on a fresh checkout. US3 acceptance scenarios satisfied.

---

## Phase 6: User Story 4 — One-Shot Migration (Priority: P2)

**Goal**: `pdbsearch migrate` upgrades a personal database from either historical schema to canonical schema with zero data loss.

**Independent Test**: Seed an ephemeral DB in the legacy schema (`bibtext_id` typo). Run migration. Assert all paper/author/bib row counts match and content is unchanged.

- [ ] T025 [US4] Write Alembic revision `migrations/versions/001_initial_schema.py`: creates canonical four-table schema (`bibtex_id` column, no DDL FKs on `authors_papers`); `upgrade()` and `downgrade()` implemented
- [ ] T026 [US4] Write Alembic revision `migrations/versions/002_fix_bibtext_typo.py`: `upgrade()` checks whether `bibtext_id` column exists on `papers` and `bib`; if so renames to `bibtex_id`; idempotent; `downgrade()` reverses; also checks `bib` table column name `bibtext` vs `bibtex`
- [ ] T027 [US4] Write migration tests in `tests/test_migrations.py`: test revision 001 from empty DB; test revision 002 from legacy-typo schema; assert idempotency (run migrate twice, row counts unchanged); test that `pdbsearch migrate` subcommand completes without error via `CliRunner`

**Checkpoint**: Migration works on both historical schema variants. US4 acceptance scenarios pass.

---

## Phase 7: User Story 5 — Bulk Import (Priority: P3)

**Goal**: `pdbsearch import --tex FILE --bib FILE` inserts all cited entries; partial failure leaves committed rows; missing bib records logged + skipped.

**Independent Test**: Run `pdbsearch import` against fixture `.tex` + `.bib` pair with N entries. Assert N papers, authors, and BibTeX entries in DB after import.

- [ ] T028 [P] [US5] Create test fixtures `tests/fixtures/literature_overview.tex` and `tests/fixtures/bib.bib` with at least 3 entries (1 missing from .bib to test skip-with-warning path)
- [ ] T029 [US5] Implement `src/paper_sorts/services/import_service.py`: `extract_papers_from_tex_bib(tex_path: Path, bib_path: Path) -> Iterator[PaperCreate]`; ports `get_data()` + `get_bibtex_information()` logic from legacy `helpers.py` / `get_data.py` using pylatexenc + pybtex; skips entries with no matching bib record (logs warning); yields `PaperCreate` objects one at a time
- [ ] T030 [US5] Implement `src/paper_sorts/cli/importer.py`: Typer `import` subcommand with `--tex` and `--bib` options; calls `import_service.extract_papers_from_tex_bib`; adds each paper via `paper_service.add_paper` with per-paper commit; non-interactive; prints progress and summary
- [ ] T031 [US5] Write import tests in `tests/test_import.py`: test full import of fixture pair; test skip-and-continue on missing bib entry; test idempotency (re-import skips duplicates); assert per-paper commit by verifying partial import state after simulated failure

**Checkpoint**: Bulk import works end-to-end. US5 acceptance scenarios pass.

---

## Phase 8: Remove Legacy Code & Documentation Polish

**Purpose**: Delete superseded flat-layout modules, update docs for doc-currency gate (Principle I).

- [ ] T032 Delete legacy flat-layout modules: `paper_sorts/add.py`, `paper_sorts/search.py`, `paper_sorts/get_data.py`, `paper_sorts/psycopg_db.py`, `paper_sorts/user_interaction.py`, `paper_sorts/database_connector.py`, `paper_sorts/config_reader.py`, `paper_sorts/helpers.py`, `paper_sorts/__init__.py`, `paper_sorts/run.py`; delete `paper_sorts/` directory
- [ ] T033 Delete legacy tests: `tests/test_database_connector.py`, `tests/test_user_interaction.py` (replaced by new test suite)
- [ ] T034 [P] Update `README.md`: replace all legacy-stack references (Poetry, psycopg2, UserInteraction, PsycopgDB, `python paper_sorts/run.py`) with modernized stack; include `uv sync`, `uv run pdbsearch`, `uv run pytest` commands
- [ ] T035 [P] Update `CLAUDE.md`: remove legacy architecture section; add modernized architecture description matching `src/paper_sorts/` layout; update Commands section to `uv run pdbsearch`; ensure forbidden tokens (Poetry, psycopg2, UserInteraction, PsycopgDB) are absent
- [ ] T036 Write `tests/test_doc_currency.py`: mechanical test that `README.md` and `CLAUDE.md` do NOT contain `Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB` (enforces constitution Principle I doc-currency gate)
- [ ] T037 Write `tests/test_config.py`: unit tests for `Settings` (env var loading, `.env` loading, missing required field error, Fernet source with missing key file error, database_url property)

---

## Phase 9: Benchmark Harness & Green Build

**Purpose**: Satisfy constitution Principle IV baseline-benchmark gate; confirm all quality gates pass.

- [ ] T038 Write `tests/benchmarks/bench_baseline.py`: wall-clock timing tests for search-by-title, search-by-author, add, update, delete against seeded ephemeral DB; records results to `tests/benchmarks/baseline.json`; MUST execute successfully (not skipped); annotate with `@pytest.mark.benchmark` or use `time.perf_counter` and assert reasonable bound
- [ ] T039 Write `tests/benchmarks/baseline.json`: initial recorded baseline results (generated by running T038 once)
- [ ] T040 Run `uv run ruff check src tests` — fix all violations
- [ ] T041 Run `uv run mypy src` — fix all type errors
- [ ] T042 Run `uv run pytest --cov=src/paper_sorts --cov-report=term-missing` — confirm per-layer coverage ≥ 80% for `db/`, `services/`, `cli/`, `config.py`; fix any gaps

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 — BLOCKS all user story phases
- **Phase 3 (US1 — Architecture doc)**: Depends on Phase 1 only (no code required)
- **Phase 4 (US2 — Modern CLI)**: Depends on Phase 2 — core deliverable
- **Phase 5 (US3 — Test suite)**: Depends on Phase 2 (conftest needs session + fixtures)
- **Phase 6 (US4 — Migration)**: Depends on Phase 2 (needs models for migration env)
- **Phase 7 (US5 — Bulk import)**: Depends on Phase 4 (paper_service.add_paper needed)
- **Phase 8 (Remove legacy)**: Depends on Phases 4, 5, 6, 7 all complete
- **Phase 9 (Green build)**: Depends on Phase 8

### User Story Dependencies

- **US1 (Architecture doc)**: Independent — can proceed in parallel with Phase 2
- **US2 (Modern CLI)**: Depends on Phase 2 foundational; blocks US5
- **US3 (Test suite)**: Depends on Phase 2; conftest + fixtures feed US2 tests
- **US4 (Migration)**: Depends on Phase 2 (ORM models); independent of US2
- **US5 (Bulk import)**: Depends on US2 (paper_service)

### Within Each Phase

- Models (T006) before repositories (T008)
- Session (T007) before repositories (T008)
- Repositories before services (T013)
- Services before CLI commands (T014–T019)
- All foundational (Phase 2) before tests that use the DB

### Parallel Opportunities

- T003 and T004 (ruff/mypy config, pytest config) — parallel
- T006 and T009 and T010 and T011 (models, config, logging, prompts) — parallel within Phase 2 after T007
- T013 (paper_service) and T025–T026 (Alembic revisions) — parallel after Phase 2
- T014, T015, T016, T017, T018 (CLI subcommands) — parallel after T013
- T028 (import fixtures) — parallel with US2 work
- T034 and T035 (README, CLAUDE.md) — parallel

---

## Parallel Example: Phase 4 (US2 CLI)

```bash
# After T013 (paper_service) is done, launch all CLI subcommands in parallel:
Task T014: "Implement cli/search.py"
Task T015: "Implement cli/add.py"
Task T016: "Implement cli/update.py"
Task T017: "Implement cli/delete.py"
Task T018: "Implement cli/migrate.py"
# Then T019 (app.py) wires them all together
```

---

## Implementation Strategy

### MVP First (US1 + US2)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL)
3. Complete Phase 3: US1 Architecture doc (quick win)
4. Complete Phase 4: US2 Modern CLI
5. **STOP and VALIDATE**: Run scripted dialog through every CLI path

### Incremental Delivery

1. Setup + Foundational → skeleton ready
2. US1 → architecture documented
3. US2 → full CLI rebuilt (MVP!)
4. US3 → test suite self-contained
5. US4 → migration command works
6. US5 → bulk import works
7. Remove legacy → clean repo
8. Green build → all gates pass

---

## Notes

- **Schema preservation**: Never add NOT NULL outside PKs, DDL FKs on `authors_papers`, or extra indexes
- **Layer isolation**: `db/` only imports sqlalchemy; `services/` only uses DTOs; `cli/` calls services
- **Prompt routing**: Every `input()` / prompt must go through `cli/prompts.py` (constitution Principle III)
- **Real DB tests**: Never mock the SQLAlchemy session or repositories (constitution Principle II)
- **Benchmark must run**: `tests/benchmarks/bench_baseline.py` must execute, not be permanently skipped (constitution Principle IV gate)
- **Doc-currency**: After T032, `README.md` and `CLAUDE.md` must not contain `Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB`
