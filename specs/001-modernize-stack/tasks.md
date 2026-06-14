# Tasks: Modernize the Stack (001-modernize-stack)

**Input**: Design documents from `/specs/001-modernize-stack/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/cli-commands.md

**Organization**: Tasks grouped by user story to enable independent implementation and testing.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization, tool configuration, and build system migration from Poetry/pyproject.toml to uv/hatchling.

- [ ] T001 Rewrite pyproject.toml: replace `[tool.poetry]` with PEP 621 `[project]` metadata (Python ≥ 3.11, hatchling backend, entry point `pdbsearch`); add all runtime deps (SQLAlchemy, Typer, pydantic-settings, Alembic, psycopg[binary], pybtex, pylatexenc, cryptography, rich) and dev deps (pytest, pytest-postgresql, pytest-cov, ruff, mypy) in pyproject.toml
- [ ] T002 [P] Add ruff configuration to pyproject.toml: `[tool.ruff]` with select = ["E","F","I","UP"], target-python = "py311"
- [ ] T003 [P] Add mypy configuration to pyproject.toml: `[tool.mypy]` strict = true, python_version = "3.11"
- [ ] T004 [P] Add pytest configuration to pyproject.toml: `[tool.pytest.ini_options]` testpaths = ["tests"], python_files = ["test_*.py"]
- [ ] T005 Create src-layout skeleton: `src/paper_sorts/__init__.py`, `src/paper_sorts/cli/__init__.py`, `src/paper_sorts/db/__init__.py`, `src/paper_sorts/services/__init__.py`
- [ ] T006 [P] Initialize Alembic environment: create `migrations/` with `env.py`, `script.py.mako`, `alembic.ini`; configure `env.py` to import SQLAlchemy metadata from `src/paper_sorts/db/models.py`
- [ ] T007 Run `uv sync --all-extras` and verify the environment builds cleanly (no import errors)

**Checkpoint**: Build system and project skeleton ready.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented — config, logging, ORM models, session management, Alembic migrations, test fixtures.

**CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T008 Implement `src/paper_sorts/config.py`: pydantic-settings `Settings` class with `PDBSEARCH_DATABASE_URL`, `PDBSEARCH_LOG_LEVEL`; custom `FernetConfigSource` that decrypts legacy INI; four-source priority (CLI > env > .env > Fernet INI)
- [ ] T009 [P] Implement `src/paper_sorts/logging_config.py`: `setup_logging(level: str) -> None` using `logging.config.dictConfig` with RichHandler on stdout and optional FileHandler; called once from `cli/app.py` at startup
- [ ] T010 [P] Implement `src/paper_sorts/db/models.py`: SQLAlchemy 2.x `Base`, `Bib`, `Paper`, `Author`, `AuthorPaper` mapped classes using `Mapped` / `mapped_column`; schema-preservation contract (no NOT NULL outside PKs, no DDL FKs on `authors_papers`)
- [ ] T011 Implement `src/paper_sorts/db/session.py`: `get_engine(database_url: str) -> Engine` and `with_session(engine: Engine) -> Iterator[Session]` context manager (commit on success, rollback on exception); only place that creates SQLAlchemy `Engine`
- [ ] T012 Create Alembic revision 001 `migrations/versions/001_initial_schema.py`: verbatim port of `DatabaseConnector.create_tables()` DDL using `op.create_table()` — creates `bib`, `papers`, `authors_id`, `authors_papers` tables; downgrade drops all four
- [ ] T013 Create Alembic revision 002 `migrations/versions/002_converge_legacy_bibtext_id.py`: detects `bibtext_id` typo column in `papers` and `bib`; renames to `bibtex_id` when present; idempotent (skip if `bibtex_id` already exists); downgrade reverses rename
- [ ] T014 Create `tests/conftest.py`: `postgresql_proc` fixture using pytest-postgresql with `pg_ctl` at `/usr/bin/pg_ctl`; `ephemeral_db_url` fixture that applies Alembic migrations to the ephemeral DB and returns the DSN string
- [ ] T015 [P] Create `tests/fixtures/seed_papers.py`: `SEED_PAPERS` list of `PaperCreate` dicts with at least 3 papers, 4 authors, multiple author-paper links covering edge cases (shared title, shared author, LaTeX accents in bibtex)
- [ ] T016 [P] Write architecture documentation `docs/architecture.md`: reverse-engineer legacy codebase; document purpose, user journeys, data model (four tables with schema), control flow (CLI → UserInteraction → DatabaseConnector → PsycopgDB), config approach, install/run, and known limitations (bibtext_id typo, same-author-name deduplication)

**Checkpoint**: Foundation ready — user story implementation can now begin.

---

## Phase 3: User Story 2 — Modernized Codebase, Same User-Facing Behaviour (Priority: P1)

**Goal**: End user gets the same search/add/update/delete operations against the same personal database; internals rebuilt on SQLAlchemy, Typer, pydantic-settings, ruff.

**Independent Test**: Run `uv run pytest tests/test_repositories.py tests/test_paper_service.py tests/test_cli.py` — all pass against the ephemeral DB. Run `uv run ruff check src` and `uv run mypy src` — both clean.

- [ ] T017 [US2] Implement `src/paper_sorts/db/repositories.py`: Pydantic DTOs `PaperSummary` and `PaperCreate`; `PaperRepository` with `search_by_title(session, title) -> list[PaperSummary]`, `search_by_author(session, author) -> list[PaperSummary]`, `add_paper(session, paper: PaperCreate) -> PaperSummary`, `update_field(session, paper_id: int, table: Literal["papers","bib","authors_id"], field: str, value: str) -> None`, `delete_paper(session, paper_id: int) -> None`; all operations in the session passed from caller (no engine creation here)
- [ ] T018 [US2] Implement `src/paper_sorts/services/paper_service.py`: `search_by_title(engine, title) -> list[PaperSummary]`, `search_by_author(engine, author) -> list[PaperSummary]`, `add_paper(engine, paper: PaperCreate) -> PaperSummary`, `update_field(engine, paper_id: int, table: str, field: str, value: str) -> None`, `delete_paper(engine, paper_id: int) -> None`; each function opens one `with_session()` block; no SQL, no rich, no I/O
- [ ] T019 [P] [US2] Implement `src/paper_sorts/cli/prompts.py`: `ask_nonempty(prompt: str) -> str` (re-prompts on empty), `ask_choice(options: list[str], prompt: str) -> int` (1-indexed, validates range), `ask_confirm(summary: str) -> bool` (accepts 1/y/yes and 2/n/no), `ask_search_method() -> Literal["author","title"]`; all use `rich.prompt.Prompt.ask` — sole importer of `rich.prompt` in `src/`
- [ ] T020 [US2] Implement `src/paper_sorts/cli/search.py`: Typer command `search_cmd(ctx: typer.Context)`; calls `search_by_author` or `search_by_title` via paper_service; displays results with rich; disambiguation menu via `ask_choice` for multiple matches; plain-language error on not-found
- [ ] T021 [P] [US2] Implement `src/paper_sorts/cli/add.py`: Typer command `add_cmd(ctx: typer.Context)`; gathers author(s), title, bibtex_key, bibtex (inline or from file), summary via prompts.py; calls `paper_service.add_paper`; success/failure messages
- [ ] T022 [P] [US2] Implement `src/paper_sorts/cli/update.py`: Typer command `update_cmd(ctx: typer.Context)`; prompts for table, field, identifier, new value; shows confirmation via `ask_confirm`; calls `paper_service.update_field`; plain-language error on failure
- [ ] T023 [P] [US2] Implement `src/paper_sorts/cli/delete.py`: Typer command `delete_cmd(ctx: typer.Context)`; searches for paper first, shows summary, requires `ask_confirm`; calls `paper_service.delete_paper`; plain-language error on failure
- [ ] T024 [US2] Implement `src/paper_sorts/cli/app.py`: `app = typer.Typer()`; register `search`, `add`, `update`, `delete` subcommands; `@app.callback(invoke_without_command=True)` drops into four-option interactive menu when no subcommand; `entry_point()` function as pyproject.toml script target; calls `setup_logging` at startup; reads `Settings` and creates engine
- [ ] T025 [US2] Write `tests/test_repositories.py`: real-DB integration tests for `PaperRepository` — add, search_by_title (one match, zero match, two papers same title), search_by_author (found, not found), update_field (title, contents, bibtex, author), delete_paper; seed with `SEED_PAPERS`; no mocking of session or repositories
- [ ] T026 [P] [US2] Write `tests/test_paper_service.py`: integration tests for `paper_service` functions using ephemeral DB; verify service orchestrates repositories correctly
- [ ] T027 [P] [US2] Write `tests/test_cli.py`: CLI layer tests via Typer `CliRunner`; cover `search`, `add`, `update`, `delete` subcommands and interactive menu; mock user inputs using monkeypatch on prompts; verify plain-language error messages on failure paths
- [ ] T028 [P] [US2] Write `tests/test_config.py`: unit tests for `Settings`: empty input, malformed env, missing key file (Fernet source), .env override, env var override; no DB required

**Checkpoint**: Core CRUD CLI fully functional and tested.

---

## Phase 4: User Story 3 — Reproducible Test Suite (Priority: P2)

**Goal**: `uv sync --all-extras && uv run pytest` succeeds on a fresh checkout with no personal database.

**Independent Test**: Run `uv run pytest` with no `PDBSEARCH_DATABASE_URL` set and no `database.crypt`/`key` files present. All tests pass.

- [ ] T029 [US3] Verify `tests/conftest.py` ephemeral DB setup works without any personal DB: run `uv run pytest tests/test_repositories.py -v` and confirm all pass; fix any fixture issues (postgresql_proc port conflicts, migration failures)
- [ ] T030 [US3] Verify seed data isolation: each test that asserts specific rows must reference `SEED_PAPERS` in a fixture or comment; grep for any hardcoded strings like `"Pino, J."` or `"Wang2021LargeScaleSA"` and replace with seed data references
- [ ] T031 [P] [US3] Remove old test files `tests/test_database_connector.py` and `tests/test_user_interaction.py` (depend on live DB and contain intentional failures); confirm no remaining reference to `database.crypt` or personal DB credentials in test code

**Checkpoint**: Clean test run with zero personal DB dependency.

---

## Phase 5: User Story 4 — One-Shot Migration (Priority: P2)

**Goal**: User runs `pdbsearch migrate` on an existing personal database (either `bibtex_id` or `bibtext_id` schema) and all rows are preserved.

**Independent Test**: Apply revision 001 to ephemeral DB, manually insert `bibtext_id` column, run revision 002, verify column renamed and rows preserved. Re-run migration — must be idempotent.

- [ ] T032 [US4] Implement `src/paper_sorts/cli/migrate.py`: Typer command `migrate_cmd(ctx: typer.Context, revision: str = "head")`; runs `alembic upgrade <revision>` via Alembic's Python API (`alembic.config.Config` + `alembic.command.upgrade`); plain-language success/error output; NOT in interactive menu (admin-only subcommand)
- [ ] T033 [US4] Write migration integration test in `tests/test_repositories.py` (or separate `tests/test_migrations.py`): apply rev 001 to ephemeral DB; insert rows into `bibtext_id` schema; apply rev 002; verify rows exist under `bibtex_id`; re-run rev 002 (idempotency); verify row counts match

**Checkpoint**: Migration command functional and idempotent.

---

## Phase 6: User Story 5 — Bulk Import (Priority: P3)

**Goal**: `pdbsearch import --tex <file> --bib <file>` imports all cited entries from a LaTeX + BibTeX file pair.

**Independent Test**: Run import against `tests/fixtures/` tex+bib fixture. Verify paper count, author count, and bibtex entries in ephemeral DB. Re-run (idempotency via bibtex_key uniqueness). Verify skipped entry with no matching .bib record is logged as warning.

- [ ] T034 [US5] Implement `src/paper_sorts/services/import_service.py`: `extract_papers_from_tex_bib(tex_path: str, bib_path: str) -> Iterator[PaperCreate]`; ports logic from `paper_sorts/helpers.get_data` + `get_bibtex_information` using pybtex and pylatexenc; yields one `PaperCreate` per matched tex+bib entry; logs warning and yields nothing for unmatched citations; no DB access
- [ ] T035 [US5] Implement `src/paper_sorts/cli/importer.py`: Typer command `import_cmd(ctx: typer.Context, tex: Path, bib: Path)`; iterates `import_service.extract_papers_from_tex_bib`; calls `paper_service.add_paper` per-paper (per-paper commit for partial failure safety); skips duplicates (caught via DB unique constraint on bibtex_id); NOT in interactive menu (admin-only subcommand)
- [ ] T036 [P] [US5] Create test fixtures: `tests/fixtures/sample.tex` and `tests/fixtures/sample.bib` with 3+ entries; one citation key in .tex with no matching .bib record; one entry with LaTeX accents in title/author
- [ ] T037 [P] [US5] Write `tests/test_import_service.py`: unit tests for `extract_papers_from_tex_bib` against fixture files; verify correct PaperCreate objects yielded; verify missing .bib entry is skipped; no DB required
- [ ] T038 [US5] Write import CLI integration test in `tests/test_cli.py` (add section): run `pdbsearch import --tex sample.tex --bib sample.bib` via CliRunner against ephemeral DB; verify papers present; re-run (idempotent)

**Checkpoint**: Bulk import functional and idempotent.

---

## Phase 7: Performance Baseline Benchmark (Principle IV Gate)

**Purpose**: The constitution requires an *executing* baseline benchmark. This phase creates it.

- [ ] T039 Create `tests/benchmarks/bench_baseline.py`: uses `timeit` to measure wall-clock time for search_by_title, search_by_author, add_paper, update_field, delete_paper against the seeded ephemeral DB; writes results to `tests/benchmarks/baseline.json`; NOT permanently `@pytest.mark.skip`'d — must execute when invoked via `uv run pytest tests/benchmarks/`
- [ ] T040 [P] Create `tests/benchmarks/__init__.py` (empty) and `tests/benchmarks/baseline.json` placeholder (`{}`)

**Checkpoint**: `uv run pytest tests/benchmarks/` executes and writes baseline.json.

---

## Phase 8: Legacy Removal & Doc Cleanup

**Purpose**: Remove the flat-layout `paper_sorts/` package and update docs so no legacy-stack tokens remain (Principle I doc-currency gate).

- [ ] T041 Delete legacy flat-layout directory `paper_sorts/` (all files: `__init__.py`, `add.py`, `config_reader.py`, `database_connector.py`, `get_data.py`, `helpers.py`, `psycopg_db.py`, `run.py`, `search.py`, `user_interaction.py`)
- [ ] T042 [P] Update `README.md`: replace Poetry commands with uv; replace `python paper_sorts/run.py` with `uv run pdbsearch`; replace pylint with ruff; remove references to `database.crypt`/`key` as the only config option; describe all four config sources; verify forbidden tokens (`Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB`) are absent
- [ ] T043 [P] Update `CLAUDE.md`: reflect new src-layout, uv commands, constitution v1.3.0-b2-hardened, new architecture layers (cli/, services/, db/); verify forbidden tokens absent
- [ ] T044 Run full quality gate: `uv run ruff check src tests`, `uv run mypy src`, `uv run pytest` — all must pass; fix any issues

**Checkpoint**: Legacy code removed; quality gates green; no legacy tokens in docs.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 completion — BLOCKS all user stories
- **US2 (Phase 3)**: Depends on Phase 2 completion
- **US3 (Phase 4)**: Depends on Phase 3 completion (tests reference the new test suite)
- **US4 (Phase 5)**: Depends on Phase 2 completion (Alembic env); can run parallel to US3
- **US5 (Phase 6)**: Depends on Phase 3 (paper_service.add_paper); can start after T018
- **Benchmark (Phase 7)**: Depends on Phase 3 completion
- **Cleanup (Phase 8)**: Depends on all prior phases

### Within Each Phase

- Tasks marked [P] within a phase can run in parallel
- Models/DTOs before services; services before CLI commands
- Tests can be written in parallel with implementation

### Parallel Opportunities Within Phase 3 (US2)

```bash
# After T017 (repositories) completes:
Task T018: paper_service.py
Task T028: test_config.py (no service dep)

# After T018 (paper_service) completes:
Task T019: prompts.py     # no service dep, can be parallel
Task T020: cli/search.py
Task T021: cli/add.py
Task T022: cli/update.py
Task T023: cli/delete.py
Task T025: test_repositories.py
Task T026: test_paper_service.py
Task T027: test_cli.py
```

---

## Implementation Strategy

### MVP First

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL)
3. Complete Phase 3: US2 (core CRUD)
4. **STOP and VALIDATE**: `uv run pytest && uv run ruff check src && uv run mypy src`

### Incremental Delivery

1. Setup + Foundational → skeleton ready
2. US2 → CRUD CLI working, tests passing (MVP)
3. US3 → clean test run without personal DB
4. US4 → migration command working
5. US5 → bulk import working
6. Phase 7 → benchmark baseline recorded
7. Phase 8 → legacy removed, docs clean, all gates green

---

## Notes

- [P] tasks = different files, no inter-task dependencies within phase
- Schema-preservation contract: do NOT add NOT NULL outside PKs, do NOT add DDL FKs to `authors_papers`
- All prompts MUST route through `src/paper_sorts/cli/prompts.py` (Principle III)
- `sqlalchemy` import MUST stay isolated to `src/paper_sorts/db/` (Principle I)
- Benchmark (T039) MUST NOT be `@pytest.mark.skip`'d — Principle IV gate
- After T041 (legacy removal), forbidden tokens (`Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB`) must not appear in README.md or CLAUDE.md
- Commit after each phase or logical group of tasks
