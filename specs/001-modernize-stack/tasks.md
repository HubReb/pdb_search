# Tasks: Modernize the Stack

**Input**: Design documents from `/specs/001-modernize-stack/`
**Prerequisites**: plan.md ✅, spec.md ✅, research.md ✅, data-model.md ✅, contracts/cli-commands.md ✅, quickstart.md ✅

**Organization**: Tasks are grouped by user story (US1–US5) enabling independent implementation and testing. All tests are included because the constitution mandates a real-DB integration test suite and per-layer coverage gates.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no shared-state dependencies)
- **[Story]**: Which user story this task belongs to (US1–US5)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Replace Poetry/pyproject.toml, establish src-layout skeleton, configure toolchain.

- [ ] T001 Rewrite `pyproject.toml` at repo root: switch build backend to hatchling, declare `[project]` metadata (name=paper-sorts, python≥3.11), add all runtime deps (sqlalchemy[psycopg], psycopg[binary], typer[all], pydantic-settings, alembic, pybtex, pylatexenc, cryptography, rich), add all dev/test deps (pytest, pytest-postgresql, pytest-cov, ruff, mypy, pytest-benchmark), declare entry point `pdbsearch = "paper_sorts.cli.app:app"`, and run `uv sync --all-extras`
- [ ] T002 [P] Create `src/paper_sorts/__init__.py` (empty, package marker), `src/paper_sorts/cli/__init__.py`, `src/paper_sorts/services/__init__.py`, `src/paper_sorts/db/__init__.py` — establish the four-layer src-layout package skeleton
- [ ] T003 [P] Add `ruff.toml` (or `[tool.ruff]` in `pyproject.toml`) with `target-version="py311"`, `line-length=100`, select `["E","F","I","UP"]`, and `[tool.mypy]` with `strict=true`, `python_version="3.11"`, `mypy_path="src"`
- [ ] T004 [P] Initialize Alembic: run `uv run alembic init migrations` from repo root to create `migrations/env.py`, `migrations/script.py.mako`, `alembic.ini`; update `alembic.ini` to set `script_location = migrations` and configure `sqlalchemy.url` from env var `PDBSEARCH_DATABASE_URL`; update `migrations/env.py` to import `Base.metadata` from `paper_sorts.db.models` for `target_metadata`
- [ ] T005 Create `tests/` directory structure: `tests/__init__.py`, `tests/fixtures/__init__.py`, `tests/benchmarks/__init__.py`; add `pytest.ini` or `[tool.pytest.ini_options]` in `pyproject.toml` with `testpaths=["tests"]`, `addopts="--tb=short"`, and `[tool.coverage.run]` with `source=["src/paper_sorts"]`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core DB layer, config, and logging — all user stories depend on these.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T006 Create `src/paper_sorts/db/models.py`: define SQLAlchemy 2.x `Base = DeclarativeBase()` and four ORM models with full type hints and docstrings: `Bib(bibtex_id: Mapped[str] PK, bibtex: Mapped[str] unique)`, `Paper(id: Mapped[int] PK, title: Mapped[str|None], contents: Mapped[str|None], bibtex_id: Mapped[str|None] FK→bib.bibtex_id)`, `Author(id: Mapped[int] PK, author: Mapped[str|None])`, `AuthorPaper(id: Mapped[int] PK, author_id: Mapped[int|None], paper_id: Mapped[int|None])` — NO NOT NULL outside PKs, NO FKs on `authors_papers`, NO extra indexes per schema preservation rule
- [ ] T007 Create `src/paper_sorts/db/session.py`: define `get_engine(database_url: str) -> Engine` and `with_session(database_url: str)` context manager using `with Session(engine) as session: yield session; session.commit()` with rollback on exception; import only from `sqlalchemy` (not from other app modules); add full docstrings and type hints
- [ ] T008 Create `src/paper_sorts/config.py`: define pydantic-settings v2 `Settings` model with fields `database_url: str`, `log_level: str = "INFO"`, `config_file: str | None = None`, `key_file: str | None = None`; add `model_config = SettingsConfigDict(env_prefix="PDBSEARCH_", env_file=".env")`; implement `FernetConfigSource(PydanticBaseSettingsSource)` that reads the encrypted INI when `config_file` and `key_file` are both set, decrypts with `cryptography.fernet.Fernet`, parses with `configparser.ConfigParser`, and returns `{"database_url": "postgresql+psycopg://..."}` from the `[postgresql]` section; full docstrings and type hints
- [ ] T009 [P] Create `src/paper_sorts/logging_config.py`: define `setup_logging(log_level: str = "INFO") -> None` using `logging.config.dictConfig` with a `RichHandler` handler to stdout and an optional `FileHandler` if `PDBSEARCH_LOG_FILE` env var is set; full docstrings
- [ ] T010 Create `migrations/versions/001_initial_schema.py`: Alembic revision that in `upgrade()` creates `authors_papers(id SERIAL PK, author_id INT, paper_id INT)`, `authors_id(id SERIAL PK, author TEXT)`, `bib(bibtex_id TEXT PK, bibtex TEXT UNIQUE)`, `papers(id SERIAL PK, title TEXT, contents TEXT, bibtex_id TEXT, FK→bib.bibtex_id)` — verbatim port of `DatabaseConnector.create_tables()` DDL; `downgrade()` drops all four tables in reverse FK order; full docstring
- [ ] T011 Create `migrations/versions/002_fix_bibtext_typo.py`: Alembic revision that in `upgrade()` conditionally renames column `bibtext_id` → `bibtex_id` in `bib` table (using `op.execute("ALTER TABLE bib RENAME COLUMN bibtext_id TO bibtex_id")` guarded by a check whether the column exists), and same for `papers` table; `downgrade()` reverses the renames; idempotent via `IF EXISTS` pattern; full docstring
- [ ] T012 Create `tests/conftest.py` with session-scoped `postgresql_proc` fixture (pytest-postgresql, pointing at `/usr/bin/pg_ctl`), `ephemeral_db_url` fixture that constructs the SQLAlchemy-compatible URL `postgresql+psycopg://...`, and `db_session` fixture that runs `alembic upgrade head` against the ephemeral URL then yields a `Session`; add `tests/fixtures/seed_papers.py` defining `SEED_PAPERS: list[PaperCreate]` with ≥3 papers covering: (a) a paper with multiple authors including author "Pino, J.", bibtex key "Wang2021LargeScaleSA", title containing "Large-Scale"; (b) a paper with title "Direct speech-to-speech translation with discrete units" with its full author list; (c) a minimal test paper with one author

**Checkpoint**: Foundation complete — sessions, models, migrations, config, seed fixture all available.

---

## Phase 3: User Story 1 — Architecture Document (Priority: P1)

**Goal**: Produce `docs/architecture.md` capturing the pre-modernization codebase purpose, user journeys, data model, control flow, configuration approach, install/run instructions, and known limitations.

**Independent Test**: A reader can answer "What does it do? What is the data model? Where would I add a new field?" from the document alone, without opening source.

- [ ] T013 [US1] Create `docs/architecture.md` describing the **pre-modernization** legacy codebase: (a) purpose and scope; (b) the five user journeys (search by author, search by title, add inline, add from .bib, bulk import, update, delete); (c) data model — four tables with column names, FK relationships, the `bibtex_id` vs `bibtext_id` dual-variant note; (d) control flow from user prompt to `UserInteraction` → `DatabaseConnector` → `PsycopgDB` → PostgreSQL and back; (e) configuration — Fernet-encrypted INI, `ConfigReader`, how credentials are loaded; (f) rollback semantics — describe what `rollback_database_addition` does and when partial writes are cleaned up; (g) install/run instructions for the legacy stack; (h) known limitations (identical author deduplication, no CI-friendly test suite, developer-local DB dependency)

**Checkpoint**: US1 complete — architecture document present and readable.

---

## Phase 4: User Story 2 — Modernized Codebase, Same Behaviour (Priority: P1) 🎯 MVP

**Goal**: Replace legacy procedural modules with the modern four-layer stack; all existing CLI flows preserved.

**Independent Test**: `uv run pdbsearch --help` shows all subcommands; interactive mode shows four-option menu; `uv run pytest tests/test_repositories.py tests/test_services.py tests/test_cli.py` all pass green.

### DB Repository Layer (US2)

- [ ] T014 [US2] Create `src/paper_sorts/db/repositories.py`: define pydantic `PaperCreate(BaseModel)` with fields `title: str`, `contents: str`, `bibtex_id: str`, `bibtex: str`, `authors: list[str]` and `PaperSummary(BaseModel)` with fields `paper_id: int`, `title: str`, `contents: str`, `bibtex_id: str`, `bibtex: str`, `authors: str`; define `BibRepository` with `add(session, bibtex_id, bibtex)`, `get(session, bibtex_id) -> str | None`, `update(session, bibtex_id, new_bibtex) -> None`; `AuthorRepository` with `get_or_create(session, name: str) -> int` (returns author_id), `link_to_paper(session, author_id: int, paper_id: int) -> None`, `unlink_from_paper(session, author_id: int, paper_id: int) -> None`, `cleanup_orphan(session, author_id: int) -> None`; `PaperRepository` with `add(session, paper: PaperCreate) -> int`, `get_by_title(session, title: str) -> list[PaperSummary]`, `get_by_author(session, author: str) -> list[PaperSummary]`, `delete(session, paper_id: int) -> None`, `update_field(session, table: Literal["papers","bib","authors_id"], column: str, identifier: str | int, value: str) -> None` with `assert_never` for exhaustiveness; all methods have full docstrings and type hints; ONLY `sqlalchemy` imports in this file
- [ ] T015 [P] [US2] Write integration tests in `tests/test_repositories.py` using the `db_session` fixture and `SEED_PAPERS`: test `get_by_title` (exact match, no match), `get_by_author` (found, not found), `add` (new paper appears in subsequent search), `delete` (paper removed, authors cleaned up), `update_field` for each supported column/table combination, duplicate bibtex key raises appropriate error; seed database from `SEED_PAPERS` at test start

### Service Layer (US2)

- [ ] T016 [US2] Create `src/paper_sorts/services/paper_service.py`: implement `search_by_title(db_url: str, title: str) -> list[PaperSummary]`, `search_by_author(db_url: str, author: str) -> list[PaperSummary]`, `add_paper(db_url: str, paper: PaperCreate) -> None`, `update_field(db_url: str, table: Literal["papers","bib","authors_id"], column: str, identifier: str | int, value: str) -> None`, `delete_paper(db_url: str, paper_id: int) -> None`; each function uses `with_session(db_url)` and calls repository methods; NO sqlalchemy imports, NO rich, NO I/O; full docstrings and type hints
- [ ] T017 [P] [US2] Write integration tests in `tests/test_services.py` using `ephemeral_db_url` and `SEED_PAPERS`: test each service function end-to-end against the real ephemeral DB (seed, call service, assert on return value or subsequent query); include error path tests (search returns empty list, update unknown table raises ValueError)

### CLI Layer (US2)

- [ ] T018 [US2] Create `src/paper_sorts/cli/prompts.py`: implement `ask_text(prompt: str) -> str` (re-prompts on empty input), `ask_choice(prompt: str, options: list[str]) -> int` (1-indexed, re-prompts on out-of-range), `ask_confirm(prompt: str) -> bool` (accepts 1/y/yes for True, 2/n/no for False), `ask_file(prompt: str) -> str` (prompts until a file path is given); all using `rich.prompt.Prompt.ask` or `input` only inside this module; full docstrings and type hints; this is the ONLY module in `src/paper_sorts/` permitted to import `rich.prompt`
- [ ] T019 [P] [US2] Write unit tests in `tests/test_prompts.py` using `monkeypatch` to stub `input`: test `ask_text` with empty first input then valid input (re-prompts), `ask_choice` with out-of-range then valid (re-prompts), `ask_confirm` with each accepted form (1, y, yes, 2, n, no), `ask_file` with non-existent then existing file
- [ ] T020 [US2] Create `src/paper_sorts/cli/app.py`: define `app = typer.Typer(name="pdbsearch")`, declare global options `--database-url`, `--log-level`, `--config`, `--key` via a Typer callback; call `setup_logging()` in the callback; when invoked with no subcommand, drop into the four-option interactive menu using prompts from `cli/prompts.py` calling `search`/`add`/`update`/`delete` subcommand functions; register subcommands `search`, `add`, `update`, `delete`, `import` (from `importer`), `migrate`
- [ ] T021 [US2] Create `src/paper_sorts/cli/search.py`: implement `search_callback(db_url: str)` that asks user to pick "Search by author" / "Search by paper title" / abort via `ask_choice`, then calls `paper_service.search_by_author` or `paper_service.search_by_title`, displays disambiguation list if >1 result via `ask_choice`, pretty-prints the chosen result as `title: {title}\nauthors: {authors}\nsummary: {contents}\nbib entry: {bibtex}` using `rich.console.Console`; error paths print plain-language message, log via `logging.getLogger(__name__)`
- [ ] T022 [US2] Create `src/paper_sorts/cli/add.py`: implement `add_callback(db_url: str)` that uses `ask_text` prompts to collect author list (comma-separated), title, bibtex key, bibtex entry (direct input or from file via `ask_choice` + file read), summary; builds `PaperCreate` DTO; calls `paper_service.add_paper`; prints success or plain-language error
- [ ] T023 [US2] Create `src/paper_sorts/cli/update.py`: implement `update_callback(db_url: str)` that uses `ask_choice` to select table (papers / bib / authors / abort), then column (context-dependent submenu), then `ask_text` for identifier and new value, then `ask_confirm` for the destructive confirmation showing a summary of the exact change; calls `paper_service.update_field`; prints success or plain-language error
- [ ] T024 [US2] Create `src/paper_sorts/cli/delete.py`: implement `delete_callback(db_url: str)` that prompts for a title, searches, presents disambiguation if needed via `ask_choice`, then shows the found paper details and asks `ask_confirm` "Are you sure you want to delete '{title}'?"; calls `paper_service.delete_paper`; prints success or plain-language error
- [ ] T025 [P] [US2] Write CLI integration tests in `tests/test_cli.py` using Typer's `CliRunner` and the `ephemeral_db_url` fixture: test `pdbsearch --help` lists all subcommands; test `search` subcommand with seeded data produces expected output; test `add` subcommand inserts a new paper retrievable via `search`; test `update` subcommand with `n` confirmation makes no change; test `delete` subcommand with `y` confirmation removes the paper; each test seeds via `SEED_PAPERS`

**Checkpoint**: US2 complete — all CLI flows work; repository, service, and CLI tests green.

---

## Phase 5: User Story 3 — Reproducible Test Suite (Priority: P2)

**Goal**: The test suite runs from a fresh checkout with no personal database. Ephemeral PG spins up automatically.

**Independent Test**: `uv run pytest` passes on a machine that has never had the project's personal database. Zero tests skip for "needs live DB" reason.

- [ ] T026 [US3] Write `tests/test_migrations.py`: test that `alembic upgrade head` against the ephemeral DB creates all four tables with the correct columns (query `information_schema.columns`); test that `alembic downgrade base` removes all four tables; test idempotency (run `upgrade head` twice, no error)
- [ ] T027 [P] [US3] Write `tests/test_doc_currency.py` (constitution Gate G3): open `README.md` and `CLAUDE.md` and assert none of `["Poetry", "psycopg2", "UserInteraction", "PsycopgDB"]` appears as a case-sensitive substring; this test fails if legacy tokens remain after cleanup in T040
- [ ] T028 [P] [US3] Write `tests/test_config.py`: test `Settings` loads from env vars (patch `os.environ`), loads from `.env` file (tmp_path fixture with a `.env` file), `FernetConfigSource` raises `ValueError` with clear message when `config_file` points to a non-existent file, `FernetConfigSource` raises `ValueError` when key file is present but wrong key (bad Fernet token)

**Checkpoint**: US3 complete — `git clone && uv sync && uv run pytest` passes end-to-end.

---

## Phase 6: User Story 4 — One-Shot Personal DB Migration (Priority: P2)

**Goal**: `pdbsearch migrate` upgrades a personal database from either historical schema variant to the canonical schema with zero data loss.

**Independent Test**: Seed an ephemeral DB with the `bibtext_id` (sic) legacy DDL, run `pdbsearch migrate`, assert all rows are preserved with correct column names.

- [ ] T029 [US4] Create `src/paper_sorts/cli/migrate.py`: implement `migrate_callback(db_url: str)` that calls `alembic.config.Config` with the project `alembic.ini`, sets `sqlalchemy.url` to `db_url`, and runs `alembic.command.upgrade(config, "head")`; prints "Migration complete." on success or plain-language error on failure; add docstring and type hints
- [ ] T030 [P] [US4] Write migration integration test in `tests/test_migrations.py` (extend the file from T026): create an ephemeral DB with the legacy `bibtext_id` DDL (using raw psycopg), insert test rows, run `alembic upgrade head` via `pdbsearch migrate` equivalent, assert row counts match before/after (papers, authors, authorships, bib entries), assert column is now named `bibtex_id`

**Checkpoint**: US4 complete — personal DB migration is one-shot and idempotent.

---

## Phase 7: User Story 5 — Bulk Import from LaTeX/BibTeX (Priority: P3)

**Goal**: `pdbsearch import --tex FILE --bib FILE` imports all entries from a `.tex`/`.bib` pair.

**Independent Test**: Run `pdbsearch import --tex tests/fixtures/literature_overview.tex --bib tests/fixtures/bib.bib` against seeded ephemeral DB; verify N papers, their authors, and bibtex entries are present.

- [ ] T031 [US5] Create `src/paper_sorts/services/import_service.py`: implement `extract_papers_from_tex_bib(tex_content: str, bib_content: str) -> Iterator[PaperCreate]` that (a) parses the `.tex` content with `pylatexenc.latex2text.LatexNodes2Text` to extract `(title, bibtex_key, description)` triples using the `*...<cit.>` / `\cite{}` pattern from the legacy `get_data` functions; (b) parses the `.bib` content with `pybtex.database.parse_string`; (c) for each extracted title/key pair, looks up the bibtex entry and author list; (d) yields a `PaperCreate` for entries with a match, logs a WARNING (not exception) for entries with no matching bibtex record; NO I/O, NO UI, NO sqlalchemy imports; full docstrings and type hints
- [ ] T032 [US5] Create `src/paper_sorts/cli/importer.py`: implement `import_callback(db_url: str, tex: Path, bib: Path)` Typer subcommand (non-interactive); reads both files as text; calls `import_service.extract_papers_from_tex_bib`; for each `PaperCreate` calls `paper_service.add_paper` in a per-paper try/except (per-paper commit semantics per constitution Principle IV); prints a summary count at end; logs per-paper errors without stopping the import
- [ ] T033 [P] [US5] Add test fixture files `tests/fixtures/literature_overview.tex` and `tests/fixtures/bib.bib` (3–5 entries); write `tests/test_services.py` import tests (extend the file from T017): test `extract_papers_from_tex_bib` with the fixture files yields the expected `PaperCreate` list; test that a `.tex` citation key with no matching `.bib` entry is skipped with no exception; write CLI import test in `tests/test_cli.py` (extend from T025): test `pdbsearch import --tex ... --bib ...` inserts expected papers into ephemeral DB

**Checkpoint**: US5 complete — bulk import works end-to-end with per-paper commit semantics.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Benchmark harness, per-layer coverage gate, doc cleanup, legacy module removal, final quality gate.

- [ ] T034 Write `tests/benchmarks/bench_baseline.py`: benchmark harness using `time.perf_counter` (or `pytest-benchmark` if available) that seeds the ephemeral DB with `SEED_PAPERS`, then times 5 operations: search-by-title, search-by-author, add a paper, update a field, delete a paper; on first run writes results to `tests/benchmarks/baseline.json`; on subsequent runs asserts each operation is within 2× of the recorded baseline; the test MUST NOT be permanently `@pytest.mark.skip`'d (constitution Gate G2); add `@pytest.mark.benchmark` marker and include it in default test run
- [ ] T035 Write `tests/test_coverage_gate.py`: verify per-layer line coverage meets ≥ 80% for each of the four layers using `coverage` data (run `coverage json` and parse the per-file data for `paper_sorts/db/`, `paper_sorts/services/`, `paper_sorts/cli/`, `paper_sorts/config.py`); alternatively configure `[tool.coverage.report]` in `pyproject.toml` with `fail_under = 80` and per-module entries; document that this is constitution Gate G1
- [ ] T036 [P] Update `README.md`: replace Poetry commands with uv commands, replace `paper_sorts/run.py` invocation with `uv run pdbsearch`, replace psycopg2/pylint/unittest references with psycopg v3/ruff/pytest, describe `--database-url` env var option, describe `pdbsearch migrate` for existing databases; verify `tests/test_doc_currency.py` passes after this change
- [ ] T037 Update `CLAUDE.md`: replace legacy architecture section with the new four-layer src-layout description, replace legacy command examples with uv commands, reference `docs/architecture.md` for the legacy stack description, update constitution version reference to v1.3.0-b2-hardened; verify `tests/test_doc_currency.py` passes
- [ ] T038 Remove legacy flat-layout modules: delete `paper_sorts/add.py`, `paper_sorts/search.py`, `paper_sorts/get_data.py`, `paper_sorts/user_interaction.py`, `paper_sorts/database_connector.py`, `paper_sorts/psycopg_db.py`, `paper_sorts/config_reader.py`, `paper_sorts/helpers.py`, `paper_sorts/run.py`, `paper_sorts/__init__.py`; delete the empty `paper_sorts/` directory; update any remaining imports; remove the old test files `tests/test_database_connector.py` and `tests/test_user_interaction.py` (replaced by new test suite) — this satisfies FR-012
- [ ] T039 Run all quality gates and fix any issues: `uv run ruff check src tests`, `uv run ruff format --check src`, `uv run mypy src`, `uv run pytest` (all tests including benchmarks and coverage gate); resolve any type errors, lint warnings, or test failures; ensure all four constitution principles pass
- [ ] T040 Final integration smoke test: run `uv run pdbsearch --help` and verify all six subcommands are listed; run `uv run pytest -v` and confirm zero failures, ≥ 80% per-layer coverage, benchmark harness executes (not skipped), and `test_doc_currency.py` passes; commit final state

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 completion — BLOCKS all user story phases
- **US1 (Phase 3)**: Depends only on Phase 1 (needs repo structure); can start after T001–T005
- **US2 (Phase 4)**: Depends on Phase 2 completion (needs session, models, repositories, seed fixture)
- **US3 (Phase 5)**: Depends on Phase 2 (needs ephemeral DB fixture); T027 also depends on T036–T037 (doc cleanup) but can be written now and will fail until then
- **US4 (Phase 6)**: Depends on Phase 2 (Alembic infrastructure) and T029 depends on T004/T010/T011
- **US5 (Phase 7)**: Depends on Phase 4 service layer (T016) for `add_paper`
- **Polish (Phase 8)**: Depends on all US phases complete; T038 (legacy removal) must be last structural change; T040 (smoke test) must be absolute last

### Within-Phase Dependencies

- T006 (models) → T007 (session) → T014 (repositories) → T016 (services) → T020–T024 (CLI)
- T010 (migration 001) and T011 (migration 002) depend on T006 (models) and T004 (alembic init)
- T012 (conftest) depends on T007 (session) and T010 (migration 001)
- T018 (prompts) is independent; can start in parallel with T014
- T015, T017, T019, T025 (tests) depend on their corresponding implementation tasks

### Parallel Opportunities

Within Phase 1: T002, T003, T004 can all run in parallel after T001.
Within Phase 2: T009 is independent. T010, T011 can proceed in parallel after T006+T004.
Within Phase 4: T015 (repo tests) can be written in parallel with T016 (services); T019 (prompt tests) can be written in parallel with T018 (prompts).

---

## Parallel Example: Phase 2 Foundational

```bash
# After T001 (pyproject.toml):
Task T002: Create src/ package skeleton
Task T003: Configure ruff + mypy
Task T004: Initialize Alembic
Task T005: Create tests/ skeleton
# All four can run in parallel

# After T006 (ORM models):
Task T007: session.py
Task T009: logging_config.py  ← also independent, run in parallel
# T007 and T009 can run in parallel
```

---

## Implementation Strategy

### MVP First (US1 + US2 as functional baseline)

1. Complete Phase 1: Setup (T001–T005)
2. Complete Phase 2: Foundational (T006–T012)
3. Complete Phase 3: US1 Architecture Doc (T013) — documents what we're replacing
4. Complete Phase 4: US2 Modernized Codebase (T014–T025) — the core deliverable
5. **STOP and VALIDATE**: `uv run pytest tests/test_repositories.py tests/test_services.py tests/test_cli.py` all green; `uv run pdbsearch` shows four-option menu

### Incremental Delivery

1. Setup + Foundational → DB layer functional
2. US1 → architecture documented
3. US2 → modern CLI working, tests green
4. US3 → fresh-checkout test suite works
5. US4 → personal DB migration works
6. US5 → bulk import works
7. Polish → legacy removed, coverage gates pass, benchmarks green

---

## Notes

- `[P]` tasks target different files with no shared-state dependencies — safe to parallelise
- Commit after each phase or logical group (constitution says frequent commits)
- Per constitution Principle II: NEVER mock the SQLAlchemy session or DB driver in persistence tests
- Per constitution Principle III: ALL prompts must route through `src/paper_sorts/cli/prompts.py`
- Per constitution Principle I: ONLY `src/paper_sorts/db/` imports `sqlalchemy` or any DB driver
- Schema preservation: NO NOT NULL outside PKs, NO FKs on `authors_papers`, NO extra indexes
- T038 (legacy removal) is the point of no return — only execute after all new tests are green
- Benchmark harness (T034) MUST execute, not be permanently skipped (constitution Gate G2)
- Doc-currency test (T027) MUST pass after T036/T037 doc updates (constitution Gate G3)
