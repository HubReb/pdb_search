# Tasks: Modernize the Stack

**Input**: Design documents from `/specs/001-modernize-stack/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/cli-commands.md, quickstart.md

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1–US5)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization — pyproject.toml, src-layout skeleton, tooling config.

- [ ] T001 Replace pyproject.toml: switch from Poetry to uv/PEP 621/hatchling build backend with Python>=3.11, add all required dependencies (sqlalchemy, psycopg[binary], typer, pydantic-settings, alembic, pybtex, pylatexenc, cryptography, rich, pytest, pytest-postgresql, ruff, mypy) at `pyproject.toml`
- [ ] T002 Create src-layout package skeleton: `src/paper_sorts/__init__.py`, `src/paper_sorts/cli/__init__.py`, `src/paper_sorts/services/__init__.py`, `src/paper_sorts/db/__init__.py`
- [ ] T003 [P] Configure ruff in `pyproject.toml` (lint rules, line-length, per-file ignores for tests) and mypy in `pyproject.toml` (strict, src layout)
- [ ] T004 [P] Create `uv.lock` by running `uv sync --all-extras` to pin dependencies

**Checkpoint**: `uv sync --all-extras` succeeds; `uv run ruff check src` and `uv run mypy src` run (may have no source yet)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Config, logging, ORM models, session, and Alembic must exist before any service or CLI work can begin.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T005 Implement `src/paper_sorts/config.py`: pydantic-settings `Settings` with `database_url` (PostgresDsn), `log_level` (str, default "INFO"), `config_file` (str|None), `key_file` (str|None); custom Fernet-encrypted INI source that reads `config_file`+`key_file` when both are set; four-source priority (CLI > env `PDBSEARCH_*` > .env > Fernet INI)
- [ ] T006 [P] Implement `src/paper_sorts/logging_config.py`: `configure_logging(log_level: str) -> None` using `logging.config.dictConfig`; RichHandler to stdout + optional FileHandler; called once from cli/app.py at startup
- [ ] T007 [P] Implement `src/paper_sorts/db/models.py`: SQLAlchemy 2.x `DeclarativeBase`; four ORM models: `Paper(id, title, contents, bibtex_id)`, `Bib(bibtex_id PK, bibtex)`, `Author(id, author)`, `AuthorPaper(id, author_id, paper_id)` — no DDL FKs on `AuthorPaper`, FK from `papers.bibtex_id → bib.bibtex_id`; full type hints and docstrings
- [ ] T008 Implement `src/paper_sorts/db/session.py`: `with_session(url: str)` as `contextlib.contextmanager` yielding `Session`; commit on clean exit, rollback + re-raise on exception; deterministic close; full type hints and docstring
- [ ] T009 Initialize Alembic: `alembic init migrations`; update `migrations/env.py` to import `Base` from `src/paper_sorts/db/models.py` and read `database_url` from `Settings`
- [ ] T010 Write Alembic migration `migrations/versions/001_initial_schema.py`: verbatim port of `DatabaseConnector.create_tables()` DDL — creates `authors_papers`, `authors_id`, `bib`, `papers` tables; upgrade + downgrade
- [ ] T011 Write Alembic migration `migrations/versions/002_converge_schema.py`: conditional DDL — rename `papers.bibtext_id` → `papers.bibtex_id` if the typo column exists; idempotent; downgrade is no-op

**Checkpoint**: `uv run alembic upgrade head` succeeds against a local DB; `uv run mypy src` passes on db/ and config.py

---

## Phase 3: User Story 1 — Architecture Documentation (Priority: P1)

**Goal**: A written architecture document covering purpose, user journeys, data model, control flow, configuration, install/run, and known limitations of the pre-modernization codebase. This is the acceptance reference for all other stories.

**Independent Test**: A Python developer unfamiliar with the project reads the document and can answer "What does it do? How is the data modeled? Where would I add a new field?" without opening source.

- [ ] T012 [US1] Write `docs/architecture.md`: purpose, user journeys (search/add/update/delete/import), four-table data model with ER relationships, control flow (CLI → UserInteraction → DatabaseConnector → PsycopgDB → PostgreSQL), configuration approach (Fernet INI), install/run instructions for legacy + modern stack, known limitations (author dedup by string, no FK on authors_papers, single-user only, no CI due to live-DB test dependency)

**Checkpoint**: Document exists; answers the three questions from SC-001 without needing to open source files

---

## Phase 4: User Story 2 — Modernized Codebase, Same Behavior (Priority: P1) 🎯 MVP

**Goal**: Rebuilt CLI on modern stack with identical user-facing behavior for all five operations (search, add, update, delete, import).

**Independent Test**: Run each CLI path against seeded test data — search by title (unique), search by title (multiple), search by author, add inline, add from .bib, update field + confirm y, update + confirm n, delete, quit. Each must produce equivalent output to the legacy stack.

### Persistence Layer (T013–T019)

- [ ] T013 [P] [US2] Implement pydantic DTOs in `src/paper_sorts/db/repositories.py`: `PaperCreate(title, authors: list[str], bibtex_key, summary, bibtex_text)` and `PaperSummary(paper_id, title, authors: list[str], summary, bibtex_key, bibtex_text)`; full type hints and docstrings
- [ ] T014 [P] [US2] Implement `BibRepository` in `src/paper_sorts/db/repositories.py`: `get_by_key(session, key) -> Bib | None`, `create(session, key, text) -> Bib`; full type hints and docstrings; sqlalchemy only in this module
- [ ] T015 [P] [US2] Implement `AuthorRepository` in `src/paper_sorts/db/repositories.py`: `get_or_create(session, name) -> Author`, `get_by_paper_id(session, paper_id) -> list[Author]`; full type hints and docstrings
- [ ] T016 [US2] Implement `PaperRepository` in `src/paper_sorts/db/repositories.py`: `search_by_title(session, title) -> list[PaperSummary]`, `search_by_author(session, author) -> list[PaperSummary]`, `get_by_id(session, paper_id) -> PaperSummary | None`, `create(session, paper: PaperCreate) -> PaperSummary`, `update_field(session, paper_id, field, value) -> None`, `delete(session, paper_id) -> None`; full parameterised JOIN queries; no SQL strings outside db/ modules; full type hints and docstrings

### Service Layer (T017–T019)

- [ ] T017 [US2] Implement `src/paper_sorts/services/paper_service.py`: `search_by_title(title, with_session_fn)`, `search_by_author(author, with_session_fn)`, `add_paper(paper: PaperCreate, with_session_fn)`, `update_field(paper_id, field: Literal[...], value, with_session_fn)` with `match`/`case` and `assert_never`, `delete_paper(paper_id, with_session_fn)`; no sqlalchemy imports; receives `with_session_fn` parameter (default from db.session); full type hints and docstrings
- [ ] T018 [P] [US2] Implement `src/paper_sorts/cli/prompts.py`: `ask_nonempty(prompt) -> str` (re-prompts on empty), `ask_choice(options: list[str]) -> int` (1-indexed, validates range, re-prompts), `ask_confirm(action_desc: str) -> bool` (accepts y/yes/1 and n/no/2); ONLY module under src/paper_sorts/ permitted to import rich.prompt; full type hints and docstrings
- [ ] T019 [US2] Implement Typer CLI: `src/paper_sorts/cli/app.py` (Typer app wiring, `pdbsearch` entry point, four-option interactive menu when no subcommand), `src/paper_sorts/cli/search.py` (search subcommand: by-title + by-author sub-menus, display results via prompts.py), `src/paper_sorts/cli/add.py` (add subcommand: inline or from .bib file), `src/paper_sorts/cli/update.py` (update subcommand: search → field select → new value → confirm), `src/paper_sorts/cli/delete.py` (delete subcommand: search → display → confirm); wire `[project.scripts] pdbsearch = "paper_sorts.cli.app:app"` in pyproject.toml; full type hints and docstrings

**Checkpoint**: `uv run pdbsearch --help` lists all subcommands; `uv run pdbsearch` drops into 4-option menu; each subcommand is reachable

---

## Phase 5: User Story 3 — Reproducible Test Suite (Priority: P2)

**Goal**: `uv sync && uv run pytest` passes on a fresh checkout with no personal database.

**Independent Test**: Delete any personal database credentials, run `uv run pytest` — all tests pass.

- [ ] T020 [US3] Configure pytest in `pyproject.toml` and write `tests/conftest.py`: `postgresql_proc` fixture (postgresql_proc at `/usr/bin/pg_ctl`), `ephemeral_db_url` fixture that builds a DSN for the ephemeral cluster, `db_session` fixture that runs `alembic upgrade head` against ephemeral DB then yields a Session
- [ ] T021 [P] [US3] Write canonical seed dataset `tests/fixtures/seed_papers.py`: `SEED_PAPERS: list[PaperCreate]` — at least 3 papers, ≥2 authors per paper, at least 1 author shared across papers, at least 1 paper with unique title, at least 2 papers sharing a partial title for disambiguation testing; comment explaining each fixture's test purpose
- [ ] T022 [P] [US3] Write `tests/test_repositories.py`: integration tests for `PaperRepository` (search by title unique, search by title multiple, search by author, create, update_field each column, delete), `AuthorRepository` (get_or_create existing, get_or_create new, get_by_paper_id), `BibRepository` (get_by_key existing, get_by_key missing, create); all tests use ephemeral DB; seed via `SEED_PAPERS`; no mocking of session/repositories/driver
- [ ] T023 [P] [US3] Write `tests/test_services.py`: integration tests for `paper_service` (search_by_title, search_by_author, add_paper, update_field, delete_paper); use ephemeral DB session factory; seed via `SEED_PAPERS`; no mocking
- [ ] T024 [P] [US3] Write `tests/test_config.py`: unit tests for `Settings` — env var override, .env file parsing, missing credentials produces actionable error (not stack trace), Fernet source skipped gracefully when config_file/key_file are None
- [ ] T025 [P] [US3] Write `tests/test_cli.py`: unit tests for `cli/prompts.py` — empty input re-prompt, out-of-range selection re-prompt, confirmation accepts y/yes/1/n/no/2; use `monkeypatch` to mock stdin; also smoke-test that `pdbsearch --help` runs without error via typer.testing.CliRunner
- [ ] T026 [P] [US3] Write benchmark stub `tests/benchmarks/bench_baseline.py` with `@pytest.mark.skip(reason="Awaiting T046: modern benchmark rewrite")` and `tests/benchmarks/baseline.json` with empty baseline; prevents CI from running unimplemented benchmark

**Checkpoint**: `uv run pytest` passes with all tests green; `uv run ruff check src tests` clean; `uv run mypy src` clean

---

## Phase 6: User Story 4 — One-Shot Migration (Priority: P2)

**Goal**: `pdbsearch migrate` upgrades an existing personal database (either historical schema variant) to the modernized schema with zero data loss.

**Independent Test**: Take a row-count snapshot of a personal DB (if available); run `pdbsearch migrate`; confirm counts match. Run a second time — idempotent, no error.

- [ ] T027 [US4] Implement `src/paper_sorts/cli/migrate.py`: `migrate` subcommand that calls `alembic upgrade head` programmatically; prints progress; graceful error on missing database_url; wire into `src/paper_sorts/cli/app.py` as subcommand-only (not in interactive menu); full type hints and docstring
- [ ] T028 [US4] Write `tests/test_migrations.py`: integration test that creates a table layout matching the `bibtext_id` typo variant on the ephemeral DB, runs `alembic upgrade head`, then verifies `bibtex_id` column exists and row count is preserved; run upgrade twice to verify idempotency

**Checkpoint**: `pdbsearch migrate` on a fresh DB succeeds; `pdbsearch migrate` on an already-migrated DB succeeds (idempotent); test passes

---

## Phase 7: User Story 5 — Bulk Import (Priority: P3)

**Goal**: `pdbsearch import literature.tex bib.bib` imports all cited entries into the database.

**Independent Test**: Run bulk import against fixture `.tex` + `.bib` files; verify expected paper count, author count, bib entries in DB.

- [ ] T029 [US5] Implement `src/paper_sorts/services/import_service.py`: `extract_papers_from_tex_bib(tex_path: str, bib_path: str) -> Iterator[PaperCreate]` — parses `.tex` using pylatexenc/pybtex to find `\cite{}` keys; for each key found in `.bib`, yields a `PaperCreate`; skips with logged warning if citation key not in `.bib`; preserves LaTeX accent round-trip via pybtex
- [ ] T030 [US5] Implement `src/paper_sorts/cli/importer.py`: `import_papers` subcommand accepting `tex_file` and `bib_file` positional args; calls `import_service.extract_papers_from_tex_bib`, then `paper_service.add_paper` per paper with per-paper commit (constitution IV); logs skipped entries; wire into `src/paper_sorts/cli/app.py` as subcommand-only; full type hints and docstring
- [ ] T031 [P] [US5] Write import fixture files `tests/fixtures/test_literature.tex` and `tests/fixtures/test.bib` (≥3 entries, 1 citation key missing from .bib to test skip behaviour, 1 entry with LaTeX accent); write `tests/test_import_service.py`: verify paper count, author names, skipped warning; verify LaTeX accent round-trip; use ephemeral DB

**Checkpoint**: `pdbsearch import tests/fixtures/test_literature.tex tests/fixtures/test.bib` succeeds; test passes; partial failure leaves earlier papers committed

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Cleanup, legacy removal, final quality gates.

- [ ] T032 Remove legacy flat-layout modules: `paper_sorts/add.py`, `paper_sorts/search.py`, `paper_sorts/get_data.py`, `paper_sorts/psycopg_db.py`, `paper_sorts/database_connector.py`, `paper_sorts/user_interaction.py`, `paper_sorts/helpers.py`, `paper_sorts/config_reader.py`, `paper_sorts/run.py`, `paper_sorts/__init__.py`; remove empty `paper_sorts/` directory
- [ ] T033 [P] Update `CLAUDE.md`: remove legacy architecture section, replace with modern src-layout description matching the implemented code; update Commands section to use `uv run` commands; update SpecKit section to reference active feature artifacts
- [ ] T034 [P] Update `README.md`: modern install + run instructions using uv; reference quickstart.md for full configuration docs
- [ ] T035 Run final quality gates and fix all issues: `uv run ruff check src tests` (zero warnings), `uv run ruff format --check src tests` (no diff), `uv run mypy src` (zero errors), `uv run pytest` (all tests green); commit final clean state

**Checkpoint**: All three gates green; `uv run pdbsearch --help` works; legacy directory gone

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 completion — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Phase 2 — can start in parallel with US2
- **US2 (Phase 4)**: Depends on Phase 2 — MVP; service + CLI + persistence
- **US3 (Phase 5)**: Depends on US2 (Phase 4) completion — tests exercise the implemented stack
- **US4 (Phase 6)**: Depends on Phase 2 (Alembic migrations) — can start after T011
- **US5 (Phase 7)**: Depends on US2 (service layer, specifically T017 add_paper)
- **Polish (Phase 8)**: Depends on all user stories

### Within US2 (Phase 4)

T013–T016 (persistence) → T017 (service) → T018 (prompts, parallel) → T019 (CLI, depends on T017+T018)

### Parallel Opportunities

```bash
# Phase 2 parallelizable tasks:
T006 logging_config.py  ||  T007 models.py

# Phase 4 persistence layer (all different files):
T013 DTOs  ||  T014 BibRepository  ||  T015 AuthorRepository
# then:
T016 PaperRepository (depends on T013–T015)
# then parallel:
T017 paper_service.py  ||  T018 prompts.py
# then:
T019 CLI (depends on T017+T018)

# Phase 5 tests (all different test files):
T021 seed_papers.py  ||  T022 test_repositories.py  ||  T023 test_services.py
||  T024 test_config.py  ||  T025 test_cli.py  ||  T026 bench stub
```

---

## Implementation Strategy

### MVP First (US2 — Modernized Codebase)

1. Complete Phase 1: Setup (T001–T004)
2. Complete Phase 2: Foundational (T005–T011)
3. Complete Phase 4: US2 (T013–T019)
4. **STOP and VALIDATE**: `uv run pdbsearch --help`, run each subcommand manually
5. Complete Phase 5: US3 (tests, T020–T026) — validates US2 programmatically

### Incremental Delivery

1. Setup + Foundational → project compiles, migrations run
2. US2 → CLI works against a local DB (MVP)
3. US3 → test suite green, no personal DB needed
4. US1 → architecture docs (can be written at any point)
5. US4 → migration command (requires Alembic from Phase 2)
6. US5 → bulk import (requires add_paper from US2)
7. Polish → legacy removal, final gates

---

## Notes

- Constitution Principle I: only `src/paper_sorts/db/` may import sqlalchemy — enforced by mypy and code review
- Constitution Principle II: no mocking of SQLAlchemy session, repositories, or driver in persistence tests
- Constitution Principle III: all prompts route through `src/paper_sorts/cli/prompts.py` — no bare `input()` elsewhere
- Constitution Principle IV: per-paper commit in bulk import; no connection pooling beyond SQLAlchemy default
- Schema-preservation contract: no NOT NULL outside PKs, no FKs on authors_papers, no new indexes
- `[P]` tasks = different files, no dependencies — safe to execute in parallel
- `[Story]` label maps task to spec.md user story for traceability
- Commit after each logical group (e.g., after each phase checkpoint)
