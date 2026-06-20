# Tasks: Modernize the Stack

**Input**: Design documents from `/specs/001-modernize-stack/`
**Prerequisites**: plan.md ✓, spec.md ✓, research.md ✓, data-model.md ✓, contracts/cli-commands.md ✓

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Replace legacy project skeleton with modern src-layout; install toolchain

- [ ] T001 Replace `pyproject.toml` (poetry → uv/hatchling): Python ≥3.11, add SQLAlchemy, Alembic, Typer, pydantic-settings, psycopg[binary], pybtex, pylatexenc, rich, cryptography, ruff, mypy, pytest, pytest-postgresql, coverage in `pyproject.toml`
- [ ] T002 Create `src/paper_sorts/` package skeleton: `src/paper_sorts/__init__.py`, `src/paper_sorts/cli/__init__.py`, `src/paper_sorts/services/__init__.py`, `src/paper_sorts/db/__init__.py`
- [ ] T003 Run `uv sync --all-extras` and verify it exits 0

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Persistence layer, migrations, config, logging — everything user stories build on.

**CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T004 Write `src/paper_sorts/db/models.py`: four SQLAlchemy 2.x declarative models (`Bib`, `Paper`, `Author`, `AuthorPaper`) exactly matching the legacy DDL from `paper_sorts/database_connector.py`; no FKs on `authors_papers`, no new NOT NULL, no new indexes
- [ ] T005 [P] Write `src/paper_sorts/db/session.py`: `get_engine(database_url: str) -> Engine`, `with_session(engine)` context-manager that commits on success and rolls back on exception; deterministic close (no connection pooling beyond SA defaults)
- [ ] T006 [P] Write Pydantic DTOs in `src/paper_sorts/db/repositories.py`: `PaperSummary(paper_id, title, contents, bibtex_id, authors, bibtex)` and `PaperCreate(title, contents, bibtex_id, bibtex, authors)` as `pydantic.BaseModel` subclasses; no SQLAlchemy imports
- [ ] T007 Write `PaperRepository` in `src/paper_sorts/db/repositories.py`: `search_by_title(session, title) -> list[PaperSummary]`, `search_by_author(session, author) -> list[PaperSummary]`, `add_paper(session, paper: PaperCreate) -> None`, `delete_paper(session, bibtex_id: str) -> None`; all SQL via SQLAlchemy ORM/Core, no raw strings
- [ ] T008 Write `AuthorRepository` in `src/paper_sorts/db/repositories.py`: `get_or_create_author(session, name: str) -> Author`, `link_author_paper(session, author_id: int, paper_id: int) -> None`, `unlink_author_paper(session, author_id: int, paper_id: int) -> None`, `update_author_name(session, old_name: str, new_name: str) -> None`
- [ ] T009 Write `BibRepository` in `src/paper_sorts/db/repositories.py`: `get_bibtex(session, bibtex_id: str) -> str | None`, `update_bibtex(session, bibtex_id: str, new_bibtex: str) -> None`
- [ ] T010 Initialize Alembic: run `alembic init migrations` in worktree root; update `migrations/env.py` to import `src/paper_sorts/db/models.py` metadata and read `database_url` from environment
- [ ] T011 Write Alembic Revision 001 (`migrations/versions/001_initial_schema.py`): verbatim DDL port of `create_tables()` — `authors_papers`, `authors_id`, `bib` (bibtex_id PK, bibtex UNIQUE), `papers` (bibtex_id FK → bib); no extra constraints
- [ ] T012 Write Alembic Revision 002 (`migrations/versions/002_fix_bibtext_typo.py`): detect `bibtext_id` column in `bib` and `papers`; if found, rename to `bibtex_id`; idempotent (skip if column already named correctly)
- [ ] T013 Write `src/paper_sorts/config.py`: `pydantic_settings.BaseSettings` subclass `Settings` with `database_url: str`, `log_level: str = "INFO"`, `config_file: str | None`, `key_file: str | None`; env prefix `PDBSEARCH_`; `FernetSettingsSource` custom source for encrypted-INI fallback; priority order: CLI → env → .env → Fernet INI
- [ ] T014 [P] Write `src/paper_sorts/logging_config.py`: `configure_logging(log_level: str) -> None` calling `logging.config.dictConfig`; RichHandler to stdout, optional FileHandler; called once from CLI startup
- [ ] T015 Write `tests/conftest.py`: `postgresql_proc` fixture (pytest-postgresql, host pg_ctl at `/usr/bin/pg_ctl`); `ephemeral_db_url` session fixture that creates an empty DB, runs Alembic migrations to head, yields URL, drops DB
- [ ] T016 Write `tests/fixtures/seed_papers.py`: `SEED_PAPERS: list[PaperCreate]` — at minimum: the "Direct speech-to-speech translation" paper (multiple authors), the "Large-scale Self-" paper (author "Pino, J."), a paper with a BibTeX entry containing LaTeX accents, two papers sharing the same title (disambiguation test)

**Checkpoint**: Foundation complete — user story implementation can begin.

---

## Phase 3: User Story 1 — Architecture Document (Priority: P1)

**Goal**: Deliver `docs/architecture.md` — the reverse-engineered description of the legacy codebase that serves as the acceptance reference for US2.

**Independent Test**: A Python developer who has never seen the project can read the document and answer "What does it do? What is the data model? Where would I add a new field?" without opening source.

- [ ] T017 [US1] Verify `docs/architecture.md` covers all six required sections (purpose, user journeys, data model, control flow, configuration, limitations) against spec acceptance scenarios AS1-1, AS1-2, AS1-3; fix any gaps in `/home/rebekka/projects/pdb_search-repBS1/docs/architecture.md`

**Checkpoint**: US1 complete — architecture doc ready.

---

## Phase 4: User Story 2 — Modernized Codebase (Priority: P1) MVP

**Goal**: CLI rebuilt on Typer + SQLAlchemy; all legacy CLI flows preserved; ruff + mypy green.

**Independent Test**: `uv run pdbsearch --help` shows subcommands; interactive menu appears when invoked with no args; all five operations work against the seeded ephemeral DB.

### Integration tests for US2

- [ ] T018 [US2] Write `tests/test_repositories.py`: integration tests for `PaperRepository.search_by_title` (found/not found), `search_by_author` (found/not found), `add_paper` (success + duplicate bibtex_id → error), `delete_paper` (success + not found); seed from `SEED_PAPERS`; against real ephemeral DB
- [ ] T019 [P] [US2] Write `tests/test_paper_service.py`: integration tests for `paper_service.search_by_title`, `search_by_author`, `add_paper`, `update_field` (title, contents, bibtex, author), `delete_paper`; seed from `SEED_PAPERS`
- [ ] T020 [P] [US2] Write `tests/test_config.py`: unit tests for `Settings` — env-var override, `.env` file load, missing key file raises clear error, default log level

### Implementation for US2

- [ ] T021 [US2] Write `src/paper_sorts/services/paper_service.py`: `search_by_title(engine, title) -> list[PaperSummary]`, `search_by_author(engine, author) -> list[PaperSummary]`, `add_paper(engine, paper: PaperCreate) -> None`, `update_field(engine, table: Literal["papers","bib","authors_id"], identifier: str, field: str, value: str) -> None` with `match`/`case` + `assert_never`, `delete_paper(engine, bibtex_id: str) -> None`; no SQLAlchemy imports — depends on repositories only
- [ ] T022 [P] [US2] Write `src/paper_sorts/cli/prompts.py`: ALL user-facing I/O helpers: `ask_str(prompt: str) -> str` (re-prompts on empty), `ask_choice(options: list[str], prompt: str) -> int` (1-indexed, re-prompts on invalid, always has abort option), `ask_confirm(message: str) -> bool` (accepts 1/y/yes, 2/n/no); NO bare `input()` calls anywhere else in `src/paper_sorts/`
- [ ] T023 [US2] Write `src/paper_sorts/cli/search.py`: Typer `search` subcommand; delegates to `paper_service.search_by_title` or `search_by_author`; uses `prompts.ask_choice` for disambiguation; prints title/authors/summary/bibtex via `rich.console`
- [ ] T024 [P] [US2] Write `src/paper_sorts/cli/add.py`: Typer `add` subcommand with optional `--bib-file FILE`; prompts for author(s), title, bibtex key, summary; reads BibTeX from file or inline prompt; calls `paper_service.add_paper`; shows confirmation via `ask_confirm`
- [ ] T025 [P] [US2] Write `src/paper_sorts/cli/update.py`: Typer `update` subcommand; prompts table → field → identifier → new value → `ask_confirm`; calls `paper_service.update_field`; plain-language error on failure
- [ ] T026 [P] [US2] Write `src/paper_sorts/cli/delete.py`: Typer `delete` subcommand; searches for paper (by title), shows details, `ask_confirm`; calls `paper_service.delete_paper`
- [ ] T027 [US2] Write `src/paper_sorts/cli/app.py`: Typer `app`; register search/add/update/delete subcommands; `callback` with `invoke_without_command=True` that drops into 5-option interactive menu when no subcommand given; accept global `--database-url`, `--log-level`, `--config`, `--key` options; call `configure_logging` at startup; build `Settings` and `Engine`; pass engine to subcommands via Typer context

**Checkpoint**: US2 complete — `uv run pdbsearch` fully functional.

---

## Phase 5: User Story 3 — Reproducible Test Suite (Priority: P2)

**Goal**: `uv run pytest` passes on a fresh checkout with no personal database; coverage ≥ 80% on persistence layer.

**Independent Test**: Delete `~/.pgpass` and any personal DB creds; run `uv sync --all-extras && uv run pytest` from clean clone. All tests pass.

- [ ] T028 [US3] Verify `tests/conftest.py` `ephemeral_db_url` fixture actually spins up PG via pytest-postgresql and runs Alembic migrations; confirm seed fixture populates from `SEED_PAPERS`; run `uv run pytest -x -v` and fix any failures
- [ ] T029 [P] [US3] Add `tests/test_prompts.py`: unit tests for `cli/prompts.py` — `ask_str` with empty then non-empty input (via `monkeypatch`), `ask_choice` with out-of-range then valid, `ask_confirm` with y/n/yes/no/1/2

**Checkpoint**: US3 complete — fresh-checkout test run succeeds.

---

## Phase 6: User Story 4 — One-Shot Migration (Priority: P2)

**Goal**: `pdbsearch migrate` upgrades both legacy schema variants to modern schema with zero data loss.

**Independent Test**: Snapshot a DB seeded with legacy `bibtext_id` schema; run `pdbsearch migrate`; verify paper/author/authorship counts match; spot-check row content.

- [ ] T030 [US4] Write `src/paper_sorts/cli/migrate.py`: Typer `migrate` subcommand; runs `alembic upgrade head` programmatically; accepts `--revision TARGET`; idempotent
- [ ] T031 [P] [US4] Write `tests/test_migrations.py`: integration test that creates a DB with `bibtext_id` columns (Revision 000 baseline), runs Alembic to head, verifies columns renamed to `bibtex_id`, verifies data preserved

**Checkpoint**: US4 complete — `pdbsearch migrate` handles both schema variants.

---

## Phase 7: User Story 5 — Bulk Import (Priority: P3)

**Goal**: `pdbsearch import --tex FILE --bib FILE` inserts all cited entries; per-paper commit; missing-key entries skipped with warning.

**Independent Test**: Run `pdbsearch import --tex tests/fixtures/literature_overview.tex --bib tests/fixtures/bib.bib`; count rows in DB; verify N papers inserted; verify missing-key entry is absent and logged.

- [ ] T032 [US5] Write `src/paper_sorts/services/import_service.py`: `extract_papers_from_tex_bib(tex_path: str, bib_path: str) -> Iterator[PaperCreate]`; re-implements logic from `paper_sorts/helpers.py:get_data` + `get_bibtex_information`; yields `PaperCreate` per matched entry; logs warning for unmatched cite keys; no I/O or prompt calls
- [ ] T033 [US5] Write `src/paper_sorts/cli/importer.py`: Typer `import` subcommand (registered as `import` alias); required `--tex FILE` and `--bib FILE`; iterates `extract_papers_from_tex_bib`; calls `paper_service.add_paper` per entry in its own session (per-paper commit); logs warnings for skipped entries
- [ ] T034 [P] [US5] Create test fixtures: copy a representative `.tex` + `.bib` pair to `tests/fixtures/literature_overview.tex` and `tests/fixtures/bib.bib` (can be minimal — 3 entries, 1 unmatched cite key)
- [ ] T035 [P] [US5] Write `tests/test_import_service.py`: integration test for `extract_papers_from_tex_bib`; asserts matched entries returned, unmatched skipped; asserts BibTeX round-trips without corruption; uses fixture files

**Checkpoint**: US5 complete — bulk import functional.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Quality gates, legacy removal, final validation

- [ ] T036 Run `uv run ruff check src tests` and fix all lint errors
- [ ] T037 [P] Run `uv run mypy src` with strict mode; fix all type errors; ensure `assert_never` in `update_field` satisfies exhaustiveness
- [ ] T038 Run `uv run pytest --cov=src/paper_sorts --cov-report=term-missing` and verify ≥80% statement coverage on `db/` and `services/`
- [ ] T039 Remove legacy flat-layout directory `paper_sorts/` (FR-012): verify no imports of `paper_sorts.*` remain in `src/` or `tests/` first; then `git rm -r paper_sorts/`
- [ ] T040 [P] Update `README.md` to reflect uv commands and new `pdbsearch` entry point
- [ ] T041 Final end-to-end check: `uv run ruff check src tests` exits 0, `uv run mypy src` exits 0, `uv run pytest` all green

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 — BLOCKS all user stories
- **Phase 3 (US1 — Architecture doc)**: Depends on Phase 1 only (already drafted)
- **Phase 4 (US2 — Modernized CLI)**: Depends on Phase 2 — core MVP
- **Phase 5 (US3 — Test suite)**: Depends on Phase 4 (needs working tests against real code)
- **Phase 6 (US4 — Migration)**: Depends on Phase 2 (needs Alembic + models)
- **Phase 7 (US5 — Bulk import)**: Depends on Phase 4 (needs `add_paper` service)
- **Phase 8 (Polish)**: Depends on all prior phases

### User Story Dependencies

- **US1**: Can start after Phase 1 — already 90% done (architecture doc written)
- **US2**: Depends on Phase 2 foundational completion
- **US3**: Depends on US2 (ephemeral DB tests exercise real modernized code)
- **US4**: Depends on Phase 2 foundational (Alembic, models); independent of US2
- **US5**: Depends on US2 (`paper_service.add_paper` must exist)

### Within Each Phase

- Repositories before services (`db/` → `services/`)
- Services before CLI (`services/` → `cli/`)
- Config/logging before CLI startup (`config.py`, `logging_config.py` → `cli/app.py`)
- Integration tests require ephemeral DB fixture (conftest.py → test_*.py)

### Parallel Opportunities

- T005, T006, T014 can run in parallel (independent files)
- T018, T019, T020 (US2 tests) can run in parallel
- T023, T024, T025, T026 (CLI subcommands) can run in parallel after T021, T022
- T034, T035 (US5 fixtures + tests) can run in parallel with T032
- T036, T037 (ruff + mypy) can run in parallel

---

## Parallel Example: Phase 2 Foundational

```bash
# These can be worked in parallel (different files):
Task T005: "Write db/session.py"
Task T006: "Write PaperSummary / PaperCreate DTOs"
Task T014: "Write logging_config.py"
```

---

## Implementation Strategy

### MVP First (US1 + US2 Only)

1. Complete Phase 1: Setup (T001–T003)
2. Complete Phase 2: Foundational (T004–T016) — CRITICAL
3. Complete Phase 3: US1 — Architecture doc (T017)
4. Complete Phase 4: US2 — Modernized CLI (T018–T027)
5. **STOP and VALIDATE**: `uv run pytest`, `uv run pdbsearch --help`
6. Remove legacy `paper_sorts/` directory

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. US1 (architecture doc) → PR ready immediately
3. US2 (modernized CLI) → MVP ready to use
4. US3 (test suite) → CI-ready
5. US4 (migration) → existing data safe
6. US5 (bulk import) → full feature parity

---

## Notes

- [P] tasks = different files, no dependencies — safe to implement in parallel
- [Story] label maps task to spec user story for traceability
- Commit after each phase or logical group
- T039 (legacy removal) must only happen after T038 (tests green)
- Do NOT mock the SQLAlchemy session, repositories, or DB driver in integration tests (Principle II)
- All prompts MUST go through `cli/prompts.py` — no bare `input()` elsewhere (Principle III)
- `update_field` in `paper_service.py` MUST use `match`/`case` with `assert_never` for compile-time exhaustiveness (Principle I)
