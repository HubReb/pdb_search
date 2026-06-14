# Tasks: Modernize the Stack

**Input**: Design documents from `/specs/001-modernize-stack/`
**Prerequisites**: plan.md ✅, spec.md ✅, research.md ✅, data-model.md ✅, contracts/cli-commands.md ✅, quickstart.md ✅

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project re-initialization: pyproject.toml, src-layout scaffold, tooling config.

- [ ] T001 Rewrite `pyproject.toml` to use PEP 621 metadata, hatchling build backend, uv, Python >=3.11, and add all modern dependencies (sqlalchemy[mypy], psycopg[binary], typer, alembic, pydantic-settings, pybtex, pylatexenc, cryptography, rich, ruff, mypy, pytest, pytest-postgresql, coverage)
- [ ] T002 Create `src/paper_sorts/__init__.py` with package version
- [ ] T003 [P] Create `src/paper_sorts/cli/__init__.py` (empty)
- [ ] T004 [P] Create `src/paper_sorts/services/__init__.py` (empty)
- [ ] T005 [P] Create `src/paper_sorts/db/__init__.py` (empty)
- [ ] T006 Create `pyproject.toml` `[tool.ruff]`, `[tool.mypy]`, `[tool.pytest.ini_options]`, `[tool.coverage.run]` sections
- [ ] T007 Create `alembic.ini` at repo root pointing to `migrations/` directory and `src/paper_sorts/db/models.py` metadata
- [ ] T008 Create `migrations/` directory with `env.py`, `script.py.mako` wired to SQLAlchemy metadata

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before any user story can be implemented.

**CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T009 Implement `src/paper_sorts/config.py`: pydantic-settings `Settings` model with fields `database_url`, `log_level`, `config` (path to encrypted INI), `key` (path to Fernet key); implement custom `FernetIniSettingsSource` that decrypts the INI and merges into settings; priority order: CLI flags > env (`PDBSEARCH_*`) > `.env` > encrypted INI
- [ ] T010 Implement `src/paper_sorts/logging_config.py`: single `logging.config.dictConfig` call; RichHandler to stdout at configured level; optional FileHandler; called once from CLI app at startup
- [ ] T011 Implement `src/paper_sorts/db/models.py`: SQLAlchemy 2.x `DeclarativeBase`; four ORM models: `Paper`, `Bib`, `Author`, `AuthorPaper` matching the exact DDL from `DatabaseConnector.create_tables()` (no extra NOT NULL, no extra FKs, no extra indexes)
- [ ] T012 Implement `src/paper_sorts/db/session.py`: `with_session(url: str)` context manager that yields a `Session`, commits on success, rolls back on exception, and closes deterministically
- [ ] T013 Implement `src/paper_sorts/db/repositories.py`: define `PaperCreate` and `PaperSummary` pydantic models; implement `PaperRepository` (search_by_title, search_by_author, add_paper, delete_paper), `AuthorRepository` (upsert_author, link_to_paper, delete_orphans), `BibRepository` (add_bib, update_bib); all methods accept a `Session` parameter; no raw SQL strings outside db/
- [ ] T014 Implement `src/paper_sorts/cli/prompts.py`: all `input()`/`rich.prompt.Prompt.ask` calls live here; provide `ask_str(prompt)` (re-prompts on empty), `ask_choice(prompt, options)` (1-indexed, quit option, re-prompts on invalid), `ask_confirm(prompt)` (accepts 1/2 and y/n/yes/no), `ask_menu(title, options)` (numbered display)
- [ ] T015 Create `migrations/versions/001_initial_schema.py`: Alembic migration that creates the four tables with `CREATE TABLE IF NOT EXISTS` using verbatim DDL from `DatabaseConnector.create_tables()` (uses `bibtex_id` canonical column names)
- [ ] T016 Create `migrations/versions/002_converge_legacy.py`: Alembic migration that detects whether the `bib` and `papers` tables have `bibtext_id` (typo) columns and renames them to `bibtex_id`; idempotent (checks column existence before renaming)

**Checkpoint**: Foundation ready — user story implementation can now begin.

---

## Phase 3: User Story 1 — Architecture Document (Priority: P1)

**Goal**: Produce `docs/architecture.md` as the acceptance reference for US2 through US5 (FR-001, SC-001).

**Independent Test**: Hand the document to a Python developer who has never seen the project. They can describe the four tables, trace one user operation end-to-end, and answer what happens on a partial-add failure — without reading source.

- [ ] T017 [US1] Verify `docs/architecture.md` covers all required sections: purpose, user journeys, data model, control flow, configuration, install/run, known limitations, and where-to-add-a-new-field (document was created in plan phase; this task validates it against FR-001 acceptance scenarios and fixes any gaps)

**Checkpoint**: US1 complete — architecture document is the acceptance reference for all remaining stories.

---

## Phase 4: User Story 2 — Modernized Codebase, Same Behavior (Priority: P1) MVP

**Goal**: End user can run `pdbsearch` and get all existing CLI operations against their personal database, with the same prompts and output format, backed by SQLAlchemy + Typer + Alembic.

**Independent Test**: Run scripted dialog through every CLI path (search by title — single match; search by title — multiple matches; search by author; add inline; add from .bib; update title; update bibtex; abort update; delete; quit). All paths produce equivalent output against seeded data.

### Implementation for User Story 2

- [ ] T018 [P] [US2] Implement `src/paper_sorts/services/paper_service.py`: `search_by_title(session, title) -> list[PaperSummary]`, `search_by_author(session, author) -> list[PaperSummary]`, `add_paper(session, paper: PaperCreate) -> None`, `update_field(session, table: Literal["papers","bib","authors_id"], identifier: str, column: str, value: str) -> None` (with `assert_never`), `delete_paper(session, bibtex_id: str) -> None`; pure orchestration; no SQL, no I/O, no rich
- [ ] T019 [P] [US2] Implement `src/paper_sorts/services/import_service.py`: `extract_papers_from_tex_bib(tex_path: str, bib_path: str) -> Iterator[PaperCreate]`; uses pylatexenc + pybtex; yields one `PaperCreate` per matched entry; logs warning on unmatched cite key; pure data pipeline
- [ ] T020 [US2] Implement `src/paper_sorts/cli/search.py`: Typer subcommand `search`; calls `prompts.ask_menu` for author/title choice; calls `paper_service.search_by_title` or `search_by_author`; disambiguation menu when multiple results; pretty-prints with Rich; all prompts through `cli/prompts.py`
- [ ] T021 [US2] Implement `src/paper_sorts/cli/add.py`: Typer subcommand `add`; prompts for author list (comma-sep), title, BibTeX key, BibTeX source (inline or from file), summary; confirmation before writing; calls `paper_service.add_paper`; all prompts through `cli/prompts.py`
- [ ] T022 [US2] Implement `src/paper_sorts/cli/update.py`: Typer subcommand `update`; table menu (papers/bib/authors/abort), column menu (per table), identifier prompt, new-value prompt, confirmation; calls `paper_service.update_field`; all prompts through `cli/prompts.py`
- [ ] T023 [US2] Implement `src/paper_sorts/cli/delete.py`: Typer subcommand `delete`; prompts for BibTeX key; fetches and displays matching paper; confirmation before deleting; calls `paper_service.delete_paper`; all prompts through `cli/prompts.py`
- [ ] T024 [US2] Implement `src/paper_sorts/cli/importer.py`: Typer subcommand `import` (admin only, not in top-level menu); `--tex` and `--bib` path options; iterates `import_service.extract_papers_from_tex_bib`; calls `paper_service.add_paper` per entry inside individual `with_session` calls (per-paper commit); logs skips
- [ ] T025 [US2] Implement `src/paper_sorts/cli/app.py`: top-level Typer app; registers all subcommands; `--database-url`, `--log-level`, `--config`, `--key` global options; when invoked with no subcommand drops into four-option interactive menu (1=search, 2=add, 3=update, 4=quit); `migrate` and `import` are subcommand-only; calls `logging_config.setup()` at startup; loads `Settings` from config
- [ ] T026 [US2] Add `[project.scripts]` entry in `pyproject.toml`: `pdbsearch = "paper_sorts.cli.app:app"`; run `uv sync` to validate entry point resolves
- [ ] T027 [US2] Remove legacy flat-layout modules: delete `paper_sorts/add.py`, `paper_sorts/search.py`, `paper_sorts/get_data.py`, `paper_sorts/helpers.py`, `paper_sorts/psycopg_db.py`, `paper_sorts/database_connector.py`, `paper_sorts/user_interaction.py`, `paper_sorts/config_reader.py`, `paper_sorts/run.py`, `paper_sorts/__init__.py` (FR-012)

**Checkpoint**: US2 complete — `pdbsearch` is the single entry point; legacy modules removed.

---

## Phase 5: User Story 3 — Reproducible Test Suite (Priority: P2)

**Goal**: `uv sync --all-extras && uv run pytest` passes on a fresh machine with no personal database.

**Independent Test**: Clone repo on a machine with no `database.crypt` and no `key` file. Run `uv run pytest`. All tests pass.

### Implementation for User Story 3

- [ ] T028 [US3] Create `tests/conftest.py`: `postgresql_proc` fixture using `pytest_postgresql.factories.postgresql_proc` pointing to `/usr/bin/pg_ctl`; `ephemeral_db_url` fixture that builds a SQLAlchemy DSN from the proc; `seeded_session` fixture that runs Alembic migrations against the ephemeral DB and inserts `SEED_PAPERS`
- [ ] T029 [P] [US3] Create `tests/fixtures/seed_papers.py`: define `SEED_PAPERS` as a list of `PaperCreate` objects covering at minimum: one paper with a single author, one paper with multiple authors, one paper used for title disambiguation (same title as another), one paper for update tests, one paper for delete tests
- [ ] T030 [P] [US3] Create `tests/test_repositories.py`: integration tests using `seeded_session`; test `search_by_title` (found, not found, multiple matches), `search_by_author` (found, not found), `add_paper` (success, duplicate bibtex_id raises), `update_field` (title, contents, bibtex, author), `delete_paper` (removes all four table rows); no mocking
- [ ] T031 [P] [US3] Create `tests/test_migrations.py`: integration test that runs migration 001 on an empty DB and verifies all four tables exist; runs migration 002 on a DB with the `bibtext_id` typo column and verifies it is renamed; verifies both migrations are idempotent
- [ ] T032 [P] [US3] Create `tests/test_import_service.py`: integration + unit tests for `extract_papers_from_tex_bib`; fixture `.tex` + `.bib` pair with N entries; verify N `PaperCreate` objects yielded; verify missing bib key logs warning and skips; test round-trip of LaTeX accents
- [ ] T033 [P] [US3] Create `tests/test_config.py`: unit tests for `Settings`; verify env var override, `.env` file loading, missing key file produces actionable error (not stack trace), invalid log level rejected

**Checkpoint**: US3 complete — `uv run pytest` passes on fresh machine.

---

## Phase 6: User Story 4 — One-Shot Migration (Priority: P2)

**Goal**: User runs `pdbsearch migrate` and their personal database is upgraded with all data preserved.

**Independent Test**: Take snapshot of personal DB. Run `pdbsearch migrate`. Compare paper count, author count, authorship link count — must match. Spot-check a few rows for content equality.

### Implementation for User Story 4

- [ ] T034 [US4] Implement `src/paper_sorts/cli/migrate.py`: Typer subcommand `migrate`; `--revision` option (default `head`); calls Alembic `command.upgrade`; prints confirmation on success; catches and converts Alembic errors to plain-language user messages; no raw exceptions on stdout
- [ ] T035 [US4] Wire `migrate` subcommand into `src/paper_sorts/cli/app.py` (subcommand-only; not in top-level interactive menu)
- [ ] T036 [US4] Add migration idempotency test to `tests/test_migrations.py`: run `pdbsearch migrate` twice against the same ephemeral DB; verify second run exits 0 with "already up to date" (or equivalent) and row counts unchanged

**Checkpoint**: US4 complete — `pdbsearch migrate` upgrades any historical schema variant idempotently.

---

## Phase 7: User Story 5 — Bulk Import Preserved (Priority: P3)

**Goal**: `pdbsearch import --tex FILE --bib FILE` inserts all matched entries; unmatched keys are skipped with a warning; partial failures are recoverable.

**Independent Test**: Run `pdbsearch import` against a fixture pair with N entries. Verify N papers, authors, and BibTeX entries are present after import.

### Implementation for User Story 5

- [ ] T037 [US5] Create `tests/fixtures/literature_overview.tex` and `tests/fixtures/bib.bib` with at least 3 entries (including one cite key with no matching .bib record)
- [ ] T038 [US5] Add end-to-end import test to `tests/test_import_service.py`: call `import_service.extract_papers_from_tex_bib` with fixture files; for each yielded `PaperCreate`, call `paper_service.add_paper`; verify DB contains correct paper/author/bib counts; verify missing key was logged and skipped; verify re-running import skips already-inserted entries (idempotent via bibtex_id uniqueness)

**Checkpoint**: US5 complete — bulk import from tex+bib works end-to-end.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Quality gates, cleanup, documentation updates.

- [ ] T039 Run `uv run ruff check src tests` and fix all lint errors
- [ ] T040 Run `uv run ruff format src tests` and fix all format issues
- [ ] T041 Run `uv run mypy src` and fix all type errors (strict mode per constitution Principle I)
- [ ] T042 Run `uv run pytest --cov=src/paper_sorts --cov-report=term-missing` and verify coverage >= 80% on persistence layer (SC-008)
- [ ] T043 [P] Update `README.md` to reflect modern install/run commands (`uv sync`, `uv run pdbsearch`, etc.)
- [ ] T044 [P] Verify `docs/architecture.md` accurately describes the modernized architecture (update "Modernized Architecture" section if needed, or mark legacy sections clearly)
- [ ] T045 Run `uv run pytest` final clean run and confirm all tests pass with no placeholder failures
- [ ] T046 Run `uv run pdbsearch --help` and all subcommand `--help` outputs; verify they are informative and consistent

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately.
- **Foundational (Phase 2)**: Depends on Phase 1 — BLOCKS all user stories.
  - T009 (config) and T010 (logging) can start as soon as T001 (pyproject) is done.
  - T011 (models) can start in parallel with T009/T010.
  - T012 (session) depends on T011.
  - T013 (repositories) depends on T011, T012.
  - T014 (prompts) depends on T001 only.
  - T015, T016 (migrations) depend on T011.
- **User Stories (Phases 3–7)**: All depend on Phase 2 completion.
  - US1 (T017): depends on docs/architecture.md from plan phase — verify only.
  - US2 (T018–T027): T018 and T019 can run in parallel; T020–T025 each depend on T018; T025 (app.py) depends on T020–T024; T026 depends on T025; T027 (delete legacy) depends on T026.
  - US3 (T028–T033): T029–T033 can run in parallel after T028 (conftest).
  - US4 (T034–T036): T034 depends on T015/T016; T035 depends on T034; T036 depends on T035.
  - US5 (T037–T038): T038 depends on T037 and T019.
- **Polish (Phase 8)**: Depends on all desired user stories complete.

### User Story Dependencies

- **US1**: No dependencies on other stories.
- **US2**: Depends on Foundation (Phase 2). No dependencies on US1.
- **US3**: Depends on Foundation (Phase 2) + US2 (needs repositories + services). Can be built in parallel with US2 if repositories are done first.
- **US4**: Depends on Foundation (Phase 2, specifically T015/T016). T034 can be built in parallel with US2.
- **US5**: Depends on US2 (needs import_service from T019). T037 is independent.

### Parallel Opportunities Within Phase 2

```bash
# All can start immediately after T001:
T009  # config.py
T010  # logging_config.py
T014  # prompts.py

# After T001, in parallel:
T011  # models.py (needed by T012, T013, T015, T016)

# After T011:
T012  # session.py
T015  # migration 001
T016  # migration 002

# After T011 + T012:
T013  # repositories.py
```

### Parallel Opportunities Within Phase 4 (US2)

```bash
# Start in parallel:
T018  # paper_service.py
T019  # import_service.py

# After T018:
T020  # cli/search.py
T021  # cli/add.py
T022  # cli/update.py
T023  # cli/delete.py
T024  # cli/importer.py

# After T020–T024:
T025  # cli/app.py
T026  # entry point
T027  # delete legacy
```

---

## Implementation Strategy

### MVP First (US1 + US2 Only)

1. Complete Phase 1: Setup (T001–T008)
2. Complete Phase 2: Foundation (T009–T016)
3. Verify US1: T017 (architecture doc)
4. Implement US2: T018–T027
5. **STOP and VALIDATE**: `uv run pdbsearch`, run all CLI paths manually
6. Proceed to US3 (tests) to make the suite portable

### Incremental Delivery

1. Setup + Foundation → modern scaffold ready
2. US1 → architecture reference
3. US2 → working modernized CLI (MVP)
4. US3 → portable test suite (CI-ready)
5. US4 → migration command
6. US5 → bulk import verified end-to-end
7. Polish → all quality gates green

---

## Notes

- `[P]` tasks involve different files and have no dependencies on incomplete tasks — can run in parallel.
- `[Story]` label maps each task to a specific user story for traceability.
- Delete legacy flat layout (T027) only after the new entry point (`pdbsearch`) is verified working.
- Constitution Principle III: every menu must be 1-indexed and include an explicit abort/quit option.
- Constitution Principle II: no mocking SQLAlchemy session or repositories in integration tests.
- Schema preservation: do NOT add NOT NULL columns outside PKs, no new FKs on `authors_papers`, no new indexes.
