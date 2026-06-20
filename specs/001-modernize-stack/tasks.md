---

description: "Task list for 001-modernize-stack: Modernize the Stack"
---

# Tasks: Modernize the Stack

**Input**: Design documents from `/specs/001-modernize-stack/`
**Prerequisites**: plan.md ✅, spec.md ✅, research.md ✅, data-model.md ✅, contracts/cli-commands.md ✅, quickstart.md ✅

**Tests**: Included per spec.md US3 (reproducible test suite) and SC-008 (≥80% coverage on persistence layer).

**Organization**: Tasks grouped by user story to enable independent implementation and testing.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization — new pyproject.toml, uv, src-layout skeleton, quality tool configs.

- [X] T001 Convert pyproject.toml to PEP 621 format (hatchling build backend, uv, Python ≥3.11, entry point `pdbsearch`) at `pyproject.toml`
- [X] T002 [P] Create src-layout skeleton: `src/paper_sorts/__init__.py`, `src/paper_sorts/cli/__init__.py`, `src/paper_sorts/services/__init__.py`, `src/paper_sorts/db/__init__.py`
- [X] T003 [P] Add ruff and mypy configuration sections to `pyproject.toml` (ruff rules, mypy strict mode on src/)
- [X] T004 [P] Create `docs/` directory and stub `docs/architecture.md` (placeholder — filled in US1)
- [X] T005 Run `uv sync --all-extras` to verify dependency resolution and create `uv.lock`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core DB layer, config, logging, and Alembic migrations that ALL user stories depend on.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [X] T006 Create `src/paper_sorts/db/models.py` — SQLAlchemy 2.x declarative ORM models: `Paper`, `Bib`, `Author`, `AuthorPaper` (four tables, no DDL FKs on `authors_papers`, schema-preservation rules from data-model.md)
- [X] T007 Create `src/paper_sorts/db/session.py` — `get_engine(url: str) -> Engine`, `with_session(engine)` context manager (commit on success, rollback on exception)
- [X] T008 Create `src/paper_sorts/db/repositories.py` — `PaperSummary` and `PaperCreate` pydantic DTOs; `PaperRepository` (search_by_title, search_by_author, add_paper, delete_paper), `AuthorRepository`, `BibRepository`
- [X] T009 Create `src/paper_sorts/config.py` — pydantic-settings `Settings` model with `database_url`, `log_level`, `config_file`, `key_file`; custom `FernetIniSettingsSource` that reads Fernet-encrypted INI; four-source priority chain (CLI > env > .env > Fernet INI)
- [X] T010 Create `src/paper_sorts/logging_config.py` — single `logging.config.dictConfig` call; `RichHandler` to stdout + optional `FileHandler`; `configure_logging(level: str) -> None`
- [X] T011 Initialize Alembic in `migrations/` — `alembic.ini` at repo root, `migrations/env.py` connected to `Settings.database_url`, `migrations/script.py.mako`
- [X] T012 Write Alembic Revision 001 (`migrations/versions/001_initial_schema.py`) — `CREATE TABLE IF NOT EXISTS` for all four tables in canonical schema (`bibtex_id` column name, no extra NOT NULL, no extra FKs, no extra indexes)
- [X] T013 Write Alembic Revision 002 (`migrations/versions/002_converge_legacy_schema.py`) — detect `bibtext_id` (typo) columns; if present, rename `bibtext_id`→`bibtex_id` in `papers` and rename `bibtext_id`→`bibtex_id` + `bibtext`→`bibtex` in `bib`; idempotent; reversible downgrade

**Checkpoint**: Foundation ready — DB layer, config, logging, and migrations complete. User story implementation can now begin.

---

## Phase 3: User Story 1 — Architecture Document (Priority: P1)

**Goal**: Deliver `docs/architecture.md` — the reverse-engineered reference document for the legacy codebase that also serves as the acceptance reference for US2.

**Independent Test**: Hand the document to a reviewer. They can describe the four DB tables, trace "search by author" from prompt to SQL, and answer "what happens on a partial add failure?" without opening source.

### Implementation for User Story 1

- [X] T014 [US1] Write `docs/architecture.md` covering: purpose, user journeys (search/add/update/delete/import), data model (four tables + relationships), control flow (CLI → domain → DB), configuration approach (Fernet INI), install/run instructions, known limitations (identical-author dedup, bibtext_id typo history, rollback semantics on partial add)

**Checkpoint**: US1 complete. Architecture document is ready; US2 implementation can reference it.

---

## Phase 4: User Story 2 — Modernized CLI with Same Behaviour (Priority: P1) 🎯 MVP

**Goal**: `pdbsearch` CLI rebuilt on Typer, with all five operations (search, add, update, delete, import) preserving user-facing behaviour. Interactive top-level menu when invoked with no subcommand.

**Independent Test**: Run scripted dialog through every CLI path against a seeded DB: search by title (one match), search by title (multiple matches → disambiguation), search by author, add inline, add from .bib, update title confirmed, update aborted, delete confirmed, quit. All paths produce equivalent output.

### Implementation for User Story 2

- [X] T015 [US2] Create `src/paper_sorts/cli/prompts.py` — all prompt functions: `ask_input(prompt: str) -> str` (re-prompts on empty), `ask_choice(items: list[str], prompt: str) -> int` (1-indexed, quit option, re-prompts on invalid), `ask_confirmation(message: str) -> bool` (accepts 1/2 and y/n/yes/no)
- [X] T016 [US2] Create `src/paper_sorts/services/paper_service.py` — `search_by_title(session, title) -> list[PaperSummary]`, `search_by_author(session, author) -> list[PaperSummary]`, `add_paper(session, paper: PaperCreate) -> None`, `update_field(session, table: Literal["papers","bib","authors_id"], column: str, identifier: str, value: str) -> None` (match/case with assert_never), `delete_paper(session, paper_id: int) -> None`
- [X] T017 [US2] Create `src/paper_sorts/cli/search.py` — Typer `search` command; calls prompts for search type and term; calls paper_service; formats output (`title/authors/summary/bib entry`); handles disambiguation list when multiple results
- [X] T018 [US2] Create `src/paper_sorts/cli/add.py` — Typer `add` command; prompts for author(s)/title/bibtex_key/bibtex_source (file or inline)/summary; confirmation before write; calls paper_service.add_paper
- [X] T019 [US2] Create `src/paper_sorts/cli/update.py` — Typer `update` command; prompts for table/column/identifier/new_value; confirmation step summarising exact change; calls paper_service.update_field
- [X] T020 [US2] Create `src/paper_sorts/cli/delete.py` — Typer `delete` command; search for paper first; show paper summary; confirmation; calls paper_service.delete_paper
- [X] T021 [US2] Create `src/paper_sorts/cli/migrate.py` — Typer `migrate` command; runs `alembic upgrade head` programmatically via alembic.config
- [X] T022 [US2] Create `src/paper_sorts/cli/app.py` — root Typer app; registers all subcommands; `@app.callback(invoke_without_command=True)` drops into four-option interactive menu when no subcommand given; calls `configure_logging` and initialises `Settings` at startup; wires engine from `database_url`

**Checkpoint**: US2 complete. All CLI paths functional. Run scripted acceptance test.

---

## Phase 5: User Story 3 — Reproducible Test Suite (Priority: P2)

**Goal**: `pytest` suite with ephemeral PostgreSQL; no personal DB or credential files required; seed data co-located with tests; integration tests verify real SQL.

**Independent Test**: `git clone && uv sync --all-extras && uv run pytest` passes on a machine with no personal database.

### Tests for User Story 3

- [X] T023 [P] [US3] Create `tests/conftest.py` — `postgresql_proc` fixture (pytest-postgresql, host `pg_ctl` at `/usr/bin/pg_ctl`); `ephemeral_db_url` fixture (SQLAlchemy URL string); `db_engine` fixture (creates all tables via Alembic or `Base.metadata.create_all`); `db_session` fixture (yields session, rolls back after each test)
- [X] T024 [P] [US3] Create `tests/fixtures/seed_papers.py` — `SEED_PAPERS: list[PaperCreate]` with ≥5 diverse entries (different authors, titles, bibtex keys); inline comment explains which tests use which rows

### Implementation for User Story 3

- [X] T025 [US3] Create `tests/test_repositories.py` — integration tests against real ephemeral DB: search_by_title (single match, multiple matches, no match), search_by_author (found, not found), add_paper (persists and retrievable), delete_paper (removes paper + authors + bib), update_field (papers.title, papers.contents, bib.bibtex, authors_id.author), duplicate bibtex_id rejected
- [X] T026 [US3] Create `tests/test_paper_service.py` — service-layer tests using ephemeral DB session: mirrors repository tests but calls paper_service functions; ensures service correctly maps DTOs

**Checkpoint**: US3 complete. `uv run pytest` passes on ephemeral DB with no personal credentials.

---

## Phase 6: User Story 4 — One-Shot Personal Database Migration (Priority: P2)

**Goal**: `pdbsearch migrate` upgrades a personal database from either historical schema to the canonical schema, zero data loss, idempotent.

**Independent Test**: Snapshot a legacy DB (row counts). Run `pdbsearch migrate`. Row counts match exactly. Rerun is a no-op.

### Implementation for User Story 4

- [X] T027 [US4] Add migration test in `tests/test_repositories.py` — test Revision 002: create table with `bibtext_id` (old column), run `alembic upgrade head`, verify column renamed to `bibtex_id`; verify downgrade restores old name; verify idempotent (upgrade twice is a no-op)
- [X] T028 [US4] Verify `pdbsearch migrate` command (T021) prints progress and handles "already at head" gracefully with clear message

**Checkpoint**: US4 complete. Migration is safe, reversible, and idempotent.

---

## Phase 7: User Story 5 — Bulk Import from LaTeX/BibTeX (Priority: P3)

**Goal**: `pdbsearch import --tex FILE --bib FILE` inserts all cited entries, skips duplicates, commits per-paper.

**Independent Test**: Run import against fixture `.tex` + `.bib` with N entries. Verify N papers in DB. Rerun — still N papers (no duplicates). Interrupt midway — prior entries intact.

### Implementation for User Story 5

- [X] T029 [P] [US5] Create `src/paper_sorts/services/import_service.py` — `extract_papers_from_tex_bib(tex_path: Path, bib_path: Path) -> Iterator[PaperCreate]`; uses `pylatexenc` for .tex parsing and `pybtex` for .bib parsing; skips entries with no matching bib record (logs warning); preserves LaTeX accents in round-trip
- [X] T030 [P] [US5] Create `src/paper_sorts/cli/importer.py` — Typer `import` command with `--tex PATH` and `--bib PATH` options; calls import_service; calls paper_service.add_paper per entry inside its own `with_session`; logs skipped/duplicate entries; per-paper commit semantics
- [X] T031 [US5] Create `tests/test_import_service.py` — integration tests: import fixture .tex + .bib pair; verify papers/authors/bib persisted; verify duplicate bibtex_id skipped (not duplicated); verify missing-bib entry skipped with log warning; verify LaTeX accent round-trip (`\"o` in author name)
- [X] T032 [US5] Add fixture test files `tests/fixtures/sample.tex` and `tests/fixtures/sample.bib` with ≥3 entries (one with missing bib record for skip-test)

**Checkpoint**: US5 complete. Bulk import works, is idempotent, and commits per-paper.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Remove legacy flat layout, verify quality gates, update README.

- [X] T033 Remove legacy flat-layout modules per FR-012: delete `paper_sorts/add.py`, `paper_sorts/search.py`, `paper_sorts/get_data.py`, `paper_sorts/database_connector.py`, `paper_sorts/psycopg_db.py`, `paper_sorts/user_interaction.py`, `paper_sorts/helpers.py`, `paper_sorts/config_reader.py`, `paper_sorts/run.py`, `paper_sorts/__init__.py`; remove `paper_sorts/` directory
- [X] T034 Remove legacy test files: delete `tests/test_database_connector.py` (personal-DB integration test), `tests/test_user_interaction.py` (always-failing placeholder)
- [X] T035 Update `README.md` to reflect new install/run commands (`uv sync`, `uv run pdbsearch`), subcommands, and configuration sources
- [X] T036 [P] Run `uv run ruff check src tests` — fix all lint errors
- [X] T037 [P] Run `uv run mypy src` — fix all type errors (strict mode on src/)
- [X] T038 Run `uv run pytest` — confirm full suite passes (ephemeral DB, ≥80% statement coverage on persistence layer per SC-008)
- [X] T039 Verify SC-005: count Python lines under `src/paper_sorts/` (excluding tests and migrations); confirm ≥30% reduction vs. legacy ~2000 lines

**Checkpoint**: All quality gates green. Legacy code removed. Feature complete.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 completion — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Phase 2 (needs schema understanding) — no code deps on other stories
- **US2 (Phase 4)**: Depends on Phase 2 — depends on T014 (US1 architecture doc as acceptance reference)
- **US3 (Phase 5)**: Depends on Phase 2 + Phase 4 (tests exercise the service/repo layer)
- **US4 (Phase 6)**: Depends on T011–T013 (Alembic) + T021 (migrate command) + T023 (test fixture)
- **US5 (Phase 7)**: Depends on Phase 2 (session, repos) + T022 (app wiring)
- **Polish (Phase 8)**: Depends on ALL user stories complete

### Within Each User Story

- Models → Repositories → Services → CLI commands
- Prompts module (T015) before any CLI command that prompts
- Service layer (T016) before CLI commands (T017–T020)

### Parallel Opportunities

- T002, T003, T004 (Phase 1 setup tasks) — parallel
- T006, T009, T010 (models, config, logging) — parallel within Phase 2
- T011, T012, T013 (Alembic) — sequential (012 depends on 011; 013 depends on 012)
- T023, T024 (test conftest + seed) — parallel
- T029, T030 (import_service + importer CLI) — parallel
- T036, T037 (ruff, mypy) — parallel

---

## Parallel Execution Examples

### Phase 2 Parallel Start

```bash
# These can run concurrently (different files):
Task T006: Create src/paper_sorts/db/models.py
Task T009: Create src/paper_sorts/config.py
Task T010: Create src/paper_sorts/logging_config.py
```

### US5 Parallel Start

```bash
Task T029: Create src/paper_sorts/services/import_service.py
Task T030: Create src/paper_sorts/cli/importer.py
```

---

## Implementation Strategy

### MVP First (US1 + US2 — P1 stories)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: US1 (architecture doc)
4. Complete Phase 4: US2 (full CLI rebuild)
5. **STOP and VALIDATE**: Run scripted acceptance test (contracts/cli-commands.md)

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. US1 (architecture doc) → acceptance reference
3. US2 (CLI rebuild) → MVP usable CLI
4. US3 (test suite) → reproducible CI
5. US4 (migration) → personal DB upgrade safe
6. US5 (bulk import) → full feature parity
7. Polish → legacy deleted, quality gates green

---

## Notes

- [P] tasks = parallelizable (different files, no outstanding dependencies)
- [USn] label maps task to specific user story from spec.md
- Constitution Principle II: tests use real ephemeral PostgreSQL — no mocking sessions/repos/driver
- Constitution Principle III: ALL prompts through `cli/prompts.py` only (T015 is a prerequisite for T017–T020)
- Schema preservation: no NOT NULL outside PKs, no DDL FKs on `authors_papers`, no extra indexes
- Every commit after a logical group (minimum: one commit per Phase)
