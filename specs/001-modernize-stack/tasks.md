# Tasks: Modernize the Stack

**Input**: Design documents from `/specs/001-modernize-stack/`
**Prerequisites**: plan.md ✅, spec.md ✅, research.md ✅, data-model.md ✅, contracts/cli-commands.md ✅, quickstart.md ✅

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Replace legacy pyproject.toml / flat layout with uv + src-layout + modern toolchain.

- [x] T001 Replace pyproject.toml with PEP 621 hatchling build (uv, Python ≥ 3.11, src-layout) and run `uv sync --all-extras` to verify
- [x] T002 [P] Create `src/paper_sorts/` src-layout package skeleton (`__init__.py`, `cli/`, `services/`, `db/`) with empty `__init__.py` files
- [x] T003 [P] Add `pyproject.toml` ruff config (`[tool.ruff]`, `[tool.ruff.lint]`) targeting src/ and tests/; verify `uv run ruff check src` exits 0
- [x] T004 [P] Add `pyproject.toml` mypy config (`[tool.mypy]` strict on src/paper_sorts); verify `uv run mypy src` exits 0 on empty stubs
- [x] T005 [P] Add `pyproject.toml` pytest config (`[tool.pytest.ini_options]` testpaths=tests, asyncio_mode off); install pytest-postgresql; verify `uv run pytest --collect-only` exits 0

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [x] T006 Create `src/paper_sorts/db/models.py` with SQLAlchemy 2.x `DeclarativeBase` and four ORM models: `Paper`, `Bib`, `Author`, `AuthorPaper` matching canonical schema (no extra FKs on `authors_papers`, `bibtex` column UNIQUE on `Bib`)
- [x] T007 Create `src/paper_sorts/db/session.py` with `with_session()` context-manager factory using `sessionmaker(bind=engine)` and `create_engine()` driven by Settings; commits on success, rolls back on exception
- [x] T008 Create `src/paper_sorts/db/repositories.py` with Pydantic DTOs (`PaperSummary`, `PaperCreate`) and repository classes `PaperRepository`, `AuthorRepository`, `BibRepository`; no sqlalchemy imports above this module
- [x] T009 Create `src/paper_sorts/config.py` with pydantic-settings v2 `Settings` model: `database_url`, `log_level`; env prefix `PDBSEARCH_`; custom `FernetIniSettingsSource` that decrypts Fernet-encrypted INI to read `[postgresql]` section; four-source priority chain (CLI args injected externally > env > .env > encrypted INI)
- [x] T010 Create `src/paper_sorts/logging_config.py` with `configure_logging(level)` calling `logging.config.dictConfig`; RichHandler to stdout + optional FileHandler; called once from `cli/app.py` at startup
- [x] T011 Create `alembic.ini` and `migrations/env.py` wiring Alembic to `Settings.database_url` and `src/paper_sorts/db/models.py` metadata
- [x] T012 Create `migrations/versions/001_initial_schema.py` — verbatim port of four-table DDL from legacy `database_connector.py` (`CREATE TABLE bib`, `papers`, `authors_id`, `authors_papers`); no extra constraints beyond originals
- [x] T013 Create `migrations/versions/002_handle_legacy_bibtext_id.py` — idempotent guard: if column `bibtext_id` exists in `papers`, rename it to `bibtex_id`; skip if `bibtex_id` already present
- [x] T014 [P] Create `src/paper_sorts/cli/prompts.py` — sole importer of `rich.prompt`; exports `ask_str(prompt, required=True)`, `ask_int(prompt, choices)`, `ask_confirm(prompt)`, `display_paper(summary)`, `display_papers_list(summaries)`; empty input re-prompts when `required=True`; `ask_confirm` accepts `y/yes/1/n/no/2` case-insensitively

**Checkpoint**: Foundation ready — user story implementation can now begin.

---

## Phase 3: User Story 1 — Architecture Document + Modernized CLI (Priority: P1) 🎯 MVP

**Goal**: Produce architecture.md (US1) and the fully working modernized CLI covering search, add, update, delete (US2 core). Both stories are P1 and share the same foundational layer.

**Independent Test**: `uv run pdbsearch --help` exits 0; `uv run pytest tests/test_repositories.py tests/test_services.py tests/test_cli.py` all pass.

- [x] T015 [US1] Create `docs/architecture.md` documenting purpose, user journeys, data model (four tables + relationships), control flow (CLI → service → repository → DB), configuration chain, install/run instructions, known limitations (duplicate author names, no FK on authors_papers, Ctrl+C rollback semantics via context-managed session)
- [x] T016 [US1] [P] Create `src/paper_sorts/services/paper_service.py` with `search_by_title(session, title) -> list[PaperSummary]`, `search_by_author(session, author) -> list[PaperSummary]`, `add_paper(session, paper: PaperCreate) -> PaperSummary`, `update_field(session, paper_id, field: Literal["title","contents","bibtex","authors"], value) -> PaperSummary`, `delete_paper(session, paper_id) -> None`; pure orchestration, no SQL, no I/O; `update_field` uses `match`/`case` with `assert_never`
- [x] T017 [US1] [P] Create `src/paper_sorts/services/import_service.py` with `extract_papers_from_tex_bib(tex_content: str, bib_content: str) -> Iterator[PaperCreate]`; uses pybtex for BibTeX parsing, pylatexenc for LaTeX-to-text; skips missing BibTeX records with logged warning
- [x] T018 [US1] Create `src/paper_sorts/cli/app.py` — Typer app `app`; `main()` entry point called by `pdbsearch` script; registers subcommands; when invoked with no subcommand drops into four-option interactive menu (Search/Add/Update/Delete/Quit) via `prompts.ask_int`; calls `configure_logging` at startup; accepts `--database-url`, `--log-level`, `--config`, `--key` global options
- [x] T019 [US1] Create `src/paper_sorts/cli/search.py` — Typer subapp with `search_by_title_cmd` and `search_by_author_cmd`; uses `with_session()`; calls `paper_service.search_by_title/search_by_author`; disambiguation prompt for multiple matches via `prompts.ask_int`; plain-language "No papers found" on empty result; displays via `prompts.display_paper`
- [x] T020 [US1] Create `src/paper_sorts/cli/add.py` — Typer subapp `add_cmd`; prompts for title, authors (comma-separated), bibtex_id, contents, bibtex entry or .bib file path; calls `paper_service.add_paper`; re-prompts on empty required fields; plain-language error on duplicate bibtex_id
- [x] T021 [US1] Create `src/paper_sorts/cli/update.py` — Typer subapp `update_cmd`; accepts `--id`; if no id, searches first; shows current values; submenu (title/contents/bibtex/authors/Abort); prompts new value; confirmation via `prompts.ask_confirm`; aborts on `n`; calls `paper_service.update_field`
- [x] T022 [US1] Create `src/paper_sorts/cli/delete.py` — Typer subapp `delete_cmd`; accepts `--id`; if no id, searches first; shows paper details; confirmation via `prompts.ask_confirm`; calls `paper_service.delete_paper`; aborts on `n`
- [x] T023 [US1] [P] Create `tests/conftest.py` with `postgresql_proc` fixture (pg_ctl at `/usr/bin/pg_ctl`) and `ephemeral_db_url` fixture that runs Alembic migrations then yields the URL; add `tests/fixtures/seed_papers.py` with `SEED_PAPERS` list of `PaperCreate` instances covering: one unique title, two papers with same title, one paper with LaTeX-accent author name, one paper with multi-author
- [x] T024 [US1] [P] Create `tests/test_repositories.py` — integration tests against ephemeral DB: `test_add_and_find_by_title`, `test_add_and_find_by_author`, `test_update_title`, `test_delete_paper`, `test_duplicate_bibtex_id_rejected`, `test_bibtex_unique_constraint`; uses real SQLAlchemy session (no mocking)
- [x] T025 [US1] [P] Create `tests/test_services.py` — service-layer tests using the ephemeral DB session: `test_search_by_title_one_match`, `test_search_by_title_multiple`, `test_search_by_author`, `test_add_paper`, `test_update_field_title`, `test_update_field_contents`, `test_update_field_bibtex`, `test_update_field_authors`, `test_delete_paper`, `test_update_field_invalid_exhaustiveness` (assert_never path)
- [x] T026 [US1] [P] Create `tests/test_cli.py` — CLI layer tests via Typer `CliRunner`: `test_search_subcommand_no_results`, `test_search_subcommand_one_result`, `test_add_subcommand`, `test_update_subcommand`, `test_delete_subcommand`, `test_main_help`; patch `with_session` to inject ephemeral session
- [x] T027 [US1] [P] Create `tests/test_config.py` — unit tests for `Settings`: `test_env_var_overrides_default`, `test_dotenv_loaded`, `test_fernet_ini_source_missing_key_raises_clear_error`, `test_empty_input_reprompt` (for `prompts.ask_str`)

**Checkpoint**: US1 + US2 core fully functional. `uv run pytest tests/` passes.

---

## Phase 4: User Story 3 — Reproducible Test Suite Completion (Priority: P2)

**Goal**: Ensure the test suite is self-contained — no developer-local DB, no personal credentials. All fixtures co-located. Per-layer 80% coverage gate met.

**Independent Test**: `git clone && uv sync --all-extras && uv run pytest` passes on a machine with no `database.crypt`.

- [x] T028 [US3] Create `tests/test_migrations.py` — test Revision 001 creates all four tables; test Revision 002 handles `bibtext_id` rename idempotently (simulate by adding the typo column before running migration); uses ephemeral DB
- [x] T029 [US3] Create `tests/test_doc_currency.py` — mechanical doc-currency gate: reads `README.md` and `CLAUDE.md`, asserts none of `["Poetry", "psycopg2", "UserInteraction", "PsycopgDB"]` appear (constitution Principle I gate G3)
- [x] T030 [US3] Run `uv run pytest --cov=src/paper_sorts --cov-report=term-missing` and verify per-layer coverage ≥ 80% for each of `db/`, `services/`, `cli/`, `config.py`; fix coverage gaps in existing tests if any layer is below threshold

**Checkpoint**: Test suite fully self-contained and per-layer coverage gates met.

---

## Phase 5: User Story 4 — One-Shot Personal DB Migration (Priority: P2)

**Goal**: `pdbsearch migrate` upgrades either historical schema variant with zero data loss.

**Independent Test**: Snapshot row counts before migration, run `pdbsearch migrate`, compare row counts — must match exactly.

- [x] T031 [US4] Create `src/paper_sorts/cli/migrate.py` — Typer command `migrate_cmd`; calls `alembic upgrade head` programmatically via Alembic's Python API; prints "Migration complete." on success; plain-language error on failure, technical detail in log
- [x] T032 [US4] Verify migration Revision 002 (`tests/test_migrations.py::test_legacy_bibtext_id_rename`) covers both schema variants (with and without typo column); extend test if missing

**Checkpoint**: Single `pdbsearch migrate` command handles both legacy schema variants.

---

## Phase 6: User Story 5 — Bulk Import (Priority: P3)

**Goal**: `pdbsearch import --tex TEX --bib BIB` inserts all cited entries, skips missing BibTeX records, commits per-paper.

**Independent Test**: Run import against fixture pair; verify paper count, author count, BibTeX entries match expected.

- [x] T033 [US5] Create `src/paper_sorts/cli/importer.py` — Typer command `import_cmd`; accepts `--tex PATH`, `--bib PATH`; calls `import_service.extract_papers_from_tex_bib`; for each yielded `PaperCreate` calls `paper_service.add_paper` in its own `with_session()` call (per-paper commit); logs WARNING on skip; prints "Imported N papers. Skipped M entries." on completion
- [x] T034 [US5] [P] Create `tests/fixtures/literature_overview.tex` and `tests/fixtures/bib.bib` fixture pair with at least 3 valid entries and 1 citation with no matching BibTeX record
- [x] T035 [US5] Create `tests/test_import.py` — integration tests: `test_bulk_import_all_valid`, `test_bulk_import_missing_bib_skipped`, `test_bulk_import_partial_failure_preserved`, `test_latex_accent_roundtrip`; uses ephemeral DB

**Checkpoint**: Bulk import fully functional and tested.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Cleanup, legacy removal, benchmark gate, doc-currency gate, final quality checks.

- [x] T036 Remove legacy flat-layout package: delete `paper_sorts/` directory entirely (all 9 modules: `run.py`, `user_interaction.py`, `database_connector.py`, `psycopg_db.py`, `helpers.py`, `config_reader.py`, `add.py`, `search.py`, `get_data.py`) and the old `paper_sorts/__init__.py`
- [x] T037 Update `README.md` to document uv-based install, `pdbsearch` entry point, migration command, and test run; MUST NOT contain forbidden tokens `Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB`; update `CLAUDE.md` architecture section to reflect modern src-layout
- [x] T038 Create `tests/benchmarks/bench_baseline.py` — benchmark harness marked `@pytest.mark.benchmark`; records wall-clock timings for search-by-title, search-by-author, add, update, delete on seeded data against ephemeral DB; writes results to `tests/benchmarks/baseline.json`; constitution Principle IV gate G2
- [x] T039 Run full quality gate: `uv run ruff check src tests`, `uv run ruff format --check src tests`, `uv run mypy src` — fix all errors
- [x] T040 Run `uv run pytest` and verify all tests pass; run `uv run pytest tests/benchmarks/ -m benchmark` and verify benchmark executes and writes `baseline.json`
- [x] T041 [P] Verify doc-currency gate: `uv run pytest tests/test_doc_currency.py` passes (no forbidden tokens in README.md or CLAUDE.md)
- [x] T042 [P] Review `src/paper_sorts/` for any bare `input()`, `rich.prompt.Prompt.ask`, or `typer.prompt` calls outside `cli/prompts.py`; fix any violations (constitution Principle III)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1, T001–T005)**: No dependencies — start immediately
- **Foundational (Phase 2, T006–T014)**: Depends on Phase 1 completion — blocks all user stories
- **US1+US2 (Phase 3, T015–T027)**: Depends on Phase 2 — core modernized CLI
- **US3 (Phase 4, T028–T030)**: Depends on Phase 3 (needs full test suite in place)
- **US4 (Phase 5, T031–T032)**: Depends on Phase 2 (Alembic already wired)
- **US5 (Phase 6, T033–T035)**: Depends on Phase 3 (import_service already built)
- **Polish (Phase 7, T036–T042)**: Depends on all story phases complete

### User Story Dependencies

- **US1 + US2 (P1)**: Start after Phase 2. Core of the modernization.
- **US3 (P2)**: Start after Phase 3 (coverage gate requires full test suite).
- **US4 (P2)**: Can start after Phase 2 (migration CLI is independent of search/add).
- **US5 (P3)**: Start after Phase 3 (reuses `import_service` built in T017).

### Within Each Story

- Models / DTOs (T006–T008) → Session / Config (T007, T009) → Repositories (T008) → Services (T016, T017) → CLI (T018–T022) → Tests (T023–T027)

### Parallel Opportunities

- T002, T003, T004, T005 can all run in parallel (Phase 1)
- T014 (prompts.py) can be written in parallel with T006–T013
- T016, T017 (services) can be written in parallel
- T023, T024, T025, T026, T027 (tests) can be written in parallel once T006–T022 complete
- T034 (fixture files) can be written in parallel with T033

---

## Parallel Example: Phase 3 (US1 core)

```bash
# Step 1 — parallel: services + prompts (no shared files)
Task T016: src/paper_sorts/services/paper_service.py
Task T017: src/paper_sorts/services/import_service.py

# Step 2 — parallel: CLI subcommands (no shared files)  
Task T019: src/paper_sorts/cli/search.py
Task T020: src/paper_sorts/cli/add.py
Task T021: src/paper_sorts/cli/update.py
Task T022: src/paper_sorts/cli/delete.py

# Step 3 — parallel: tests (no shared files)
Task T023: tests/conftest.py + tests/fixtures/seed_papers.py
Task T024: tests/test_repositories.py
Task T025: tests/test_services.py
Task T026: tests/test_cli.py
Task T027: tests/test_config.py
```

---

## Implementation Strategy

### MVP First (US1 + US2 core — Phase 3)

1. Complete Phase 1: Setup (T001–T005)
2. Complete Phase 2: Foundational (T006–T014)
3. Complete Phase 3: US1 + US2 core (T015–T027)
4. **STOP and VALIDATE**: `uv run pytest tests/` passes; `uv run pdbsearch --help` exits 0
5. Architecture document complete (T015)

### Incremental Delivery

1. Setup + Foundational → skeleton compiles, migrations run
2. Phase 3 → working CLI with tests
3. Phase 4 → test suite self-contained, coverage gates met
4. Phase 5 → migration command tested end-to-end
5. Phase 6 → bulk import working
6. Phase 7 → legacy code removed, benchmarks recorded, all gates green

---

## Notes

- [P] tasks = different files, no intra-phase dependency
- Constitution Principle II: NO mocking of SQLAlchemy session in persistence tests
- Constitution Principle III: ALL prompts route through `cli/prompts.py`
- Constitution Principle I (G3 gate): `tests/test_doc_currency.py` is merge-blocking
- Constitution Principle IV (G2 gate): `tests/benchmarks/bench_baseline.py` must execute (not permanently skipped)
- Commit after each logical task group; use author `HubReb <17859526+HubReb@users.noreply.github.com>`
