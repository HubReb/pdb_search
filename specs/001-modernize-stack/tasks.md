# Tasks: Modernize the Stack

**Input**: Design documents from `specs/001-modernize-stack/`  
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/cli-commands.md

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Replace Poetry/flat-layout baseline with uv/src-layout skeleton. No behaviour yet.

- [ ] T001 Rewrite pyproject.toml: replace Poetry with PEP 621 + hatchling; add uv dependencies; entry point `pdbsearch = "paper_sorts.cli.app:run"` in pyproject.toml
- [ ] T002 Create src/paper_sorts/ package skeleton: __init__.py files in src/paper_sorts/, src/paper_sorts/cli/, src/paper_sorts/db/, src/paper_sorts/services/
- [ ] T003 [P] Configure ruff in pyproject.toml [tool.ruff] section with line-length, select rules (E, F, I, UP)
- [ ] T004 [P] Configure mypy in pyproject.toml [tool.mypy] section: strict=true, src layout paths
- [ ] T005 [P] Configure pytest in pyproject.toml [tool.pytest.ini_options]: testpaths=["tests"], addopts="--tb=short"

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T006 Create src/paper_sorts/logging_config.py: dictConfig with RichHandler (stdout) + optional FileHandler; `configure_logging(level: str) -> None`
- [ ] T007 Create src/paper_sorts/config.py: pydantic-settings Settings with database_url (SecretStr), log_level; custom FernetIniSource that reads encrypted config file + key file; four-source priority chain (CLI > env PDBSEARCH_* > .env > FernetIni)
- [ ] T008 Create src/paper_sorts/db/models.py: SQLAlchemy 2.x declarative ORM models for Paper, BibEntry, Author, AuthorPaper; preserve schema-preservation rule (no NOT NULL outside PK, no DDL FK on AuthorPaper, no extra indexes)
- [ ] T009 Create src/paper_sorts/db/session.py: engine factory from Settings.database_url; `with_session()` context manager (commit on success, rollback on exception); deterministic close
- [ ] T010 Create src/paper_sorts/db/repositories.py: PaperSummary + PaperCreate pydantic DTOs; PaperRepository (search_by_title, search_by_author, get_by_bibtex_id, create, delete); AuthorRepository (get_or_create, delete_orphans); BibRepository (get_or_create, update)
- [ ] T011 Create src/paper_sorts/services/paper_service.py: search_by_title, search_by_author, add_paper, update_field (match/case over Literal table arg with assert_never), delete_paper; pure orchestration — no SQL, no rich, no I/O
- [ ] T012 Create src/paper_sorts/cli/prompts.py: the ONLY module importing rich.prompt; ask_text(prompt, allow_empty=False), ask_choice(prompt, options), ask_confirm(prompt) accepting 1/2/y/n/yes/no; re-prompt on empty if not allowed
- [ ] T013 Create src/paper_sorts/cli/app.py: Typer app with callback; when invoked with no subcommand drops into 4-option menu loop (search/add/update/quit); calls configure_logging at startup; wires Settings from CLI options --database-url / --config / --key / --log-level
- [ ] T014 Initialize Alembic: alembic init migrations; configure env.py to use Settings.database_url; create migrations/versions/001_initial_schema.py with verbatim DDL for papers, bib, authors_id, authors_papers

**Checkpoint**: `uv run ruff check src` and `uv run mypy src` must pass (no code yet, just stubs with correct signatures).

---

## Phase 3: User Story 1 — Architecture Document (Priority: P1)

**Goal**: Produce a single human-readable architecture document covering purpose, user journeys, data model, control flow, configuration, install/run, and known limitations of the pre-modernization codebase.

**Independent Test**: A Python developer who has never seen the project reads `docs/architecture.md` and answers "What does it do? What is the data model? Where would I add a new field?" without opening source.

- [ ] T015 [US1] Write docs/architecture.md: reverse-engineer and document purpose, user journeys (search by title/author, add, update, delete, bulk import), data model (four tables + schema variant), control flow diagram (CLI → DatabaseConnector → PsycopgDB), configuration (ConfigReader + Fernet), install/run, known limitations and quirks

**Checkpoint**: Architecture document complete. Present to a reviewer unfamiliar with the project; they must be able to answer the three questions within 30 minutes.

---

## Phase 4: User Story 2 — Modernized Codebase, Same UX (Priority: P1) 🎯 MVP

**Goal**: All existing CLI paths work through the modernized stack. Legacy procedural modules removed.

**Independent Test**: Run a scripted dialog through every CLI path (search/add/update/delete/quit) against a seeded test database. Each path must produce equivalent output to the pre-modernization version.

### Implementation for User Story 2

- [ ] T016 [US2] Create src/paper_sorts/cli/search_cmd.py: `pdbsearch search` Typer subcommand; options --by {title,author}, --query; interactive fallback using prompts.py; disambiguation list on multiple matches; all prompts via cli/prompts.py
- [ ] T017 [US2] Create src/paper_sorts/cli/add_cmd.py: `pdbsearch add` Typer subcommand; option --from-bib FILE; inline prompt flow for author/title/bibtex-key/summary; confirmation step; calls paper_service.add_paper
- [ ] T018 [US2] Create src/paper_sorts/cli/update_cmd.py: `pdbsearch update` Typer subcommand; option --id BIBTEX_ID; if no --id: search flow to pick paper; field selection menu (title/contents/bibtex/author/abort); show old+new values in confirmation; calls paper_service.update_field
- [ ] T019 [US2] Create src/paper_sorts/cli/delete_cmd.py: `pdbsearch delete` Typer subcommand; option --id BIBTEX_ID; if no --id: search flow; confirmation showing title + id; calls paper_service.delete_paper
- [ ] T020 [US2] Register all subcommands (search, add, update, delete) in src/paper_sorts/cli/app.py; verify `pdbsearch --help` lists them all
- [ ] T021 [US2] Write tests/conftest.py: postgresql_proc fixture using host pg_ctl at /usr/bin/pg_ctl; ephemeral_db_url fixture; run Alembic migrations on ephemeral DB; seed_papers fixture from tests/fixtures/seed_papers.py
- [ ] T022 [P] [US2] Write tests/fixtures/seed_papers.py: SEED_PAPERS constant — at least 3 papers, 2 authors, 1 shared-author pair, 1 title-collision pair (two papers with same title); document which assertions each seed row supports
- [ ] T023 [P] [US2] Write tests/test_repositories.py: integration tests for PaperRepository (search_by_title exact match, search_by_title title-collision returns multiple, search_by_author, create, delete); AuthorRepository (get_or_create, delete_orphans); BibRepository (get_or_create, update); all against real ephemeral DB
- [ ] T024 [P] [US2] Write tests/test_services.py: integration tests for paper_service.search_by_title, search_by_author, add_paper, update_field (each updatable field), delete_paper; uses ephemeral DB via conftest
- [ ] T025 [US2] Write tests/test_cli.py: Typer CliRunner tests for search (title match, title collision, author match, no results), add (inline, from-bib), update (each field, abort, confirm-n), delete (confirm-y, confirm-n), quit from menu; confirm subcommands exit 0 on success
- [ ] T026 [US2] Remove legacy flat-layout modules: delete paper_sorts/ directory (add.py, config_reader.py, database_connector.py, get_data.py, helpers.py, psycopg_db.py, run.py, search.py, user_interaction.py, __init__.py); verify no import of old modules remains

**Checkpoint**: `uv run pytest tests/test_repositories.py tests/test_services.py tests/test_cli.py` all green; `uv run ruff check src`; `uv run mypy src`.

---

## Phase 5: User Story 3 — Reproducible Test Suite (Priority: P2)

**Goal**: `git clone && uv sync --all-extras && uv run pytest` succeeds with no personal database or credentials.

**Independent Test**: On a machine with no `../../database.crypt` and no `../../key`, run `uv run pytest` — all tests pass.

- [ ] T027 [US3] Write tests/test_config.py: unit tests for Settings — env var override of database_url, .env file loading, missing Fernet key produces clear ValueError not stack trace, PDBSEARCH_LOG_LEVEL parsed correctly; use monkeypatch for env vars; no real DB needed
- [ ] T028 [US3] Verify conftest.py ephemeral DB setup: assert postgresql_proc uses /usr/bin/pg_ctl; ephemeral_db_url does NOT reference ../../database.crypt; add a smoke test that checks no env var PDBSEARCH_DATABASE_URL is required when using conftest fixtures

**Checkpoint**: `uv run pytest` passes on a fresh environment with only uv + pg_ctl available.

---

## Phase 6: User Story 4 — One-Shot Migration (Priority: P2)

**Goal**: A user with a personal database (either schema variant) runs `pdbsearch migrate` and all rows are preserved.

**Independent Test**: Snapshot row counts before migration, run `pdbsearch migrate`, compare counts — must match exactly.

- [ ] T029 [US4] Create src/paper_sorts/cli/migrate_cmd.py: `pdbsearch migrate` Typer subcommand; option --target REVISION (default "head"); calls alembic upgrade; plain-language success/error messages; exit non-zero on failure; register in app.py
- [ ] T030 [US4] Create migrations/versions/002_converge_bibtext_typo.py: Alembic revision that detects if column `bibtext_id` exists in papers table and renames to `bibtex_id`; uses `op.alter_column` with `existing_type`; idempotent (no-op if `bibtex_id` already exists or `bibtext_id` not found)
- [ ] T031 [US4] Write test in tests/test_repositories.py for migration idempotency: apply migrations twice on ephemeral DB; assert schema unchanged on second run; assert seed data survives both runs

**Checkpoint**: `pdbsearch migrate` runs cleanly on ephemeral DB. Row counts preserved.

---

## Phase 7: User Story 5 — Bulk Import Preserved (Priority: P3)

**Goal**: `pdbsearch import tex_file bib_file` imports all cited entries; skips missing bib keys with logged warning; per-paper commit.

**Independent Test**: Run `pdbsearch import fixture.tex fixture.bib` against a seeded DB; verify N papers inserted, M skipped warnings logged.

- [ ] T032 [US5] Create src/paper_sorts/services/import_service.py: `extract_papers_from_tex_bib(tex_content: str, bib_content: str) -> Iterator[PaperCreate]`; use pylatexenc for tex parsing; use pybtex for bib parsing; yield PaperCreate per cited key found in bib; log warning for keys not in bib
- [ ] T033 [US5] Create src/paper_sorts/cli/import_cmd.py: `pdbsearch import TEX_FILE BIB_FILE` Typer subcommand; calls import_service.extract_papers_from_tex_bib; per-paper add via paper_service.add_paper (per-paper transaction per constitution Principle IV); print summary "Imported N, skipped M"; register in app.py
- [ ] T034 [US5] Create tests/fixtures/import_fixture.tex and tests/fixtures/import_fixture.bib: at least 3 cited entries in .tex; 2 matching bib records + 1 missing (to test skip-with-warning); document which entries should import vs. be skipped
- [ ] T035 [US5] Add test in tests/test_cli.py for import subcommand: CliRunner invoking `pdbsearch import fixture.tex fixture.bib`; assert imported count, skipped warning in output, idempotent re-run skips duplicates

**Checkpoint**: `uv run pytest -k import` green. Bulk import end-to-end works.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Verification, cleanup, and constitution compliance gates.

- [ ] T036 [P] Write tests/benchmarks/conftest.py: benchmark-specific fixtures re-using ephemeral DB; seed with SEED_PAPERS; no personal DB required
- [ ] T037 Write tests/benchmarks/bench_baseline.py: pytest-benchmark tests for search_by_title, search_by_author, add_paper, update_field, delete_paper; NOT permanently skipped (constitution G2 gate); save results via --benchmark-autosave
- [ ] T038 [P] Update README.md: remove Poetry/psycopg2/UserInteraction/PsycopgDB tokens; document uv install + `pdbsearch` entry point; link to quickstart.md
- [ ] T039 [P] Update CLAUDE.md: remove Poetry/pylint/unittest references; update Commands section with uv commands; verify no forbidden legacy tokens remain (constitution G3 doc-currency gate)
- [ ] T040 Run `uv run ruff check src tests` — fix all warnings; run `uv run ruff format src tests`
- [ ] T041 Run `uv run mypy src` — fix all type errors until clean
- [ ] T042 [P] Add test in tests/test_config.py: doc-currency gate — assert README.md and CLAUDE.md do not contain any of: "Poetry", "psycopg2", "UserInteraction", "PsycopgDB"
- [ ] T043 Run full `uv run pytest` and verify per-layer coverage ≥80% for each of: db/, services/, cli/, config.py (constitution G1 per-layer gate); use `pytest --cov=src/paper_sorts --cov-report=term-missing`
- [ ] T044 Commit all changes with logical splits; verify `git log --oneline` shows incremental progress aligned with task phases

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Phase 1 — BLOCKS all user stories
- **US1 Architecture (Phase 3)**: Can proceed after Phase 1 (only requires reading legacy code; no code to write)
- **US2 Modernized Codebase (Phase 4)**: Requires Phase 2 complete (needs models, repositories, services, prompts, app)
- **US3 Reproducible Tests (Phase 5)**: Requires Phase 4 (test infrastructure built in US2)
- **US4 Migration (Phase 6)**: Requires Phase 2 (Alembic init in T014)
- **US5 Bulk Import (Phase 7)**: Requires Phase 4 (paper_service.add_paper, prompts, app)
- **Polish (Phase 8)**: Requires all prior phases

### User Story Dependencies

- **US1 (P1)**: Independent — can start after Phase 1
- **US2 (P1)**: Requires Foundational Phase 2 — the core MVP
- **US3 (P2)**: Extends US2 test infrastructure
- **US4 (P2)**: Extends Foundational + Alembic init (T014)
- **US5 (P3)**: Extends US2 (paper_service reuse)

### Within Each User Story

- Repositories (T010) before Services (T011) before CLI (T016-T019)
- Tests (T021-T025) written after implementation in this plan (integration-first)
- Legacy modules removed (T026) only after all tests green

### Parallel Opportunities

- T003, T004, T005 (Phase 1 config): parallel
- T006, T007 (logging, config): parallel
- T008, T009, T010, T011, T012 (db/services/prompts): parallel within Phase 2 (different files)
- T016, T017, T018, T019 (CLI subcommands): parallel (different files)
- T022, T023, T024 (fixtures + repo tests + service tests): parallel
- T036, T038, T039, T042 (Polish): parallel

---

## Parallel Example: Phase 2 Foundational

```bash
# These can run simultaneously (different files):
T006: src/paper_sorts/logging_config.py
T007: src/paper_sorts/config.py
T008: src/paper_sorts/db/models.py
T009: src/paper_sorts/db/session.py
T012: src/paper_sorts/cli/prompts.py
```

---

## Implementation Strategy

### MVP First (US1 + US2)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL)
3. Complete Phase 3: US1 Architecture doc
4. Complete Phase 4: US2 Modernized CLI
5. **STOP and VALIDATE**: `uv run pytest` green; `ruff check`; `mypy` clean
6. Remove legacy modules (T026)

### Incremental Delivery

1. Setup + Foundational → skeleton with types
2. US1 Architecture doc → FR-001 satisfied
3. US2 CLI + tests → FR-002 through FR-014 satisfied; legacy removed
4. US3 fresh-checkout test → FR-008 validated
5. US4 migration → FR-005, FR-011 satisfied
6. US5 bulk import → FR-002 (import path) satisfied
7. Polish → all constitution gates verified

---

## Notes

- All prompts route through `src/paper_sorts/cli/prompts.py` — bare `input()` anywhere else is a constitution violation
- `db/` is the ONLY place importing `sqlalchemy` or any DB driver
- Services depend on DTOs only, never ORM types
- Per-paper transaction in import (T033) is a constitution Principle IV requirement
- Benchmark (T037) MUST NOT be permanently `@pytest.mark.skip` — G2 gate
- Remove Poetry/psycopg2/UserInteraction/PsycopgDB from docs — G3 gate
- Per-layer coverage ≥80% each — G1 gate
