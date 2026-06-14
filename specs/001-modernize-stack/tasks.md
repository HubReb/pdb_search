# Tasks: Modernize the Stack

**Input**: Design documents from `specs/001-modernize-stack/`
**Prerequisites**: plan.md ✅, spec.md ✅, research.md ✅, data-model.md ✅, contracts/cli-commands.md ✅, quickstart.md ✅

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to
- All file paths are relative to the repository root

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization — replace Poetry/argparse/pylint/unittest stack with uv/Typer/ruff/pytest.

- [ ] T001 Convert pyproject.toml to PEP 621 format with uv, hatchling, Python ≥3.11, and all modernized dependencies (SQLAlchemy, Typer, Alembic, pydantic-settings, psycopg[binary], pybtex, pylatexenc, rich, cryptography, ruff, mypy, pytest, pytest-postgresql) in pyproject.toml
- [ ] T002 Create src-layout skeleton: src/paper_sorts/__init__.py, src/paper_sorts/cli/__init__.py, src/paper_sorts/db/__init__.py, src/paper_sorts/services/__init__.py
- [ ] T003 [P] Configure ruff (lint + format) in pyproject.toml [tool.ruff] section with appropriate rules (E, W, F, I, UP, B)
- [ ] T004 [P] Configure mypy in pyproject.toml [tool.mypy] with strict=true for src/paper_sorts, relaxed for tests/

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T005 Create src/paper_sorts/logging_config.py: single dictConfig call with RichHandler (stdout, INFO+) and optional FileHandler via PDBSEARCH_LOG_FILE env var; expose configure_logging(level) function
- [ ] T006 Create src/paper_sorts/config.py: pydantic-settings BaseSettings with fields database_url, log_level; env prefix PDBSEARCH_; .env file support; custom FernetIniSettingsSource reading [postgresql] section from encrypted INI (--config + --key); four-source priority chain: CLI args > env > .env > Fernet INI
- [ ] T007 Create src/paper_sorts/db/models.py: SQLAlchemy 2.x declarative ORM models — Bib(bibtex_id PK, bibtex), Paper(id serial PK, title, contents, bibtex_id FK→bib.bibtex_id), Author(id serial PK, author), AuthorPaper(id serial PK, author_id int, paper_id int) — no DDL FKs on authors_papers, no NOT NULL outside PKs, no extra indexes (schema-preservation contract)
- [ ] T008 Create src/paper_sorts/db/session.py: engine factory create_engine() accepting database_url; with_session() context manager (commit on success, rollback on exception); expose Session type alias
- [ ] T009 Create src/paper_sorts/db/repositories.py: PaperSummary and PaperCreate pydantic DTOs; PaperRepository (search_by_title, search_by_author, get_by_id, add, delete); AuthorRepository (get_or_create); BibRepository (add, get_by_id) — SQLAlchemy queries only, no raw SQL strings, no psycopg imports
- [ ] T010 Initialize Alembic: alembic init migrations; configure migrations/env.py to use src/paper_sorts/db/models.py metadata and read database_url from PDBSEARCH_DATABASE_URL
- [ ] T011 Create migrations/versions/001_initial_schema.py: verbatim port of DatabaseConnector.create_tables() DDL (bib, papers, authors_id, authors_papers) — preserving schema contract exactly; include upgrade() and downgrade()
- [ ] T012 Create migrations/versions/002_fix_legacy_bibtext_column.py: detect bibtext_id (sic) column in papers table using Inspector; rename to bibtex_id if present; skip if already bibtex_id (idempotent); include upgrade() and downgrade()

**Checkpoint**: Persistence layer and config are implemented. User story work can now begin.

---

## Phase 3: User Story 2 - Modernized Codebase, Same User-Facing Behaviour (Priority: P1) 🎯 MVP

**Goal**: Deliver a working CLI with all five operations (search/add/update/delete/import) backed by the modern stack.

**Independent Test**: Run `pdbsearch --help`, `pdbsearch search`, `pdbsearch add`, `pdbsearch update`, `pdbsearch delete` against a seeded test DB and verify each operation produces equivalent output to the legacy stack.

- [ ] T013 [US2] Create src/paper_sorts/cli/prompts.py: ask_text(prompt) re-prompting on empty; ask_choice(options, prompt) returning index; ask_confirmation(summary) accepting y/n/yes/no/1/2; ask_bibtex_file(); pretty_print_paper(paper: PaperSummary) — sole module in src/ permitted to import rich.prompt
- [ ] T014 [P] [US2] Create src/paper_sorts/services/paper_service.py: search_by_title(session, term) → list[PaperSummary]; search_by_author(session, term) → list[PaperSummary]; add_paper(session, data: PaperCreate) → None; update_field(session, paper_id, field: Literal["title","contents","bibtex","author"], value) → None with match/case + assert_never; delete_paper(session, paper_id) → None — pure orchestration, no SQL, no rich
- [ ] T015 [P] [US2] Create src/paper_sorts/services/import_service.py: extract_papers_from_tex_bib(tex_path, bib_path) → Iterator[PaperCreate] — port helpers.get_data() + get_bibtex_information() logic using pylatexenc + pybtex; skip entries with no BibTeX match (log warning); pure function, no I/O side effects
- [ ] T016 [US2] Create src/paper_sorts/cli/search.py: Typer subcommand; prompt user for title or author (via prompts.ask_choice); call paper_service.search_by_title or search_by_author; if multiple results prompt disambiguation (ask_choice with abort option); display via prompts.pretty_print_paper
- [ ] T017 [US2] Create src/paper_sorts/cli/add.py: Typer subcommand; offer bib-file path or manual entry via prompts; build PaperCreate DTO; call paper_service.add_paper; display success/error
- [ ] T018 [US2] Create src/paper_sorts/cli/update.py: Typer subcommand; search-first flow (search then pick paper); ask which field to update (title/contents/bibtex/author) via ask_choice with abort; ask new value; confirmation via ask_confirmation; call paper_service.update_field
- [ ] T019 [US2] Create src/paper_sorts/cli/delete.py: Typer subcommand; search-first flow; pick paper from list with abort; confirmation via ask_confirmation; call paper_service.delete_paper
- [ ] T020 [US2] Create src/paper_sorts/cli/importer.py: Typer subcommand (not in top-level menu); --tex PATH and --bib PATH required options; call import_service.extract_papers_from_tex_bib; loop add_paper per-paper with per-paper commit; log skipped/failed entries; print summary
- [ ] T021 [US2] Create src/paper_sorts/cli/migrate.py: Typer subcommand (not in top-level menu); run alembic upgrade head programmatically against configured database_url; print success/failure message
- [ ] T022 [US2] Create src/paper_sorts/cli/app.py: root Typer app; add_typer for search, add, update, delete, importer, migrate subcommands; when invoked with no subcommand show interactive four-option menu (1 search / 2 add / 3 update / 4 delete / 5 quit) via prompts.ask_choice; global --database-url, --log-level, --config, --key options; call configure_logging at startup; expose main() entry point

**Checkpoint**: Full CLI functional against any PostgreSQL database.

---

## Phase 4: User Story 1 - Architecture Documentation (Priority: P1)

**Goal**: Deliver docs/architecture.md documenting the pre-modernization codebase.

**Independent Test**: A Python developer unfamiliar with the project can answer "what does it do / data model / where add a new field" in 30 minutes without opening source.

- [ ] T023 [P] [US1] Create docs/architecture.md: document legacy paper_sorts/ architecture — purpose, four-table data model (papers/bib/authors_id/authors_papers), three-layer control flow (UserInteraction→DatabaseConnector→PsycopgDB), configuration (ConfigReader/Fernet INI), install/run instructions, known limitations (live-DB test dependency, bibtext_id typo, duplicate procedural modules), rollback semantics in DatabaseConnector; this is the pre-modernization reference

**Checkpoint**: US1 delivered — architecture documented.

---

## Phase 5: User Story 3 - Reproducible Test Suite (Priority: P2)

**Goal**: Full pytest suite that runs on a fresh checkout with no personal database.

**Independent Test**: `git clone && uv sync --all-extras && uv run pytest` passes on a machine with no `database.crypt`.

- [ ] T024 [US3] Create tests/conftest.py: postgresql_proc fixture (scope=session, pg_ctl=/usr/bin/pg_ctl); ephemeral_db_url fixture building postgresql+psycopg://... DSN; engine fixture running alembic upgrade head; db_session fixture (Session per test, rollback on teardown); seed fixture loading SEED_PAPERS
- [ ] T025 [P] [US3] Create tests/fixtures/seed_papers.py: SEED_PAPERS constant — list of PaperCreate dicts covering: at least 2 papers with same title (disambiguation test), papers with multiple authors, BibTeX with LaTeX accents; this data replaces the developer-local "Pino, J." / "Wang2021LargeScaleSA" rows
- [ ] T026 [P] [US3] Create tests/test_repositories.py: integration tests for PaperRepository (search_by_title single match, search_by_title multi-match, search_by_author, add, delete, get_by_id not found); AuthorRepository (get_or_create idempotent); BibRepository (add, get_by_id) — all against ephemeral DB seeded by conftest
- [ ] T027 [P] [US3] Create tests/test_services.py: integration tests for paper_service (search_by_title, search_by_author, add_paper, update_field for each Literal field, delete_paper, update_field invalid field raises); import_service (extract_papers_from_tex_bib with fixture tex+bib pair, missing bib key skipped)
- [ ] T028 [P] [US3] Create tests/test_cli.py: CLI layer tests using typer.testing.CliRunner; cover all subcommands (search, add, update, delete, import, migrate) through their public entry points; verify exit codes, stdout content, and DB state after mutations; each subcommand test uses seeded ephemeral DB
- [ ] T029 [P] [US3] Create tests/test_config.py: unit tests for Settings (env var override, .env file loading, missing required field raises ValidationError, Fernet INI source with fixture encrypted file); no DB required
- [ ] T030 [P] [US3] Create tests/test_migrations.py: integration tests — upgrade head from empty DB, downgrade to base, upgrade again (idempotent); run on DB with bibtext_id column to verify revision 002 rename; run on DB already having bibtex_id to verify idempotency

**Checkpoint**: Full test suite green on ephemeral DB.

---

## Phase 6: User Story 4 - One-Shot Database Migration (Priority: P2)

**Goal**: `pdbsearch migrate` upgrades a personal database in either historical schema with zero data loss.

**Independent Test**: Take snapshot of seeded test DB, run migrate, verify row counts match.

- [ ] T031 [US4] Verify migrations/versions/002_fix_legacy_bibtext_column.py handles both schemas correctly — add integration test in tests/test_migrations.py for: (a) DB with bibtext_id (sic) column → upgrade renames it; (b) DB with bibtex_id already → upgrade is no-op; (c) run upgrade twice → idempotent (extend existing test_migrations.py)
- [ ] T032 [P] [US4] Add test_migrations.py row-count assertion test: seed both papers+authors+bib+authors_papers before migration, run upgrade head on legacy-schema DB, assert counts match exactly after migration

**Checkpoint**: Migration is verifiably idempotent and zero-loss.

---

## Phase 7: User Story 5 - Bulk Import Preserved (Priority: P3)

**Goal**: `pdbsearch import --tex lit.tex --bib refs.bib` inserts all cited entries.

**Independent Test**: Run import against fixture tex+bib pair with N entries; verify N papers in DB.

- [ ] T033 [P] [US5] Create tests/fixtures/lit_sample.tex and tests/fixtures/refs_sample.bib: fixture .tex with 3 cited entries, matching .bib with 2 of them (one missing to test skip-with-warning path); include a BibTeX entry with LaTeX accents (\"o) to test round-trip
- [ ] T034 [US5] Extend tests/test_services.py with import_service integration test: call extract_papers_from_tex_bib(lit_sample.tex, refs_sample.bib), assert 2 PaperCreate objects returned (missing key skipped), assert LaTeX accent round-trips correctly through pybtex
- [ ] T035 [US5] Extend tests/test_cli.py with import subcommand integration test: invoke pdbsearch import --tex ... --bib ... via CliRunner, assert 2 papers in DB, assert missing-key logged as warning, assert partial-failure leaves committed papers intact

**Checkpoint**: Bulk import verified end-to-end.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Legacy removal, doc-currency gate, final quality checks.

- [ ] T036 Remove legacy flat-layout modules: delete paper_sorts/add.py, paper_sorts/search.py, paper_sorts/get_data.py, paper_sorts/user_interaction.py, paper_sorts/database_connector.py, paper_sorts/psycopg_db.py, paper_sorts/config_reader.py, paper_sorts/helpers.py, paper_sorts/run.py, paper_sorts/__init__.py; remove the paper_sorts/ directory (FR-012)
- [ ] T037 [P] Update README.md: replace Poetry/argparse/pylint/unittest instructions with uv/pdbsearch/ruff/pytest equivalents; remove all references to Poetry, psycopg2, UserInteraction, PsycopgDB (doc-currency gate, Principle I)
- [ ] T038 [P] Update CLAUDE.md: replace legacy architecture description with modern src/paper_sorts/ layout; remove forbidden tokens (Poetry, psycopg2, UserInteraction, PsycopgDB); update Commands section with uv commands; mark 001-modernize-stack as complete in SpecKit section
- [ ] T039 [P] Add doc-currency gate test in tests/test_doc_currency.py: assert that README.md and CLAUDE.md do not contain any of ["Poetry", "psycopg2", "UserInteraction", "PsycopgDB"] (case-sensitive); this is a mechanical merge-blocking check per Principle I
- [ ] T040 Run uv run ruff check src tests and fix any remaining issues; run uv run ruff format src tests
- [ ] T041 Run uv run mypy src and fix any type errors (strict mode on src/paper_sorts/)
- [ ] T042 Run uv run pytest and verify all tests pass, including doc-currency gate
- [ ] T043 [P] Verify per-layer coverage: run uv run pytest --cov=src/paper_sorts --cov-report=term-missing; confirm db/, services/, cli/, config.py each individually ≥80% line coverage (constitution Principle II per-layer gate)

---

## Phase 9: Benchmark Harness (Constitution Principle IV Gate)

**Purpose**: Baseline-benchmark gate — non-regression criterion must be verifiable.

- [ ] T044 Create tests/benchmarks/bench_baseline.py: benchmark harness using pytest-benchmark or time.perf_counter; measure wall-clock for search_by_title (single match), search_by_title (multi-match), search_by_author, add_paper, update_field, delete_paper on seeded ephemeral DB; write results to tests/benchmarks/baseline.json
- [ ] T045 Run the benchmark harness against the seeded ephemeral DB; commit tests/benchmarks/baseline.json with recorded times; verify harness is NOT permanently @pytest.mark.skip'd (constitution Principle IV baseline-benchmark gate)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately
- **Phase 2 (Foundational)**: Depends on Phase 1 — BLOCKS all user story phases
- **Phase 3 (US2 — CLI)**: Depends on Phase 2 (needs repositories, session, config, models)
- **Phase 4 (US1 — Docs)**: Independent — can run in parallel with Phase 3
- **Phase 5 (US3 — Tests)**: Depends on Phase 3 (tests exercise the CLI and services)
- **Phase 6 (US4 — Migration)**: Depends on Phase 2 (migration tests) and Phase 3 (migrate subcommand)
- **Phase 7 (US5 — Import)**: Depends on Phase 3 (import subcommand and import_service)
- **Phase 8 (Polish)**: Depends on all user story phases
- **Phase 9 (Benchmarks)**: Depends on Phase 5 (needs conftest fixtures)

### Within Each Phase

- Tasks marked [P] within the same phase can run in parallel (different files)
- Unlabelled tasks within a phase run sequentially in listed order
- T013 (prompts.py) before T016–T022 (CLI subcommands that call prompts)
- T014 (paper_service) before T016–T019 (CLI that calls paper_service)
- T015 (import_service) before T020 (importer CLI)
- T024 (conftest) before T025–T030 (all tests)
- T025 (seed data) before T026–T028 (tests that use seeds)

### Parallel Opportunities

**Phase 1**: T003 and T004 in parallel after T001, T002.
**Phase 2**: T007, T008 in parallel; T009 after T007+T008; T010 after T007; T011, T012 after T010.
**Phase 3**: T014, T015, T013 in parallel; T016–T021 after T013+T014+T015.
**Phase 5**: T025–T030 in parallel after T024.
**Phase 8**: T037, T038, T039 in parallel after T036.

---

## Parallel Example: Phase 5 (Test Suite)

```bash
# After T024 (conftest) is done, launch in parallel:
Task T025: "Create tests/fixtures/seed_papers.py"
Task T026: "Create tests/test_repositories.py"
Task T027: "Create tests/test_services.py"
Task T028: "Create tests/test_cli.py"
Task T029: "Create tests/test_config.py"
Task T030: "Create tests/test_migrations.py"
```

---

## Implementation Strategy

### MVP First (US2 = Core CLI + US3 = Tests)

1. Complete Phase 1: Setup (pyproject.toml, src skeleton)
2. Complete Phase 2: Foundational (models, session, repos, config, migrations)
3. Complete Phase 3: US2 (full CLI on modern stack)
4. Complete Phase 5: US3 (reproducible test suite)
5. **STOP and VALIDATE**: `uv run pytest` green; `uv run pdbsearch --help` works
6. Continue with Phases 4, 6, 7, 8, 9

### Incremental Delivery

After each phase checkpoint, the increment should be independently runnable and testable. Phase 3 (US2) is the MVP — a working CLI is more valuable than docs or migration tooling.

### Task Count

- Phase 1: 4 tasks
- Phase 2: 8 tasks
- Phase 3 (US2): 10 tasks
- Phase 4 (US1): 1 task
- Phase 5 (US3): 7 tasks
- Phase 6 (US4): 2 tasks
- Phase 7 (US5): 3 tasks
- Phase 8 (Polish): 8 tasks
- Phase 9 (Benchmarks): 2 tasks

**Total: 45 tasks**

---

## Notes

- [P] tasks = different files, no shared state, can execute concurrently
- [Story] label maps each task to a user story for traceability
- Schema-preservation contract (data-model.md): no NOT NULL outside PKs, no DDL FKs on authors_papers, no new indexes
- All prompts route through cli/prompts.py — bare input() calls elsewhere are a Principle III violation
- Per-layer coverage gate (Principle II): each of db/, services/, cli/, config.py must independently reach ≥80%
- Doc-currency gate (Principle I): T039 is a mechanical test enforcing the forbidden-token check
- Baseline-benchmark gate (Principle IV): T044–T045 are required; harness must not be permanently skipped
