# Tasks: Modernize the Stack

**Input**: Design documents from `/specs/001-modernize-stack/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/cli-commands.md

**Tests**: REQUIRED. US2/US3/US4/US5 acceptance is test-defined, and the constitution (v1.3.0-b2-hardened) makes per-layer coverage (G1), an executing baseline benchmark (G2), and the doc-currency gate (G3) merge-blocking. Tests run on a real ephemeral PostgreSQL via pytest-postgresql.

**Organization**: by user story. MVP = Setup + Foundational + US2 (the modernized CLI with same behaviour) reaching a green build.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: parallelizable (different files, no incomplete-task dependency)
- Story label on user-story-phase tasks only

## Path Conventions

Single-project src-layout: package at `src/paper_sorts/`, tests flat under `tests/`, migrations at `migrations/`.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: uv/PEP 621 packaging, tooling config, src-layout skeleton.

- [ ] T001 Create `pyproject.toml` (PEP 621, hatchling backend, Python ≥ 3.11): runtime deps (sqlalchemy>=2, alembic, typer, rich, pydantic, pydantic-settings, psycopg[binary], cryptography, pybtex, pylatexenc) and a dev extra (pytest, pytest-postgresql, pytest-cov, ruff, mypy); console script `pdbsearch = "paper_sorts.cli.app:app"`; ruff + mypy(strict on src) + pytest config tables.
- [ ] T002 Create the src-layout package skeleton: `src/paper_sorts/__init__.py`, `src/paper_sorts/{cli,services,db}/__init__.py`, and `tests/__init__.py`, `tests/fixtures/__init__.py`, `tests/benchmarks/__init__.py`.
- [ ] T003 Generate `uv.lock` via `uv sync --all-extras` and confirm the env resolves on Python ≥ 3.11.

**Checkpoint**: `uv run python -c "import paper_sorts"` works; `uv run ruff --version`, `uv run mypy --version`, `uv run pytest --version` resolve.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: persistence models, session, config, logging, Alembic baseline, and the test harness every story depends on. No user-visible feature yet.

- [ ] T004 [P] Define the four SQLAlchemy 2.x ORM models in `src/paper_sorts/db/models.py` (`Paper`, `Bib`, `AuthorId`, `AuthorPaper`) with typed `mapped_column`s, preserving the exact schema (no extra NOT NULL/FK/index; `bib.bibtex` UNIQUE; `papers.bibtex_id` FK → `bib.bibtex_id`; `authors_papers` no FKs).
- [ ] T005 [P] Implement `with_session(...)` in `src/paper_sorts/db/session.py` — context-managed Session (commit on success, rollback on exception, deterministic close); engine factory from a `database_url`.
- [ ] T006 [P] Implement `src/paper_sorts/config.py` — pydantic-settings `Settings` with four-source priority (CLI > env `PDBSEARCH_*` > `.env` > Fernet-encrypted INI custom source); decrypt INI with key file → `database_url`; clear error when the key file is missing.
- [ ] T007 [P] Implement `src/paper_sorts/logging_config.py` — single `dictConfig` (RichHandler to stdout, optional FileHandler); `configure_logging(level, log_file)` callable once at startup.
- [ ] T008 Scaffold Alembic: `alembic.ini`, `migrations/env.py` (reads the configured `database_url`, targets `models.metadata`), `migrations/script.py.mako`.
- [ ] T009 Author revision `migrations/versions/0001_baseline_schema.py` — verbatim DDL port creating the four canonical tables.
- [ ] T010 Author revision `migrations/versions/0002_converge_legacy_bibtext.py` — idempotent, schema-inspecting rename of legacy `bibtext_id`/`bibtext` → `bibtex_id`/`bibtex` in `bib` and `papers`; no-op on canonical/fresh DBs.
- [ ] T011 [P] Create the canonical seed dataset `tests/fixtures/seed_papers.py` (`SEED_PAPERS`) — co-locate the rows the suite asserts on (incl. `Pino, J.` / `Wang2021LargeScaleSA` and the multi-author speech-translation paper); include a LaTeX-accent edge-case entry.
- [ ] T012 Implement `tests/conftest.py` — `postgresql_proc` + `ephemeral_db_url` (session-scoped, from host `pg_ctl`) and a `seeded_db` fixture that runs Alembic to head and loads `SEED_PAPERS`.
- [ ] T013 [P] Add `tests/test_migrations.py` — apply 0001 to a fresh DB; build a legacy `bibtext_id` DB and assert 0002 converges it with row-count parity (papers/authors/authorships/bib) and is idempotent on rerun (US4 / FR-011).

**Checkpoint**: ephemeral DB provisions; migrations apply; migration tests pass. Foundation ready for all stories.

---

## Phase 3: User Story 2 — Modernized Codebase, Same Behaviour (P1, MVP) 🎯

**Goal**: `pdbsearch` reproduces every legacy CLI flow (search title/author incl. disambiguation, add inline/from `.bib`, update each field with confirmation, delete, abort, quit) with equivalent output, on the modern stack.

**Independent Test**: scripted dialog through every path against `seeded_db` produces equivalent-or-improved output; abort/`n` writes nothing.

- [ ] T014 [P] [US2] Define DTOs `PaperSummary` and `PaperCreate` (pydantic) in `src/paper_sorts/db/repositories.py`.
- [ ] T015 [US2] Implement `PaperRepository` in `src/paper_sorts/db/repositories.py` — `search_by_title`, `search_by_author` (parameterised joins over the four tables, returning `PaperSummary`), `get_bibtex`, `add_paper(PaperCreate)` (single transaction), `delete_paper`, plus title/contents/bibtex update primitives.
- [ ] T016 [US2] Implement `AuthorRepository`/`BibRepository` (or author/bib methods) in `src/paper_sorts/db/repositories.py` — author upsert + link, author rename/merge, drop-author-with-no-papers, bibtex uniqueness check.
- [ ] T017 [P] [US2] Implement `src/paper_sorts/services/paper_service.py` — `search_by_title`, `search_by_author`, `add_paper`, `delete_paper`, and `update_field(table: Literal["papers","bib","authors_id"], …)` dispatching via `match`/`case` with `assert_never(table)`; rejects `authors_papers` and `*_id` columns. Pure orchestration over DTOs/repos; no SQL, no rich.
- [ ] T018 [P] [US2] Implement `src/paper_sorts/cli/prompts.py` — the only module permitted to import `rich.prompt`; helpers for non-empty text prompt (empty → re-prompt), 1-indexed numbered choice with abort, and y/n+1/2 confirmation.
- [ ] T019 [US2] Implement `src/paper_sorts/cli/search.py` — author/title sub-menu, disambiguation on multiple matches, Rich rendering matching `pretty_print_results` (title/authors/summary/bib).
- [ ] T020 [US2] Implement `src/paper_sorts/cli/add.py` — prompts for authors/title/key/bib(inline or file)/summary; build `PaperCreate`; call service; plain success/failure message.
- [ ] T021 [US2] Implement `src/paper_sorts/cli/update.py` — table→column menus, identifier + new value prompts, confirmation summarising the change; surface `ValueError` as a plain message.
- [ ] T022 [US2] Implement `src/paper_sorts/cli/delete.py` — identify + summarise + confirm + delete (paper, bib, links, orphan authors).
- [ ] T023 [US2] Implement `src/paper_sorts/cli/app.py` — Typer app wiring `search/add/update/delete` subcommands, global config options (`--database-url/--log-level/--config/--key`), `configure_logging` at startup, and the bare-invocation four-option menu (`migrate`/`import` registered but absent from the menu).
- [ ] T024 [P] [US2] Add `tests/test_repositories.py` — real-DB CRUD/search/update/delete against `seeded_db` (persistence layer ≥ 80%, G1).
- [ ] T025 [P] [US2] Add `tests/test_paper_service.py` — service-layer behaviour incl. `update_field` rejection paths (service layer ≥ 80%, G1).
- [ ] T026 [P] [US2] Add `tests/test_prompts.py` — empty-input re-prompt, malformed choice re-prompt, confirmation parsing (config/prompts unit coverage).
- [ ] T027 [US2] Add `tests/test_cli.py` — end-to-end Typer `CliRunner` over every subcommand and the bare menu, asserting parity output and that abort/`n` writes nothing (interface layer ≥ 80% via E2E, G1).

**Checkpoint**: US2 independently testable; all legacy flows reproduced and green. **This is the MVP.**

---

## Phase 4: User Story 3 — Reproducible Test Suite, No Developer-Local State (P2)

**Goal**: fresh-checkout test run with no `database.crypt`/`key`, ephemeral DB managed by fixtures, seed co-located with assertions.

**Independent Test**: clone on a machine without the personal DB → install + test passes.

- [ ] T028 [US3] Verify and document the no-local-state guarantee: assert no test imports `ConfigReader`/`database.crypt`/`key`; ensure every row-asserting test references `SEED_PAPERS` (Principle II). Add a guard test if useful.
- [ ] T029 [P] [US3] Confirm `pytest-cov` per-layer reporting and add a coverage configuration (`--cov=src/paper_sorts`) so db/services/cli/config are each measurable ≥ 80% (G1).

**Checkpoint**: suite is self-contained and deterministic across reruns.

---

## Phase 5: User Story 4 — One-Shot Migration of Existing DB (P2)

**Goal**: `pdbsearch migrate` upgrades either historical schema to canonical with zero data loss, idempotently.

**Independent Test**: snapshot a legacy DB, migrate, compare counts exactly; rerun is a no-op.

- [ ] T030 [US4] Implement `src/paper_sorts/cli/migrate.py` — subcommand running Alembic to head against the configured DB; plain-language success/failure.
- [ ] T031 [US4] Extend `tests/test_migrations.py` (or add a CLI migrate test) exercising the migrate subcommand via `CliRunner` against a legacy `bibtext_id` DB and asserting parity + idempotent rerun.

**Checkpoint**: migration command delivers US4 end-to-end.

---

## Phase 6: User Story 5 — Bulk Import from LaTeX/BibTeX (P3)

**Goal**: `pdbsearch import --tex --bib` inserts every matched entry, per-paper commit, skips unmatched keys with a warning, idempotent rerun.

**Independent Test**: import a fixture `.tex`+`.bib` with N entries → N papers/authors/bib present.

- [ ] T032 [US5] Implement `src/paper_sorts/services/import_service.py` — `extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]` (pybtex + pylatexenc), skipping citation keys with no `.bib` match.
- [ ] T033 [US5] Implement `src/paper_sorts/cli/importer.py` — `import` subcommand iterating the extractor, committing per paper (US5 AS3), logging a warning on skip, skipping existing bibtex keys on rerun.
- [ ] T034 [P] [US5] Add `tests/test_import_service.py` with a fixture `.tex`+`.bib` pair — assert N papers/authors/bib inserted, unmatched key skipped, rerun does not duplicate.

**Checkpoint**: bulk import delivered.

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: US1 architecture doc, legacy removal (FR-012), constitution amendment (FR-016/SC-007), doc-currency gate (G3), baseline benchmark (G2), final gates.

- [ ] T035 [US1] Write `docs/architecture.md` — reverse-engineered description of the legacy (pre-modernization) stack: purpose, user journeys, four-table data model + relationships, control flow (CLI → domain → DB), config approach, install/run, rollback semantics, known limitations/quirks (FR-001 / SC-001).
- [ ] T036 Remove the legacy flat layout (`paper_sorts/add.py`, `search.py`, `get_data.py`, `database_connector.py`, `user_interaction.py`, `psycopg_db.py`, `config_reader.py`, `helpers.py`, `run.py`, `paper_sorts/__init__.py`) and the legacy `tests/test_database_connector.py` / `tests/test_user_interaction.py` once US2 coverage subsumes them (FR-012).
- [ ] T037 Update `README.md` and `CLAUDE.md` to the modern stack — remove every forbidden legacy token (`Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB`); add `tests/test_doc_currency.py` asserting a case-sensitive search finds none of them in those two files (G3, merge-blocking).
- [ ] T038 Amend the constitution via `/speckit-constitution` — reconcile the stale *Development Workflow & Quality Gates* lines (`pylint paper_sorts`, `unittest`, `create_tables()`) to ruff/mypy/pytest + Alembic; record rationale in the sync-impact header (FR-016 / SC-007).
- [ ] T039 Implement `tests/benchmarks/bench_baseline.py` — an executing (not permanently skipped) baseline harness timing the five interactive ops (search by title/author, add, update, delete) on `seeded_db`, recording a baseline; document the bench command (G2, merge-blocking).
- [ ] T040 Final gate sweep: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy src`, `uv run pytest` (with per-layer coverage) all green; confirm SC-005 LOC reduction ≥ 30%.

---

## Dependencies & Execution Order

- **Setup (P1)** → **Foundational (P2)** block everything.
- **US2 (P3)** depends only on Foundational → first deliverable (MVP).
- **US3 (P4)** validates the suite US2 produced; light.
- **US4 (P5)** depends on Foundational migrations (T009/T010) + CLI app (T023).
- **US5 (P6)** depends on Foundational + DTOs/repos (T014–T016) + CLI app.
- **Polish (P7)**: T036 (legacy removal) must follow US2 coverage; T037/T039/T040 gates last; T035/T038 independent.

## Parallel Opportunities

- Foundational: T004, T005, T006, T007, T011 in parallel (distinct files); T013 after T009/T010/T012.
- US2: T014/T017/T018 parallel; tests T024/T025/T026 parallel after their targets; T027 after T023.
- Cross-story tests (T034) parallel with their service/CLI once written.

## Implementation Strategy

MVP-first: deliver Setup + Foundational + US2 to a green build, then layer US3/US4/US5, then Polish (legacy removal + constitution amendment + doc/benchmark gates). Legacy removal is deferred to Polish so US2 coverage is proven before deletion (coverage-first, FR-012).
