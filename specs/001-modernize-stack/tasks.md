# Tasks: Modernize the Stack

**Feature**: 001-modernize-stack
**Input**: plan.md, spec.md, research.md, data-model.md, contracts/cli-commands.md, quickstart.md

Tasks are grouped by phase; user-story phases carry `[US#]` labels. `[P]` =
parallelizable (distinct file, no incomplete dependency). Tests for the
persistence layer are required by Constitution Principle II and FR-008/FR-009,
so test tasks are included.

---

## Phase 1: Setup (project initialization)

- [x] T001 Convert `pyproject.toml` to PEP 621 + uv + hatchling: `[project]` metadata, Python ≥ 3.11, console script `pdbsearch = "paper_sorts.cli.app:app"`, runtime deps (sqlalchemy>=2, psycopg[binary]>=3, alembic, typer, rich, pydantic>=2, pydantic-settings>=2, pybtex, pylatexenc, cryptography), dev extras (ruff, mypy, pytest, pytest-postgresql, pytest-cov), `[tool.hatch.build.targets.wheel] packages=["src/paper_sorts"]`, in repo root `pyproject.toml`
- [x] T002 [P] Add `[tool.ruff]`, `[tool.ruff.lint]`, `[tool.ruff.format]`, `[tool.mypy]` (strict on `src`), `[tool.pytest.ini_options]` (testpaths=tests), `[tool.coverage]` config blocks to `pyproject.toml`
- [x] T003 Create the `src/paper_sorts/` package skeleton with `__init__.py` files for `paper_sorts`, `paper_sorts/cli`, `paper_sorts/services`, `paper_sorts/db`
- [x] T004 Run `uv sync --all-extras` to generate `uv.lock` and verify the environment resolves

---

## Phase 2: Foundational (blocking prerequisites for all stories)

- [x] T005 Amend the constitution v1.3.0 → v1.4.0 via `/speckit-constitution`: in §Development Workflow & Quality Gates replace "MUST pass `pylint paper_sorts` and the unittest suite" → "ruff check + ruff format --check + pytest" and the `DatabaseConnector.create_tables()` schema-sync bullet → "Alembic migration under `migrations/versions/` + affected fixtures"; update the SYNC IMPACT REPORT header; in `.specify/memory/constitution.md` (FR-016, SC-007)
- [x] T006 [P] Implement `paper_sorts/config.py`: pydantic-settings v2 `Settings` with `database_url`, `log_level`, `config`/`key` paths; four-source priority (CLI > `PDBSEARCH_*` env > `.env` > Fernet INI via a custom `PydanticBaseSettingsSource`); assemble a SQLAlchemy URL from a decrypted INI's host/port/dbname/user/password; clear actionable error on missing key/config file
- [x] T007 [P] Implement `paper_sorts/logging_config.py`: a single `configure_logging(level, log_file=None)` using `logging.config.dictConfig` — RichHandler→stdout at `level`, optional FileHandler when `log_file` set
- [x] T008 Implement `paper_sorts/db/models.py`: SQLAlchemy 2.x typed declarative `Base` + 4 models mirroring the canonical schema exactly (`Bib`, `Paper`, `AuthorId`, `AuthorPaper`) — nullable text columns, FK `papers.bibtex_id → bib.bibtex_id` named `fk_bibtex_id`, NO FKs on `authors_papers`, no extra indexes (data-model.md)
- [x] T009 Implement `paper_sorts/db/session.py`: engine factory from a URL + `with_session(engine)` context manager (commit on success, rollback on exception, always close) (Constitution IV)
- [x] T010 [P] Define pydantic DTOs `PaperSummary` and `PaperCreate` in `paper_sorts/db/repositories.py` (data-model.md DTO section)
- [x] T011 Scaffold Alembic: `alembic.ini` (script_location=migrations) + `migrations/env.py` (reads `Settings` URL, target_metadata=`Base.metadata`) + `migrations/script.py.mako`
- [x] T012 Author Alembic revision `001_initial_schema.py`: verbatim canonical DDL (the four tables exactly as in data-model.md), with a working `downgrade`
- [x] T013 Set up `tests/conftest.py`: `postgresql_proc` + `ephemeral_db_url` session fixtures off host `pg_ctl`, an `engine`/`session` fixture that runs Alembic (or `Base.metadata.create_all`) to head, and a `seeded_session` fixture loading `tests/fixtures/seed_papers.SEED_PAPERS`
- [x] T014 [P] Create `tests/fixtures/seed_papers.py` with `SEED_PAPERS` (papers w/ multiple authors, a shared-title pair for disambiguation, a BibTeX entry containing LaTeX accents/escapes) and `tests/fixtures/sample.bib`

**Checkpoint**: package imports, DB schema builds in tests, config + logging + session available.

---

## Phase 3: User Story 1 — Reverse-Engineered Architecture Documentation (P1)

**Goal**: a single document a fresh Python dev can read to answer "what does it
do / what is the data model / where do I add a field" without reading source.
**Independent test**: hand `docs/architecture.md` to such a reader; they describe
the four tables + relationships and trace one operation prompt→lookup.

- [x] T015 [P] [US1] Write `docs/architecture.md`: purpose, user journeys, the four-table data model + relationships, control flow (CLI dialog → service → repository → DB), configuration (four-source chain), install/run, and known limitations/quirks (duplicate `Last, First` authors collapse; legacy `bibtext_id` variant; partial-add rollback semantics; bulk-import key-uniqueness skip) — per FR-001, US1 acceptance scenarios

**Checkpoint**: US1 deliverable independently reviewable.

---

## Phase 4: User Story 2 — Modernized Codebase, Same User-Facing Behavior (P1) 🎯 MVP

**Goal**: rebuilt CLI offers search-by-title/author, add, update, delete with
identical prompts/outputs against the same schema.
**Independent test**: scripted dialog through every path produces equivalent or
improved output vs. legacy on the seeded fixture.

- [x] T016 [US2] Implement `PaperRepository`, `AuthorRepository`, `BibRepository` in `paper_sorts/db/repositories.py`: parameterised SQLAlchemy queries + joins for `search_by_title`, `search_by_author`, `add_paper`, `update_*`, `delete_paper`, returning DTOs only (FR-004, FR-014, Principle IV)
- [x] T017 [P] [US2] `tests/test_repositories.py`: real-DB CRUD + search tests (title one/multiple match, author search, add+retrieve, delete cascade of orphan authors, duplicate-key rejection) against `seeded_session` (Principle II, SC-008)
- [x] T018 [US2] Implement `paper_sorts/services/paper_service.py`: `search_by_title`, `search_by_author`, `add_paper`, `update_field` (match/case over `Literal["papers","bib","authors_id"]` with `assert_never`), `delete_paper` — pure orchestration over DTOs, no SQL/rich/IO (FR-014, research R11)
- [x] T019 [P] [US2] `tests/test_paper_service.py`: service-level tests over a real DB (add→search round-trip, update each field, abort-no-write, delete, author-rename merge)
- [x] T020 [US2] Implement `paper_sorts/cli/prompts.py`: the ONLY module doing `input()`/`rich.prompt` — `ask_text` (non-empty re-prompt), `ask_choice` (1-indexed menu w/ abort), `confirm` (dual-form 1/2/y/n/yes/no), `print_paper` (pretty-print), `error` (plain-language) (Principle III)
- [x] T021 [P] [US2] Implement `paper_sorts/cli/search.py` (`search` subcommand: by author / by title, disambiguation, pretty-print, not-found message) (contracts)
- [x] T022 [P] [US2] Implement `paper_sorts/cli/add.py` (`add` subcommand: authors/title/key, BibTeX inline-or-file, summary, persist) (contracts)
- [x] T023 [P] [US2] Implement `paper_sorts/cli/update.py` (`update` subcommand: table→column menus, id, value, dual-form confirm, plain error) (contracts)
- [x] T024 [P] [US2] Implement `paper_sorts/cli/delete.py` (`delete` subcommand: identify, summarise, confirm, remove paper+links+orphans+bib) (contracts)
- [x] T025 [US2] Implement `paper_sorts/cli/app.py`: Typer app wiring all subcommands, `configure_logging` + `Settings` at startup, and the no-subcommand four-option top-level menu (`1) Search 2) Add 3) Update 4) (Q)uit`); `import`/`migrate` registered but absent from the menu (contracts)
- [x] T026 [US2] Remove the legacy flat layout: delete `paper_sorts/add.py`, `search.py`, `get_data.py`, `database_connector.py`, `psycopg_db.py`, `config_reader.py`, `helpers.py`, `run.py`, `user_interaction.py`, and the legacy `paper_sorts/__init__.py`; delete `tests/test_database_connector.py` and `tests/test_user_interaction.py` (FR-012)
- [x] T027 [US2] `tests/test_cli.py`: Typer `CliRunner` end-to-end over the seeded DB — search title one/multiple, search author, add inline, add from `.bib`, update title with y and with n, delete, quit, empty-input re-prompt, plain-language error on failure (SC-002, US2 acceptance scenarios)

**Checkpoint**: MVP — all legacy CLI flows work on the modern stack; legacy code gone.

---

## Phase 5: User Story 3 — Reproducible Test Suite Without Developer-Local State (P2)

**Goal**: fresh checkout runs the suite green with no personal DB.
**Independent test**: clone on a machine that never had `database.crypt`/`key`;
documented install+test commands pass.

- [x] T028 [US3] Verify `tests/conftest.py` + fixtures fully isolate from developer-local state (no `database.crypt`/`key`/hand-curated rows); confirm every row-asserting test references its seed in `tests/fixtures/seed_papers.py` (Principle II, FR-008)
- [x] T029 [P] [US3] `tests/test_config.py`: four-source priority order, the Fernet INI source decrypting a fixture-encrypted config, and the clear-error path on missing key/missing config file (Principle II pure-helper coverage, Edge Cases)

**Checkpoint**: suite is self-contained and deterministic.

---

## Phase 6: User Story 4 — One-Shot Migration of Existing Personal Database (P2)

**Goal**: single idempotent command upgrades either historical schema to
canonical with zero data loss.
**Independent test**: snapshot a legacy DB, migrate, compare row counts
(papers/authors/authorships/bib) — exact match; rerun is a no-op.

- [x] T030 [US4] Author Alembic revision `002_converge_legacy_bibtext_id.py`: idempotently detect a legacy `bibtext_id` column (papers + bib) and converge it onto canonical `bibtex_id` (rename/copy + FK), no-op when already canonical; working `downgrade` (FR-011, AS US4-1/2/3)
- [x] T031 [US4] Implement `paper_sorts/cli/migrate.py` (`migrate` subcommand): run Alembic upgrade to head against the configured DB; create schema from scratch on an empty DB; idempotent rerun
- [x] T032 [P] [US4] `tests/test_migration.py`: build a legacy-`bibtext_id` DB + seed it, run the migration, assert row-count parity for all four tables and spot-check content equality, then rerun and assert idempotency (SC-004)

**Checkpoint**: existing personal DBs upgrade losslessly.

---

## Phase 7: User Story 5 — Bulk Import from LaTeX/BibTeX Preserved (P3)

**Goal**: one command imports all entries from a `.tex`+`.bib` pair.
**Independent test**: run import on a fixture pair with N entries; verify N
papers + authors + bib entries afterward.

- [x] T033 [US5] Implement `paper_sorts/services/import_service.py`: `extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]` (pylatexenc for `.tex`, pybtex for `.bib`), preserving legacy `get_data.py` parsing; skip a citation key with no `.bib` match with a logged warning (FR-002, US5-2)
- [x] T034 [US5] Implement `paper_sorts/cli/importer.py` (`import` subcommand: `--tex`/`--bib`, per-paper commit so a partial failure preserves earlier papers, rerun skips present keys) (Constitution IV, US5-3)
- [x] T035 [P] [US5] Create `tests/fixtures/literature_overview.tex` + `tests/fixtures/bib.bib` (N entries incl. one cited key with no `.bib` match) and `tests/test_import_service.py`: assert N papers/authors/bib present, the unmatched key skipped, and rerun idempotency (US5 acceptance scenarios)

**Checkpoint**: bulk import preserved.

---

## Phase 8: Polish & Cross-Cutting Concerns

- [x] T036 [P] Add `tests/benchmarks/bench_baseline.py` (wall-clock timing of search/add/update/delete on the seeded fixture) + `tests/benchmarks/baseline.json`; mark the harness `@pytest.mark.skip` if it cannot self-calibrate in CI (SC-006, Constitution IV)
- [x] T037 [P] Update `README.md` to the modern install/run/config instructions (mirror quickstart.md)
- [x] T038 Run the full gate: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy src`, `uv run pytest --cov=src/paper_sorts/db`; fix any failures; confirm persistence-layer coverage ≥ 80 % (SC-008) and project LOC under `src/paper_sorts/` down ≥ 30 % vs. legacy ~2 000 (SC-005)
- [x] T039 Drift audit: grep `specs/`, `CLAUDE.md`, `docs/`, the constitution for stale legacy entity names (`PsycopgDB`, `DatabaseConnector`, `ConfigReader`, `bibtext_id` outside the migration/edge-case context, pylint, unittest); reconcile or remove

---

## Dependencies & order

- **Setup (T001–T004)** → everything.
- **Foundational (T005–T014)** → all user stories. T008 (models) precedes
  T012/T013/T016; T009 (session) precedes T016; T011 (Alembic scaffold)
  precedes T012/T030/T031.
- **US1 (T015)** is independent (doc only) — can run any time after Setup.
- **US2 (T016–T027)** is the MVP; depends on Foundational. T016→T018→T020→
  (T021–T024)→T025; T026 (delete legacy) after T025; tests T017/T019/T027
  alongside their targets.
- **US3 (T028–T029)** depends on the conftest/fixtures from Foundational +
  the config from T006.
- **US4 (T030–T032)** depends on Alembic scaffold (T011) + models (T008).
- **US5 (T033–T035)** depends on DTOs (T010) + repositories (T016).
- **Polish (T036–T039)** last.

## Parallel opportunities

- T002 ∥ (after T001); T006 ∥ T007 ∥ T010 ∥ T014 (distinct files).
- Within US2: T021 ∥ T022 ∥ T023 ∥ T024 (separate CLI modules, after T020);
  test tasks T017 ∥ T019 alongside.
- T029, T032, T035, T036, T037 are each `[P]` within their phase.

## Implementation strategy

MVP = Setup + Foundational + US1 + US2. That delivers the reverse-engineered
doc and a behaviour-equivalent modern CLI with the legacy code removed. US3
(self-contained tests), US4 (migration), US5 (bulk import) layer on
incrementally, each independently testable. Polish closes the quality gates
(ruff/mypy/pytest/coverage), the perf baseline, and the drift audit.
