---

description: "Task list for feature 001-modernize-stack"
---

# Tasks: Modernize the Stack

**Input**: Design documents from `specs/001-modernize-stack/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: Tests are **REQUIRED** for this feature (spec FR-008, FR-009, SC-008, and User Story 3). Test tasks are included in each user story phase.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story. Phases 1 and 2 are blocking prerequisites; phases 3–7 deliver the user stories in priority order; phase 8 is cross-cutting polish.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: Which user story this task belongs to (US1–US5). Setup, Foundational, and Polish tasks have no story label.
- File paths in descriptions are repo-relative.

## Path Conventions

- Source: `src/paper_sorts/...` (PEP 517/518 src-layout, mainstream)
- Migrations: `migrations/` (Alembic at repo root)
- Tests: `tests/...`
- Architecture doc: `docs/architecture.md`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project-level configuration changes that every later phase depends on. The constitution amendment is first because spec FR-016 forbids violating the existing constitution while modernizing — every subsequent task assumes the v1.3.0 text is in effect.

- [X] T001 Apply constitution v1.3.0 amendment via `/speckit-constitution` per `specs/001-modernize-stack/research.md` § R10 (five bundled amendments: Principles I–IV plus Stack & Constraints (Section 2) — replace `pylint` with `ruff`; replace `psycopg2`-named driver-isolation rule with persistence-layer rule; replace `unittest` with `pytest`; replace `helpers.get_user_input` references with `paper_sorts.cli.prompts`; replace function-level perf references with layer-level; in Stack & Constraints, replace "Python ^3.10, dependencies managed by Poetry" with "Python ≥ 3.11, dependencies managed by uv" and replace "Driver is `psycopg2`" with "Driver is `psycopg` v3").
- [X] T002 Rewrite `pyproject.toml` to PEP 621 + uv (no `[tool.poetry]` anywhere). Required tables: `[project]` with `name = "paper-sorts"`, `version = "0.1.0"`, `requires-python = ">=3.11"`, `dependencies = [...]` listing `sqlalchemy>=2.0`, `alembic`, `typer`, `rich`, `pydantic-settings>=2.0`, `psycopg[binary]>=3.1`, `pybtex`, `pylatexenc`, `cryptography`. `[project.optional-dependencies]` with `dev = [...]` listing `pytest`, `pytest-postgresql`, `pytest-cov`, `ruff`, `mypy`. `[project.scripts]` with `pdbsearch = "paper_sorts.cli.app:main"`. `[build-system] requires = ["hatchling"]`, `build-backend = "hatchling.build"`. `[tool.hatch.build.targets.wheel] packages = ["src/paper_sorts"]`. `[tool.uv]` block (lockfile config). Tool config: `[tool.ruff]`, `[tool.ruff.lint]`, `[tool.ruff.format]`, `[tool.mypy]` (strict for `src/paper_sorts/`), `[tool.pytest.ini_options]` (with `tests/` paths and pytest-postgresql plugin). Drop `psycopg2-binary` and `pylint` from dependencies entirely.
- [X] T003 Run `uv sync --all-extras` to resolve runtime + dev deps; commit the generated `uv.lock`.
- [X] T004 [P] Create `tests/fixtures/seed_papers.py` and `tests/fixtures/sample.bib` defining a minimal but representative dataset (≥ 3 papers, ≥ 2 with shared author, ≥ 1 with two papers same title for disambiguation testing) referenced by all integration tests.
- [X] T005 [P] Create `docs/` directory with a `.gitkeep` so it tracks; the architecture document lands here in Phase 3.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Capture the SC-006 baseline against the *current* implementation and stand up the test harness skeleton. No framework-bearing code lands in this phase — all baseline measurements must be against the unchanged `paper_sorts/` source so non-regression has a real reference.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [X] T006 Create `tests/conftest.py` with a session-scoped `pytest-postgresql` fixture exposing a clean ephemeral Postgres URL; no Alembic invocation yet (the schema is not under version control until Phase 4).
- [X] T007 Create `tests/benchmarks/bench_baseline.py` — drives the existing `paper_sorts/run.py` CLI via subprocess against the seeded fixture: search-by-title, search-by-author, single add (inline), single update (title), single delete; records wall-clock per operation to `tests/benchmarks/baseline.json`. **Deviation from plan:** the legacy interactive CLI is broken on the search and update journeys (search crashes in `pretty_print_results` on `bibtex_data[1]`; update silently fails when the user types the menu number `1` for the table because `UserInteraction.update` forwards `"1"` to `update_entry` whose dispatch only accepts canonical names). The bench drives `add` + `update` via subprocess (the working subset) and times `search_by_title`, `search_by_author`, `delete` against `DatabaseConnector` directly. The asymmetry is mirrored on the modern side in T046. Both legacy bugs are now noted in `docs/architecture.md` §2.3 / §4.2.
- [X] T008 Run `uv run pytest tests/benchmarks/bench_baseline.py --baseline-record` against the current implementation; commit `tests/benchmarks/baseline.json` as the SC-006 reference and document the host hardware in a header comment.

**Checkpoint**: Baseline captured. Constitution at v1.3.0. Ready for user story implementation.

---

## Phase 3: User Story 1 - Reverse-Engineered Architecture Documentation (Priority: P1)

**Goal**: A single document under `docs/architecture.md` that lets a Python developer understand the current system without reading source.

**Independent Test**: Hand the document to a developer who has never seen the project; they answer "what does it do, what is the data model, where would I add a field" within 30 minutes (spec SC-001).

### Implementation for User Story 1

- [X] T009 [US1] Write `docs/architecture.md` covering: purpose; user journeys (search/add/update/delete/import) with prompt traces; data model (the four-table schema as it actually exists, including the `bibtext_id` legacy variant); control flow `UserInteraction → DatabaseConnector → PsycopgDB`; configuration via Fernet-encrypted INI + `ConfigReader`; install/run via `python paper_sorts/run.py -c ... -k ...`; known limitations (per-class log files, mocked-DB tests forbidden, legacy procedural modules `add.py`/`search.py`/`get_data.py`, schema variants `bibtex_id` vs `bibtext_id`, duplicate authorship rows tolerated). Reference module names verbatim because the document is also the rename map for Phase 4.

**Checkpoint**: Architecture document complete. It is the acceptance reference for User Story 2 — every flow it describes must work in the modernized version.

---

## Phase 4: User Story 2 - Modernized Codebase, Same User-Facing Behavior (Priority: P1) 🎯 MVP

**Goal**: An end user runs the rebuilt CLI and gets every operation they had before — same prompts, same outputs, same data — against the same personal database, with internals built on mainstream Python libraries.

**Independent Test**: Scripted dialog through every existing CLI path (search by title with one match, search by title with multiple matches, search by author, add inline, add from `.bib`, update each updatable field, abort an update at confirmation, delete entry, quit) produces equivalent or improved output to the current version against the same seeded fixture (spec SC-002).

### Persistence layer (db/)

- [X] T010 [P] [US2] Create namespace files: `src/paper_sorts/__init__.py`, `src/paper_sorts/cli/__init__.py`, `src/paper_sorts/services/__init__.py`, `src/paper_sorts/db/__init__.py`.
- [X] T011 [US2] Create `src/paper_sorts/db/models.py` per `specs/001-modernize-stack/data-model.md` SQLAlchemy 2.x sketch: `Paper` (table `papers`), `Author` (table `authors_id`, column `author`), `BibEntry` (table `bib`, table-level `UNIQUE (bibtex)`), `Authorship` (table `authors_papers`, **no DDL FKs** on `author_id` or `paper_id`); all string/int columns nullable per the verbatim original schema; `relationship(secondary=...)` uses explicit `primaryjoin`/`secondaryjoin` so ORM navigation works without DDL FKs.
- [X] T012 [US2] Create `src/paper_sorts/db/session.py` — `create_engine(database_url, future=True)`; `sessionmaker(...)`; `with_session()` context manager that commits on success and rolls back + re-raises on exception. No connection pool sizing beyond SQLAlchemy default (constitution Principle IV).

### Migrations (initial schema)

- [X] T013 [US2] Run `alembic init migrations` from repo root. Edit `migrations/env.py` to import `target_metadata = paper_sorts.db.models.Base.metadata` and to read the database URL from `paper_sorts.config.Settings`.
- [X] T014 [US2] Create `migrations/versions/001_initial_schema.py` per `specs/001-modernize-stack/contracts/database-schema.md` — verbatim DDL of the original `create_tables()`: `CREATE TABLE IF NOT EXISTS` for `bib`, `papers`, `authors_id`, `authors_papers`; **no NOT NULL** outside primary keys; **no FK** on `authors_papers`; the only FK is `papers.bibtex_id → bib.bibtex_id`; the only UNIQUE outside PKs is `bib.bibtex`. `downgrade()` drops in reverse order.

### Configuration & logging

- [X] T015 [P] [US2] Create `src/paper_sorts/config.py` — pydantic-settings `Settings` model with `database_url`, `log_level`, `log_file`, `fernet_config`, `fernet_key`; sources in priority order CLI args > env (`PDBSEARCH_*`) > `.env` > Fernet INI (custom `pydantic_settings.PydanticBaseSettingsSource` subclass); raise `ValueError("Fernet config requires a key file")` when `fernet_config` is set without `fernet_key` (spec edge case "lost key").
- [X] T016 [P] [US2] Create `src/paper_sorts/logging_config.py` — single `logging.config.dictConfig`; default sinks: `RichHandler` to stdout at `INFO`, optional `FileHandler` when `Settings.log_file` is set; called once from `cli/app.py` at startup.

### Repositories & service layer

- [X] T017 [US2] Create `src/paper_sorts/db/repositories.py` — `PaperRepository.find_by_title`, `find_by_author`, `add`, `update_field` (only `title` or `contents`), `delete`; `AuthorRepository.upsert(name)`, `update_name(id, name)`; `BibRepository.add(bibtex_id, bibtex)`, `update(bibtex_id, bibtex)`. Plus pydantic models `PaperSummary` and `PaperCreate` colocated here so `services/` and `cli/` import them without crossing the SQLAlchemy boundary. This file is the **only** place outside `db/session.py` and `db/models.py` that imports `sqlalchemy`.
- [X] T018 [US2] Create `src/paper_sorts/services/paper_service.py` — `search_by_title(title)`, `search_by_author(name)`, `add_paper(PaperCreate)` (single transaction: insert bib, insert paper, insert/upsert authors, link via `authors_papers`; rolls back atomically on any failure — replaces `DatabaseConnector.rollback_database_addition`), `update_field(table, field, identifier, value)` (preserves the table×field grid from `UserInteraction.update`; rejects updating `bibtex_id` itself), `delete_paper(paper_id)` (cascades `authors_papers` cleanup, removes orphan authors, conditionally removes the bib row when no other paper references it).

### CLI prompts (the UX-consistency wrapper)

- [X] T019 [P] [US2] Create `src/paper_sorts/cli/prompts.py` — wraps `rich.prompt.Prompt.ask`, `IntPrompt.ask`, `Confirm.ask`. Public functions: `ask_text(prompt: str) -> str` (re-prompts on empty), `ask_choice(prompt: str, options: list[str]) -> int` (1-indexed; mandatory abort/quit option as the last entry; re-prompts on out-of-range), `ask_confirm(prompt: str) -> bool` (accepts `1`/`y`/`yes` for true, `2`/`n`/`no` for false, anything else returns false and logs an error). This is the **only** module under `src/paper_sorts/` permitted to import `rich.prompt` (constitution Principle III v1.3.0).

### CLI subcommands

- [X] T020 [P] [US2] Create `src/paper_sorts/cli/search.py` — `pdbsearch search [--by author|title --query Q]` per `specs/001-modernize-stack/contracts/cli-commands.md`; interactive when flags absent; calls `paper_service.search_by_*`; renders results with the existing pretty-print format (title / authors joined with " and " / summary / bib).
- [X] T021 [P] [US2] Create `src/paper_sorts/cli/add.py` — `pdbsearch add [--bib-file F --summary S]`; interactive otherwise; preserves the prompt sequence in cli-commands.md.
- [X] T022 [P] [US2] Create `src/paper_sorts/cli/update.py` — `pdbsearch update`; preserves the two-step menu (papers/bib/authors → field → id → confirm); rejects updating `bibtex_id` itself.
- [X] T023 [P] [US2] Create `src/paper_sorts/cli/delete.py` — `pdbsearch delete [--id N]`; mandatory confirmation showing paper title and id; calls `paper_service.delete_paper`.

### CLI app wiring

- [X] T024 [US2] Create `src/paper_sorts/cli/app.py` — Typer app with global callback for `--config / --key / --database-url / --log-level`; default command (no subcommand) drops into the top-level menu **with the original four options only**: 1 search / 2 add / 3 update / 4 quit (per `contracts/cli-commands.md` § "Why only four options" — delete, import, and migrate are subcommands but not menu entries). Uses `prompts.ask_choice`; initialises logging once via `logging_config`; opens a session via `db.session.with_session` for each subcommand invocation.
- [X] T025 [US2] Wire search/add/update/delete subcommands into `cli/app.py` (importer and migrate are added in their own user-story phases). `pdbsearch --help` lists them.

### Remove the legacy implementation

- [X] T026 [US2] `git rm` the old flat-layout source: `paper_sorts/run.py`, `paper_sorts/user_interaction.py`, `paper_sorts/database_connector.py`, `paper_sorts/psycopg_db.py`, `paper_sorts/config_reader.py`, `paper_sorts/helpers.py`, `paper_sorts/add.py`, `paper_sorts/search.py`, `paper_sorts/get_data.py`, and the now-empty `paper_sorts/__init__.py`. Remove the `paper_sorts/` directory. Drop the `legacy-baseline` extra (and `psycopg2-binary` with it) from `pyproject.toml`; remove the `prepend_sys_path = src` workaround from `alembic.ini`; remove the `uv sync --extra legacy-baseline` install line from `docs/architecture.md` § 6. Verify `python -c "import paper_sorts; print(paper_sorts.__file__)"` resolves under `src/paper_sorts/`.
- [X] T027 [US2] Update `README.md` install/run section: replace `python run.py` invocations with `pdbsearch`; update the configuration example to mention the three new sources (env / .env / Fernet INI) per `specs/001-modernize-stack/quickstart.md`.

### Unit tests for the new pure modules

- [X] T028 [P] [US2] `tests/unit/test_prompts.py` — empty input re-prompts; menu rejects 0 and out-of-range input; `1`/`y`/`yes` confirm and `2`/`n`/`no` abort; unrecognised confirmation returns False and emits a log.
- [X] T029 [P] [US2] `tests/unit/test_config.py` — env / .env / Fernet sources resolve in correct priority; `Settings(fernet_config=..., fernet_key=None)` raises `ValueError` with the documented message; absent `database_url` from any source raises `ValueError`.

### MVP gate

- [ ] T030 [US2] Manually drive the top-level menu end-to-end against an Alembic-migrated test DB seeded with `tests/fixtures/seed_papers.py`: confirm every dialog path from `contracts/cli-commands.md` produces equivalent output. Document the run as a checkpoint commit.

**Checkpoint**: User Story 2 complete. The modernized CLI works end-to-end against a fresh DB. The legacy `paper_sorts/` directory is gone; `src/paper_sorts/` is the only source tree.

---

## Phase 5: User Story 3 - Reproducible Test Suite Without Developer-Local State (Priority: P2)

**Goal**: A fresh-clone test run completes successfully on a machine that has never had the project's personal database.

**Independent Test**: From a clean checkout, `git clone && uv sync --all-extras && uv run pytest` succeeds in under 5 minutes wall-clock without any `database.crypt`/`key` file present (spec SC-003).

### Test infrastructure

- [X] T031 [US3] Extend `tests/conftest.py`: the session-scoped pytest-postgresql fixture now runs `alembic upgrade head` against the ephemeral DB once per session, then loads `tests/fixtures/seed_papers.py`; add a per-test transaction-rollback fixture for isolation between tests.

### Integration tests for User Story 2's CLI surface

- [X] T032 [P] [US3] `tests/integration/test_search.py` — single-match-by-title, multi-match disambiguation, no-match-by-title; same three for author search.
- [X] T033 [P] [US3] `tests/integration/test_add.py` — add inline; add from `.bib` file; duplicate `bibtex_id` rejected with plain-language error; missing `.bib` file rejected; partial-failure rolls back atomically (no orphan rows).
- [X] T034 [P] [US3] `tests/integration/test_update.py` — update title; update contents; update bibtex; abort confirmation leaves data unchanged; non-existent paper id rejected; attempting to update `bibtex_id` itself is rejected.
- [X] T035 [P] [US3] `tests/integration/test_delete.py` — delete with `authors_papers` cascade; orphan author removed; bib row preserved if another paper still references it; non-existent id rejected.

### Coverage and fresh-checkout gates

- [ ] T036 [US3] Run `uv run pytest --cov=paper_sorts.db --cov-fail-under=80`; verify SC-008 passes. If under 80 %, add focused tests for the uncovered repository methods.
- [ ] T037 [US3] On a fresh clone (or simulate via `git clean -fdx && uv sync --all-extras`), without any `database.crypt`/`key` file in the working tree or in `../../`, run `uv run pytest` and verify it succeeds in under 5 minutes (SC-003).

**Checkpoint**: The test suite runs from any clean checkout. CI is unblocked.

---

## Phase 6: User Story 4 - One-Shot Migration of Existing Personal Database (Priority: P2)

**Goal**: A user with a personal database (current `bibtex_id` schema or legacy `bibtext_id` schema) runs one command and ends up on the modernized schema with all rows preserved.

**Independent Test**: Snapshot row counts on a real existing DB, run `pdbsearch migrate`, compare counts — they match exactly (spec SC-004).

- [ ] T038 [US4] Create `migrations/versions/002_legacy_bibtext_to_bibtex.py` per `specs/001-modernize-stack/contracts/database-schema.md`: reflective via `sa.inspect(bind)`; rename `papers.bibtext_id → bibtex_id` if the legacy column is present; same for `bib.bibtext_id`; idempotent on already-modern DB; `downgrade()` raises `NotImplementedError` with the documented reason.
- [ ] T039 [US4] Create `src/paper_sorts/cli/migrate.py` — `pdbsearch migrate` subcommand: invokes `alembic.command.upgrade(cfg, "head")` against `Settings.database_url`; reports row counts (`papers`, `authors_id`, `bib`, `authors_papers`) before and after; idempotent (prints `Schema is at head (002). No migrations to apply.` when current).
- [ ] T040 [US4] Wire `migrate` into `cli/app.py` as a Typer subcommand only — **not** as a menu entry. Migrate is an admin/setup operation; the same UX-surface-preservation reasoning that keeps delete and import out of the menu (see `contracts/cli-commands.md` § "Why only four options") applies here. Add a single-line reference to it in README and `quickstart.md`.
- [ ] T041 [P] [US4] `tests/integration/test_migrations.py` — fresh DB ends at revision 002; modern DB is a no-op (counts unchanged); legacy `bibtext_id` DB renames cleanly with row counts identical pre/post; an interrupted migration (simulate via `monkeypatch` on `op.alter_column` raising mid-call) leaves the DB on the previous revision and a re-run converges to 002.

**Checkpoint**: Existing personal databases can be upgraded with one command. SC-004 verified.

---

## Phase 7: User Story 5 - Bulk Import from LaTeX/BibTeX Preserved (Priority: P3)

**Goal**: A user with a `.tex` literature overview and a corresponding `.bib` file imports all entries via a single command, just as the current `get_data.py` allows.

**Independent Test**: Run import against a fixture pair; verify N papers + their authors + their bib entries are present afterward.

- [ ] T042 [US5] Create `src/paper_sorts/services/import_service.py` — port the parsing logic from old `paper_sorts/get_data.py:get_data` and `paper_sorts/helpers.py:get_bibtex_information` into a clean iterator: `extract_papers_from_tex_bib(tex_path: Path, bib_path: Path) -> Iterator[PaperCreate]`. Per-paper yielding preserves the per-paper-commit semantic (constitution Principle IV bulk-import rule). Logs WARN when a `.tex` cite has no matching `.bib` record; logs INFO when an import call already exists in the DB and is therefore skipped.
- [ ] T043 [US5] Create `src/paper_sorts/cli/importer.py` — `pdbsearch import <tex-file> <bib-file>` subcommand; iterates `import_service.extract_papers_from_tex_bib` and calls `paper_service.add_paper` for each yielded record inside its own transaction; reports total inserted, skipped, and warned counts on completion.
- [ ] T044 [US5] Wire `import` into `cli/app.py`.
- [ ] T045 [P] [US5] `tests/integration/test_import.py` — happy path with the fixture `.tex`/`.bib` pair (N entries, all inserted); missing-bib-key path (entry skipped, WARN logged, import continues); idempotent re-run (second invocation inserts 0 new, all skipped); partial-failure (inject a service-layer exception on the 3rd entry; verify entries 1 and 2 are persisted).

**Checkpoint**: All five user stories delivered.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Verify the success criteria that span the whole feature and clean up.

- [ ] T046 Run `uv run pytest tests/benchmarks/bench_baseline.py --baseline-compare`; verify SC-006 (no measurable regression vs. `tests/benchmarks/baseline.json` from T008). If a regression is observed, investigate root cause; do **not** add indexes or other speculative optimisations — the constitution permits them only with a Complexity Tracking entry, and any need would have to be measurement-justified.
- [ ] T047 [P] Run `uv run ruff check . && uv run ruff format --check .`; fix any violations.
- [ ] T048 [P] Run `uv run mypy src/`; fix any type errors.
- [ ] T049 Verify SC-005 — line count under `src/paper_sorts/` (excluding `tests/` and `migrations/`) is at least 30 % lower than the pre-modernization `paper_sorts/` line count. Capture the before/after numbers in the final commit message. If the cut is < 30 %, the difference is likely lingering boilerplate worth investigating before merge.
- [ ] T050 Update `CLAUDE.md` — replace the "3-layer architecture" description (currently `UserInteraction → DatabaseConnector → PsycopgDB`) with the new layered architecture (`cli/` → `services/` → `db/`); update the legacy-modules paragraph (they no longer exist); SPECKIT START/END markers continue to point at `specs/001-modernize-stack/plan.md` until merge.
- [ ] T051 Re-validate `specs/001-modernize-stack/checklists/requirements.md` against the implemented system; mark items complete; add a "verified-on" date.
- [ ] T052 Final review pass: skim the diff against `main`; confirm the branch reads cleanly in commit order — amendment → baseline → architecture doc → modernization → tests → migration → import → polish; squash housekeeping commits if any are noise.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: T001 first (constitution amendment unblocks the ruff/pytest/etc. config that follows). T002 depends on T001. T003 depends on T002. T004, T005 are parallel to each other and depend only on T003 having installed the right packages.
- **Phase 2 (Foundational)**: Depends on Phase 1. T007 depends on T006 (uses the conftest fixture). T008 depends on T007.
- **Phase 3 (US1)**: Depends on Phase 2. Independent of all later phases — the architecture document is written from the **current** code, before User Story 2 deletes it.
- **Phase 4 (US2)**: Depends on Phase 2 and on Phase 3 being either complete or in flight (the architecture doc is the acceptance reference; without it, equivalence claims are unverifiable).
- **Phase 5 (US3)**: Depends on Phase 4 — the integration tests target the modernized CLI.
- **Phase 6 (US4)**: Depends on Phase 4 (needs the modernized DB layer + Alembic). Independent of Phase 5; could run in parallel if staffed.
- **Phase 7 (US5)**: Depends on Phase 4. Independent of Phase 5 and Phase 6; could run in parallel if staffed.
- **Phase 8 (Polish)**: Depends on Phases 4–7.

### User Story Dependencies

- **US1 (P1)** is fully independent — written from the existing code as documentation; can start the moment T008 commits.
- **US2 (P1, MVP)** requires US1 *to exist as a draft* so that "same UX" claims are testable. The detailed scenarios in US1's document are the acceptance reference.
- **US3 (P2)** requires US2's CLI/service/repository surface to exist — the integration tests drive it.
- **US4 (P2)** requires US2's persistence layer and Alembic wiring (T013, T014). Independent of US3.
- **US5 (P3)** requires US2's `paper_service.add_paper`. Independent of US3 and US4.

### Within Each User Story

- Within US2: persistence layer (T010–T014) → config & logging (T015, T016) → repositories & service (T017, T018) → CLI prompts (T019) → CLI subcommands (T020–T023) → app wiring (T024, T025) → legacy removal (T026, T027) → tests (T028, T029) → MVP gate (T030).
- Within US3: conftest extension (T031) → integration tests in parallel (T032–T035) → coverage gate (T036) → fresh-checkout gate (T037).
- Within US4: Alembic revision (T038) → CLI subcommand (T039) → wiring (T040) → tests (T041).
- Within US5: service (T042) → CLI subcommand (T043) → wiring (T044) → tests (T045).

### Parallel Opportunities

- **Phase 1**: T004 ‖ T005 after T003.
- **Phase 4 (US2)** has the largest parallel surface:
  - After T010: T011 + T015 + T016 + T019 in parallel (different files, no internal dependencies).
  - After T017 + T019: T020 + T021 + T022 + T023 in parallel (each subcommand is its own file).
  - T028 ‖ T029 (different test files).
- **Phase 5 (US3)**: T032 + T033 + T034 + T035 in parallel after T031.
- **Phase 8**: T047 ‖ T048.

---

## Parallel Example: User Story 2

```bash
# After persistence-layer skeleton (T010-T014) is in place,
# launch the four CLI subcommand implementations in parallel:
Task: "Create src/paper_sorts/cli/search.py"
Task: "Create src/paper_sorts/cli/add.py"
Task: "Create src/paper_sorts/cli/update.py"
Task: "Create src/paper_sorts/cli/delete.py"

# After T017 + T018 + T019 land, launch the unit tests in parallel:
Task: "Create tests/unit/test_prompts.py"
Task: "Create tests/unit/test_config.py"
```

## Parallel Example: User Story 3

```bash
# After T031 (conftest extended), launch all integration tests in parallel:
Task: "Create tests/integration/test_search.py"
Task: "Create tests/integration/test_add.py"
Task: "Create tests/integration/test_update.py"
Task: "Create tests/integration/test_delete.py"
```

---

## Implementation Strategy

### MVP First (US1 + US2 only)

1. Phases 1, 2, 3, 4 — that's the minimum that delivers a usable modernized CLI on top of the existing personal database.
2. **STOP and VALIDATE**: at T030, manually drive every dialog from `contracts/cli-commands.md` and confirm equivalence. If anything regresses, fix before proceeding.
3. The MVP can be merged to `main` at this point; Phases 5–7 are quality + completeness improvements that can ship in follow-up commits or remain on the feature branch.

### Incremental Delivery

After MVP merges, US3, US4, and US5 are each independently shippable increments. Recommended order:

1. **US3 first** — because without it, the MVP's equivalence claim is verified manually-once. US3 makes it verified-on-every-commit.
2. **US4 second** — addresses real user data. The user with an existing personal DB cannot use the modernized stack until `pdbsearch migrate` exists.
3. **US5 last** — least common operation; delaying it has the lowest user impact.

### Parallel Team Strategy (if multiple developers)

After Phase 4 completes, US3, US4, and US5 are mutually independent. Three developers could pick up one each.

---

## Notes

- [P] tasks = different files, no incomplete dependencies.
- [Story] label maps tasks to spec.md user stories for traceability. Setup, Foundational, and Polish phases have no story label per the format rules.
- This plan deliberately inserts the constitution amendment (T001) as the very first task and the baseline benchmark (T007–T008) as the **last** Foundational tasks before any user-story code. Both are gates: amendment unlocks the framework swap; baseline gives SC-006 a real reference.
- The architecture document (T009 / US1) is written **before** the legacy modules are deleted (T026 / US2), so it captures the system as it actually existed.
- No speculative optimisations: indexes, async drivers, connection pools beyond default, caching layers — all forbidden without measurement-driven Complexity Tracking entries (constitution Principle IV).
- The schema is preserved verbatim from the original (no NOT NULL added, no FK added on `authors_papers`); tightening lives at the application layer per `data-model.md`'s schema-vs-application invariant table.
