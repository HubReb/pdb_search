# Tasks: Modernize the Stack

**Feature**: 001-modernize-stack | **Branch**: `001-modernize-stack`
**Input**: plan.md, research.md, data-model.md, contracts/, quickstart.md

Constitution: v1.3.0-b2-hardened (this worktree). Mechanical gates that MUST be
satisfied (not waived): **G1** per-layer ≥80% coverage, **G2** executing
baseline benchmark, **G3** doc-currency forbidden-token check.

Conventions: all paths absolute from repo root `/home/rebekka/projects/pdb_search-repOH2`.
`[P]` = parallelizable (distinct file, no incomplete dependency).

---

## Phase 1: Setup

- [ ] T001 Write `pyproject.toml` (PEP 621 `[project]`, Python ≥3.11, `hatchling` build backend, `[project.scripts] pdbsearch`, src-layout package discovery, runtime deps SQLAlchemy 2.x / psycopg[binary] v3 / alembic / typer / pydantic-settings / cryptography / pybtex / pylatexenc, dev extras pytest / pytest-postgresql / pytest-cov / mypy / ruff, `[tool.ruff]`, `[tool.pytest.ini_options]`, `[tool.coverage]`, `[tool.mypy]` strict on src) at `pyproject.toml`
- [ ] T002 Create src-layout package skeleton: `src/paper_sorts/__init__.py`, `src/paper_sorts/cli/__init__.py`, `src/paper_sorts/services/__init__.py`, `src/paper_sorts/db/__init__.py`
- [ ] T003 Run `uv sync --all-extras` and confirm the environment resolves (lockfile `uv.lock` written)
- [ ] T004 [P] Write `alembic.ini` and `migrations/env.py` + `migrations/script.py.mako` wired to read the DB URL from `paper_sorts.config` and target `paper_sorts.db.models` metadata

## Phase 2: Foundational (blocking prerequisites)

- [ ] T005 Implement the four ORM models (`Paper`, `Bib`, `Author`, `AuthorPaper`) with SQLAlchemy 2.x typed declarative + relationships, preserving the schema contract (no NOT NULL outside PKs, no DDL FK on `authors_papers`, no extra indexes) in `src/paper_sorts/db/models.py`
- [ ] T006 Define pydantic DTOs `PaperSummary` and `PaperCreate` per data-model.md in `src/paper_sorts/db/repositories.py`
- [ ] T007 Implement `db/session.py`: engine factory + `with_session(engine)` context manager (commit on success, rollback on exception, deterministic close) in `src/paper_sorts/db/session.py`
- [ ] T008 Implement `paper_sorts.config.Settings` (pydantic-settings v2) with the four-source priority chain (CLI flags > `PDBSEARCH_*` env > `.env` > Fernet-encrypted INI custom source) and a `ConfigError` for missing-key/decrypt-failure in `src/paper_sorts/config.py`
- [ ] T009 [P] Implement `logging_config.py`: single `dictConfig` (RichHandler to stdout + optional FileHandler), `setup_logging(level, file)` in `src/paper_sorts/logging_config.py`
- [ ] T010 Implement `cli/prompts.py` (the ONLY module importing `rich.prompt`): `ask_text` (non-empty re-prompt), `ask_choice` (1-indexed menu, mandatory abort, re-prompt on out-of-range), `confirm` (accepts `1`/`2`/`y`/`n`/`yes`/`no`) in `src/paper_sorts/cli/prompts.py`
- [ ] T011 Alembic revision `001_initial_schema`: verbatim port of the original canonical-`bibtex_id` DDL (four tables) in `migrations/versions/001_initial_schema.py`
- [ ] T012 Set up `tests/conftest.py` (`postgresql_proc`, `ephemeral_db_url`, an engine fixture, and a `seeded_session`/`seeded_engine` that runs Alembic to head and loads the seed) and the co-located seed dataset `tests/fixtures/seed_papers.py` (`SEED_PAPERS`, reproducing legacy fingerprints `Pino, J.` / `Wang2021LargeScaleSA` / the multi-author speech-to-speech row)

## Phase 3: User Story 2 — Modernized codebase, same behavior (P1, MVP) 🎯

**Goal**: same search/add/update/delete CLI behaviour on the modern stack.
**Independent test**: scripted dialog through every CLI path produces equivalent
output to legacy against the seed data (`test_cli.py` via `CliRunner`).

- [ ] T013 [US2] Implement `PaperRepository` (`search_by_title`, `search_by_author` parameterised joins returning `PaperSummary`; `add`; `update_title`/`update_contents`; `delete` removing links + orphaned authors + paper + bib), `AuthorRepository.rename`, `BibRepository.update_bibtex` in `src/paper_sorts/db/repositories.py`
- [ ] T014 [P] [US2] Persistence-layer tests (real DB, no mocking): search by title (1 match / multi-match), search by author, add+retrieve, update title/contents/bibtex/author, delete with author orphan cleanup; reference `SEED_PAPERS` in `tests/test_repositories.py`
- [ ] T015 [US2] Implement `services/paper_service.py` (`search_by_title`, `search_by_author`, `add_paper`, `update_field` with `match`/`case` over `Literal[...]` table + `assert_never`, `delete_paper`) raising typed domain errors in `src/paper_sorts/services/paper_service.py`
- [ ] T016 [P] [US2] Service-layer tests for paper_service (incl. update_field rejecting ID columns and `authors_papers`; duplicate-bibtex error) in `tests/test_paper_service.py`
- [ ] T017 [US2] Implement `cli/search.py` interactive flow (author/title sub-menu, disambiguation, legacy pretty-print output, plain not-found message) in `src/paper_sorts/cli/search.py`
- [ ] T018 [US2] Implement `cli/add.py` (authors/title/key/file-or-inline-bib/summary prompts via prompts.py; atomic persist) in `src/paper_sorts/cli/add.py`
- [ ] T019 [US2] Implement `cli/update.py` (table menu → column menu → id → value → confirmation summary) in `src/paper_sorts/cli/update.py`
- [ ] T020 [US2] Implement `cli/delete.py` (identify → summarise → confirm → delete) in `src/paper_sorts/cli/delete.py`
- [ ] T021 [US2] Implement `cli/app.py`: Typer app wiring all subcommands, global config options (`--database-url`/`--log-level`/`--config`/`--key`), `setup_logging` at startup, and the no-subcommand four-option top-level menu (Search/Add/Update/Quit) in `src/paper_sorts/cli/app.py`
- [ ] T022 [P] [US2] Interface-layer tests (G1): `CliRunner` exercising `search`, `add`, `update`, `delete` (+ `import`, `migrate` smoke) and the no-subcommand menu; abort/quit/empty-reprompt/confirm-no paths in `tests/test_cli.py`

## Phase 4: User Story 3 — Reproducible test suite (P2)

**Goal**: suite runs on a fresh checkout with no developer-local DB.
**Independent test**: `uv run pytest` passes with no `database.crypt`/`key`.

- [ ] T023 [P] [US3] Config-layer tests: four-source priority resolution, `PDBSEARCH_*` env, `.env`, Fernet-encrypted source round-trip, missing-key → `ConfigError` in `tests/test_config.py`
- [ ] T024 [US3] Verify full suite is self-contained (no path to `../../database.crypt`/`key`); add a coverage configuration that reports per-layer and confirm `uv run pytest` green on a clean tree

## Phase 5: User Story 4 — One-shot migration (P2)

**Goal**: upgrade either historical schema to canonical in one idempotent command.
**Independent test**: seed a `bibtext_id` (sic) DB and a `bibtex_id` DB; migrate;
row counts identical; rerun is a no-op.

- [ ] T025 [US4] Alembic revision `002_converge_legacy`: idempotent rename of legacy `bibtext_id`→`bibtex_id` on `papers`/`bib` guarded on `information_schema` (no-op on canonical/fresh) in `migrations/versions/002_converge_legacy.py`
- [ ] T026 [US4] Implement `cli/migrate.py` (`pdbsearch migrate`): inspect schema, bring to head idempotently, report row counts unchanged in `src/paper_sorts/cli/migrate.py`
- [ ] T027 [P] [US4] Migration tests (real DB): build a legacy-`bibtext_id` schema with rows → migrate → assert canonical + identical paper/author/authorship/bib counts; build canonical → migrate is no-op; rerun idempotent in `tests/test_migrate.py`

## Phase 6: User Story 5 — Bulk import preserved (P3)

**Goal**: import all entries from a `.tex`+`.bib` pair, per-paper commit.
**Independent test**: import a fixture pair with N entries → N papers/authors/bib.

- [ ] T028 [US5] Implement `services/import_service.extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]` (pybtex + pylatexenc; skip+log keys with no `.bib` match) in `src/paper_sorts/services/import_service.py`
- [ ] T029 [US5] Implement `cli/importer.py` (`pdbsearch import --tex --bib`, per-paper commit, dupes skipped via BibTeX-key uniqueness) in `src/paper_sorts/cli/importer.py`
- [ ] T030 [P] [US5] Import-service tests + a `.tex`/`.bib` fixture pair: N entries imported, unmatched key skipped, partial-failure preserves earlier papers in `tests/test_import_service.py` (+ fixtures under `tests/fixtures/`)

## Phase 7: User Story 1 — Reverse-engineered architecture doc (P1)

**Goal**: a single architecture document for the legacy stack.
**Independent test**: a fresh Python dev can answer "what does it do / data model
/ where to add a field" from the doc alone.

- [ ] T031 [P] [US1] Write `docs/architecture.md`: purpose, user journeys, four-table data model + relationships, control flow (CLI dialog → service → persistence), config approach, install/run, rollback semantics, known limitations/quirks (duplicate-author identity, `bibtext_id` typo schema) in `docs/architecture.md`

## Phase 8: Polish & Cross-Cutting (gates, docs, legacy removal)

- [ ] T032 Remove legacy flat-layout modules `paper_sorts/add.py`, `paper_sorts/search.py`, `paper_sorts/get_data.py`, and the rest of the legacy `paper_sorts/` package + legacy `tests/test_*.py` once covered (FR-012)
- [ ] T033 Rewrite `README.md` for the modern stack — MUST NOT contain forbidden tokens `Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB` (G3) in `README.md`
- [ ] T034 Rewrite `CLAUDE.md` for the modern stack (architecture, commands, schema, tests, SpecKit pointer to plan) — MUST NOT contain the G3 forbidden tokens in `CLAUDE.md`
- [ ] T035 [P] Implement the doc-currency test (G3): a case-sensitive search asserting `README.md`/`CLAUDE.md` contain none of `Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB` in `tests/test_doc_currency.py`
- [ ] T036 Implement the executing baseline benchmark (G2): `tests/benchmarks/bench_baseline.py` times the five interactive ops against the seeded DB and writes/asserts `tests/benchmarks/baseline.json` — must run (not `@pytest.mark.skip`); add `tests/benchmarks/__init__.py`
- [ ] T037 Amend the constitution via `/speckit-constitution` (FR-016/SC-007): Development Workflow & Quality Gates `pylint paper_sorts`+unittest → `ruff check src tests`+`pytest`; `DatabaseConnector.create_tables()` → Alembic migrations under `migrations/versions/` (PATCH bump)
- [ ] T038 Per-layer coverage gate (G1): run `uv run pytest --cov` and confirm each of `db/`, `services/`, `cli/`, `config.py` independently ≥80%; add tests where any layer is short
- [ ] T039 Final green gate: `uv run ruff check src tests`, `uv run ruff format --check src tests`, `uv run mypy src`, `uv run pytest` all pass; verify SC-005 line-count reduction ≥30% vs legacy ~2300

---

## Dependencies & order

- **Setup (T001–T004)** before everything.
- **Foundational (T005–T012)** blocks all user stories (models, DTOs, session,
  config, prompts, revision 001, conftest+seed).
- **US2 (T013–T022)** is the MVP — depends only on Foundational.
- **US3 (T023–T024)** depends on config (T008) + conftest (T012); can follow US2.
- **US4 (T025–T027)** depends on models + revision 001 + session.
- **US5 (T028–T030)** depends on `PaperCreate` (T006) + repositories (T013).
- **US1 (T031)** is doc-only; independent (describes the *legacy* stack, so do
  it before/while legacy is still present, but no code dependency).
- **Polish (T032–T039)**: legacy removal (T032) after US2/US4/US5 cover it;
  G1/G2/G3 + constitution amendment + final gate last.

## Parallel opportunities

- T004 ∥ T001/T002 groundwork; T009 ∥ T008.
- Within US2: tests T014/T016/T022 ∥ each other once their targets exist.
- T023, T027, T030, T031, T035 are `[P]` across their phases (distinct files).

## MVP scope

**US2 (Phase 3)** delivered on top of Setup+Foundational is the minimal usable
product: a modern CLI with search/add/update/delete behaviour-equivalent to
legacy, tested against a real ephemeral DB. US3/US4/US5/US1 and the gates layer
on top.

## Implementation strategy

Build bottom-up: Setup → Foundational → US2 (MVP, prove parity early) → US3
(self-contained suite) → US4 (migration) → US5 (bulk import) → US1 (arch doc) →
Polish (legacy removal, G1/G2/G3 gates, FR-016 amendment, final green). Commit
per phase.
