# Tasks: Modernize the Stack

**Feature**: 001-modernize-stack | **Branch**: `001-modernize-stack` (worktree `rep/001-OH3`)
**Input**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

Tests are first-class here: the constitution (v1.3.0-b2-hardened) mandates real-DB integration tests
(Principle II), per-layer ≥80% coverage (gate G1), an executing baseline benchmark (gate G2), and a
doc-currency token scan (gate G3). Those are merge-blocking, so test tasks appear inline with each story.

Story → phase map: US1 (P1) architecture doc · US2 (P1, MVP) modern CLI/services/db parity · US3 (P2)
fresh-checkout test suite · US4 (P2) migration command · US5 (P3) bulk import.

---

## Phase 1: Setup

- [ ] T001 Write `pyproject.toml` at repo root: PEP 621 `[project]` (name `paper-sorts`, `requires-python = ">=3.11"`), runtime deps (sqlalchemy>=2, alembic, psycopg[binary]>=3, typer, rich, pydantic-settings>=2, cryptography, pybtex, pylatexenc), `[project.optional-dependencies] dev` (pytest, pytest-postgresql, pytest-cov, ruff, mypy), `[project.scripts] pdbsearch = "paper_sorts.cli.app:app"`, hatchling build backend (`packages = ["src/paper_sorts"]`), and `[tool.ruff]`/`[tool.mypy]` (strict on src)/`[tool.pytest.ini_options]` sections.
- [ ] T002 Create the `src/paper_sorts/` package skeleton with empty `__init__.py` in `src/paper_sorts/`, `src/paper_sorts/cli/`, `src/paper_sorts/services/`, `src/paper_sorts/db/`, and `tests/` + `tests/fixtures/` + `tests/benchmarks/` dirs.
- [ ] T003 Run `uv sync --all-extras` and confirm the environment resolves; capture `uv.lock`.

---

## Phase 2: Foundational (blocking prerequisites)

- [ ] T004 [P] Implement `src/paper_sorts/db/models.py`: SQLAlchemy 2.x typed declarative `Base` + four models `Bib` (table `bib`), `Paper` (`papers`), `Author` (`authors_id`), `AuthorPaper` (`authors_papers`) mirroring the preserved schema (no extra NOT NULL/FK/index); `Paper.authors` relationship via the link table for query ergonomics only.
- [ ] T005 [P] Implement `src/paper_sorts/db/session.py`: `make_engine(database_url)` and a `with_session(engine)` context manager that commits on clean exit, rolls back on exception, and always closes.
- [ ] T006 [P] Implement `src/paper_sorts/config.py`: pydantic-settings v2 `Settings` (database_url, log_level, log_file, fernet config/key paths) with a custom `PydanticBaseSettingsSource` decrypting the Fernet INI; four-source priority CLI > env (`PDBSEARCH_*`) > `.env` > Fernet INI; clear error on missing/unreadable key.
- [ ] T007 [P] Implement `src/paper_sorts/logging_config.py`: single `logging.config.dictConfig` with a RichHandler to stdout and an optional FileHandler, exposed as `configure_logging(level, log_file=None)`.
- [ ] T008 Scaffold Alembic: `alembic.ini` + `migrations/env.py` (reads `database_url` from Settings, target_metadata = `Base.metadata`) + `migrations/script.py.mako`.
- [ ] T009 Author Alembic revision `001_initial_schema` in `migrations/versions/`: create the four tables exactly as the legacy canonical DDL (`bib(bibtex_id PK, bibtex UNIQUE)`, `papers(id, title, contents, bibtex_id FK→bib)`, `authors_id(id, author)`, `authors_papers(id, author_id, paper_id)` with no FKs). Verbatim port — no schema tightening.

---

## Phase 3: User Story 1 — Reverse-Engineered Architecture Doc (P1)

**Goal**: a single document a fresh Python dev can use to answer what/data-model/where-to-add-a-field without reading source.
**Independent test**: hand `docs/architecture.md` to a new reader; they describe the four tables + relationships and trace "search by author" prompt→lookup.

- [ ] T010 [US1] Write `docs/architecture.md` describing the **legacy** stack (read from `paper_sorts/*.py` before deletion): purpose; user journeys; the four-table data model + relationships; control flow CLI dialog → `UserInteraction` → `DatabaseConnector` → `PsycopgDB` → DB; encrypted-INI config; install/run; and quirks (the `bibtex_id` vs `bibtext_id` schema split, duplicate-author identity collapse, mid-add rollback semantics).

---

## Phase 4: User Story 2 — Modernized Codebase, Same Behaviour (P1) 🎯 MVP

**Goal**: every legacy CLI flow works identically on the modern stack.
**Independent test**: scripted dialog over every path (search title single/multi, search author, add inline, add from .bib, update title, update bibtex, abort update, delete, quit) yields equivalent output against seeded data.

### Persistence layer

- [ ] T011 [US2] Implement DTOs in `src/paper_sorts/db/repositories.py`: pydantic `PaperSummary` (paper_id, title, authors, summary, bibtex_id, bibtex) and `PaperCreate` (title, summary, bibtex_id, bibtex, authors).
- [ ] T012 [US2] Implement `PaperRepository` in `src/paper_sorts/db/repositories.py`: `get_by_title`, `add`, `update_title`, `update_contents`, `delete`, `exists_bibtex_id` — parameterised SQLAlchemy queries with joins, returning DTOs/primitives only.
- [ ] T013 [US2] Implement `AuthorRepository` in `src/paper_sorts/db/repositories.py`: `get_papers_by_author`, `rename`, `link` (create author if absent), `unlink_all_for_paper` (drop orphan authors).
- [ ] T014 [US2] Implement `BibRepository` in `src/paper_sorts/db/repositories.py`: `add`, `update` (reject duplicate bibtex), `delete`.

### Service layer

- [ ] T015 [US2] Implement `src/paper_sorts/services/paper_service.py`: `search_by_title`, `search_by_author`, `add_paper` (bib+paper+links, rollback on failure), `update_field` (`match`/`case` over `Literal["papers","bib","authors_id"]` with `assert_never`, refusing `*_id` columns), `delete_paper` (links→orphan authors→paper→bib). Opens `with_session`; no SQL/rich/I/O.

### Presentation layer

- [ ] T016 [US2] Implement `src/paper_sorts/cli/prompts.py`: the ONLY module importing `rich.prompt`. Provide `ask_text` (re-prompt until non-empty), `ask_choice` (1-indexed menu with mandatory abort, out-of-range re-prompts), `confirm` (accepts `1`/`2` and `y`/`n`/`yes`/`no`), and a pretty-print of a `PaperSummary` matching legacy format.
- [ ] T017 [US2] Implement `src/paper_sorts/cli/search.py`: interactive search (by author / by title), disambiguation list for multiple matches, plain "not found" message; uses paper_service + prompts.
- [ ] T018 [US2] Implement `src/paper_sorts/cli/add.py`: prompt author CSV→title→bibtex key→inline-vs-file bib→summary; build `PaperCreate`; call `add_paper`; plain success/failure message.
- [ ] T019 [US2] Implement `src/paper_sorts/cli/update.py`: table→column→id→new value→confirmation summary; abort writes nothing; calls `update_field`.
- [ ] T020 [US2] Implement `src/paper_sorts/cli/delete.py`: locate paper by title, confirm with summary, call `delete_paper`.
- [ ] T021 [US2] Implement `src/paper_sorts/cli/app.py`: Typer app wiring `search`/`add`/`update`/`delete` (+ `import`/`migrate` registered later); root callback resolves Settings + calls `configure_logging`; no-subcommand invocation drops into the four-option top-level menu (Search/Add/Update/Quit). Failures log + plain-language stdout, never raw tracebacks.

### Tests (real DB)

- [ ] T022 [P] [US2] Create `tests/fixtures/seed_papers.py` with `SEED_PAPERS` (≥2 papers incl. a shared-title pair and a multi-author paper, plus a LaTeX-accent bib entry) and a helper to load it into a session.
- [ ] T023 [US2] Implement `tests/conftest.py`: `postgresql_proc` + `ephemeral_db_url` session fixtures; a `migrated_db` fixture running Alembic `upgrade head`; a `seeded_session` fixture loading `SEED_PAPERS`.
- [ ] T024 [P] [US2] Write `tests/test_db_repositories.py`: real-DB tests for all three repositories (CRUD, joins, orphan-author cleanup, duplicate-bibtex rejection), asserting against co-located `SEED_PAPERS`.
- [ ] T025 [P] [US2] Write `tests/test_services.py`: real-DB tests for `paper_service` (search title single/multi, search author, add+rollback, update each field + `_id` refusal, delete).
- [ ] T026 [P] [US2] Write `tests/test_prompts.py`: unit tests for `cli/prompts` covering empty input re-prompt, malformed/out-of-range re-prompt, and success for `ask_text`/`ask_choice`/`confirm`.
- [ ] T027 [US2] Write `tests/test_cli.py`: Typer `CliRunner` end-to-end over `search`/`add`/`update`/`delete` (and the no-subcommand menu) against the migrated/seeded DB — satisfies interface-layer coverage (G1).
- [ ] T028 [US2] Remove the legacy flat layout: delete `paper_sorts/` (all modules) and the legacy `tests/test_database_connector.py` / `tests/test_user_interaction.py` (FR-012), once parity tests above are green.

---

## Phase 5: User Story 3 — Reproducible Test Suite, No Local State (P2)

**Goal**: fresh checkout, no personal DB, suite passes.
**Independent test**: clone with no `database.crypt`/`key`; `uv sync` + `uv run pytest` passes.

- [ ] T029 [US3] Verify `tests/conftest.py` provisions PG purely via pytest-postgresql off host `pg_ctl` with zero dependency on `database.crypt`/`key`/personal rows; document the seed-data coupling in fixture comments.
- [ ] T030 [P] [US3] Write `tests/test_config.py`: unit tests for the four-source priority chain (CLI > env > .env > Fernet INI) incl. the lost-key clear-error path — no live DB needed.
- [ ] T031 [US3] Add per-layer coverage config (gate G1): pytest-cov over `src/paper_sorts/{db,services,cli,config.py}`; confirm each layer independently ≥80%; record the command in quickstart.

---

## Phase 6: User Story 4 — One-Shot Migration (P2)

**Goal**: upgrade either historical schema to canonical in one idempotent action, zero data loss.
**Independent test**: snapshot row counts (papers/authors/links/bib) before & after `migrate`; they match.

- [ ] T032 [US4] Author Alembic revision `002_converge_legacy_typo` in `migrations/versions/`: rename `bibtext_id`/`bibtext` → `bibtex_id`/`bibtex` in `bib` and `papers` **only when present** (guarded on `information_schema.columns`); idempotent, transactional.
- [ ] T033 [US4] Implement `src/paper_sorts/cli/migrate.py` and register it in `app.py` as a subcommand-only command running Alembic `upgrade head` against the configured DB.
- [ ] T034 [P] [US4] Write `tests/test_migrations.py`: real-DB tests — fresh DB → canonical schema; a hand-built legacy-typo DB → converged with row counts preserved; rerun is a no-op (idempotency).

---

## Phase 7: User Story 5 — Bulk Import from LaTeX/BibTeX (P3)

**Goal**: import all entries from a `.tex`+`.bib` pair via one command, per-paper commit.
**Independent test**: run import over fixture pair with N entries; N papers + authors + bib present; key with no .bib match skipped.

- [ ] T035 [US5] Implement `src/paper_sorts/services/import_service.py`: `extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]` (pybtex + pylatexenc), yielding one `PaperCreate` per cited entry with a matching `.bib` record; unmatched keys skipped.
- [ ] T036 [US5] Implement `src/paper_sorts/cli/importer.py` and register `import` in `app.py` (subcommand-only): iterate the extractor, call `add_paper` per item (per-paper commit), log a warning on skipped/unmatched keys.
- [ ] T037 [P] [US5] Add `tests/fixtures/literature_overview.tex` + `tests/fixtures/sample.bib` and write `tests/test_import.py`: real-DB import of the pair inserts N papers/authors/bib; unmatched key skipped; rerun does not duplicate.

---

## Phase 8: Polish & Cross-Cutting Concerns

- [ ] T038 [P] Implement the baseline benchmark harness (gate G2) under `tests/benchmarks/` (`conftest.py`, `bench_baseline.py`, `baseline.json`): measure + record wall-clock for the five interactive ops (search title, search author, add, update, delete) against a seeded DB; it MUST execute (not be permanently skipped).
- [ ] T039 [P] Write `tests/test_doc_currency.py` (gate G3): case-sensitive assertion that `README.md` and `CLAUDE.md` contain none of `Poetry`, `psycopg2`, `UserInteraction`, `PsycopgDB`.
- [ ] T040 Rewrite `README.md` for the modern stack (uv install, `pdbsearch` subcommands, config priority, ephemeral-PG tests) — doc-currency clean (no forbidden tokens).
- [ ] T041 Final gate sweep: `uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy src`, `uv run pytest` (incl. coverage + benchmark) all green; fix any residual type/lint/coverage gaps.

---

## Dependencies & Execution Order

- **Setup (T001–T003)** → blocks everything.
- **Foundational (T004–T009)** → blocks all user stories (models/session/config/logging/migrations needed everywhere).
- **US1 (T010)** is independent of the code and can run anytime after the legacy modules are read (do it before T028 deletes them).
- **US2 (T011–T028)** is the MVP; depends on Foundational. T028 (delete legacy) runs last in the story, after parity tests pass.
- **US3 (T029–T031)** depends on US2's tests existing.
- **US4 (T032–T034)** depends on Foundational (Alembic) + US2 (CLI app to register `migrate`).
- **US5 (T035–T037)** depends on US2 (`add_paper`) + Foundational.
- **Polish (T038–T041)** last; T039/T040 enforce/clean docs, T041 is the green-build gate.

## Parallel opportunities

- T004–T007 (models/session/config/logging) are independent files → parallel.
- Within US2: T024/T025/T026 (separate test files) parallel; T011–T014 share `repositories.py` so serialise.
- T030, T034, T037, T038, T039 are independent test/bench files → parallel.

## MVP scope

US1 + US2 (architecture doc + modern CLI with full behavioural parity and real-DB tests). US3–US5 are
incremental P2/P3 increments layered on the same foundation.
