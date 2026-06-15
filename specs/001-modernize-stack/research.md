# Phase 0 Research: Modernize the Stack

All Technical Context unknowns are resolved here. The spec defers concrete
framework picks to this phase ("mainstream framework" = widely adopted,
actively maintained, recognisable to a 2026 Python developer). The
constitution's v1.3.0 Stack & Constraints section already commits several of
these, so most decisions confirm an existing constraint rather than open a
new one.

## R1. ORM / database toolkit → SQLAlchemy 2.x + psycopg v3

- **Decision**: SQLAlchemy 2.x (typed declarative ORM + Core) over psycopg v3
  (binary). Repositories wrap a `Session`; services never see ORM types.
- **Rationale**: Mandated by Constitution §Stack & Constraints (FR-004).
  SQLAlchemy 2.x's `Mapped[...]` / `mapped_column` give first-class type hints
  satisfying Principle I; parameterised queries + joins + transactions
  (FR-004) are native. psycopg v3 is the supported driver; the legacy code
  already half-migrated psycopg2→psycopg3 inconsistently (root cause of the
  driver-isolation rule).
- **Alternatives**: raw psycopg3 (keeps hand-written SQL — the thing being
  removed); SQLModel (thin wrapper, less mature, pulls pydantic into the ORM
  layer and blurs the DTO boundary); Django ORM / Peewee (wrong ecosystem fit,
  Django is web-framework baggage).

## R2. CLI framework → Typer

- **Decision**: Typer for subcommands (`search/add/update/delete/import/
  migrate`) plus a no-subcommand top-level four-option menu.
- **Rationale**: FR-006 ("mainstream CLI framework", subcommands SHOULD map to
  the top-level menu). Typer is type-hint-driven (Principle I synergy), built
  on Click, bundles Rich. `typer.testing.CliRunner` gives end-to-end CLI tests
  without a subprocess. `import` and `migrate` are admin/scripted operations —
  subcommand-only, deliberately absent from the four-option menu.
- **Alternatives**: argparse (the bespoke thing being replaced); Click (Typer
  is Click + type hints, strictly nicer here); python-fire (less explicit
  contract).

## R3. Migrations → Alembic

- **Decision**: Alembic, `migrations/versions/`. Revision 001 = verbatim port
  of the legacy DDL (canonical schema). Revision 002 = converge the legacy
  `bibtext_id` (sic) variant onto canonical `bibtex_id` (FR-011), idempotent.
- **Rationale**: FR-005 (versioned, reversible migrations replacing the
  runtime `create_tables()`). Alembic is the SQLAlchemy-native standard.
  Idempotency (FR-011, AS US4-3): revision 002 inspects the live schema and
  only renames/copies when the legacy column is present; rerun is a no-op.
- **Alternatives**: yoyo-migrations / raw SQL scripts (not SQLAlchemy-native,
  more glue); keeping `create_tables()` (explicitly removed by FR-005).

## R4. Schema preservation (the hard constraint)

- **Decision**: Replicate the original four tables **exactly**, including their
  looseness. Canonical column is `bibtex_id` (the `DatabaseConnector` spelling,
  not the `get_data.py` `bibtext_id` typo).
  - `bib(bibtex_id TEXT PK, bibtex TEXT UNIQUE)`
  - `papers(id SERIAL PK, title TEXT, contents TEXT, bibtex_id TEXT FK→bib.bibtex_id)`
  - `authors_id(id SERIAL PK, author TEXT)`
  - `authors_papers(id SERIAL PK, author_id INT, paper_id INT)` — **no DDL FKs**.
- **Rationale**: Constitution Principle IV + schema-preservation contract: do
  NOT add NOT NULL outside PKs, do NOT add FKs to `authors_papers`, do NOT add
  indexes the original lacked. The legacy DDL leaves `papers.title/contents`,
  `authors_id.author`, and both `authors_papers` int columns nullable; the ORM
  models must mirror that (`nullable=True`). The original `bib` DDL is the
  malformed `bibtex text unique (bibtex)` — interpreted as `bibtex TEXT UNIQUE`.
- **Alternatives**: "fixing" the schema (adding FKs to `authors_papers`,
  NOT NULL on title) — rejected: breaks the preservation contract and the
  migration's row-count parity guarantee.

## R5. Config → pydantic-settings v2, four-source chain

- **Decision**: `paper_sorts.config.Settings` (pydantic-settings v2) with
  priority: CLI flags (`--database-url`, `--log-level`) > env (`PDBSEARCH_*`)
  > `.env` > Fernet-encrypted INI (custom `PydanticBaseSettingsSource`).
- **Rationale**: FR-007 (mainstream settings lib, env + `.env`, Fernet
  preserved as one source). pydantic-settings is the standard; custom sources
  are first-class. Lost-key / missing-file → clear actionable error, not a
  stack trace (Edge Case, FR-003).
- **Alternatives**: dynaconf (heavier, less type-safe); hand-rolled
  `ConfigReader` (the bespoke thing being removed); environ-config (smaller
  ecosystem).
- **Detail**: connection settings collapse to a single SQLAlchemy URL
  (`postgresql+psycopg://user:pass@host:port/db`) assembled from the
  host/port/dbname/user/password the Fernet INI and legacy config expose.

## R6. Logging → dictConfig + RichHandler

- **Decision**: One `logging.config.dictConfig` in `logging_config.py`:
  RichHandler → stdout at the configured level; optional FileHandler when a log
  path is configured. Called once from `cli/app.py` startup.
- **Rationale**: FR-013 (mainstream logging; per-class file output MAY remain
  as an option but MUST NOT be the only sink). Replaces the five bespoke
  `create_logger(...)` FileHandler factories. Principle III: failures log full
  detail AND surface a short plain-language line; raw exceptions never reach
  stdout.
- **Alternatives**: structlog (nice but extra dependency for a personal tool);
  loguru (non-stdlib, fights `dictConfig`); keeping per-class loggers (the
  thing being consolidated).

## R7. Testing → pytest + pytest-postgresql + pytest-cov

- **Decision**: pytest; `pytest-postgresql` spins an ephemeral PG per session
  from the host `pg_ctl`; persistence tests run against it (no mocks).
  `pytest-cov` measures coverage (SC-008 ≥ 80 % on the persistence layer).
  Seed dataset co-located at `tests/fixtures/seed_papers.py::SEED_PAPERS`.
- **Rationale**: FR-008/FR-009, Constitution Principle II. Fresh-checkout
  runs with no `database.crypt`/`key` (FR-008, US3). CliRunner exercises CLI
  paths against the seeded DB (SC-002). Migration tests build a legacy-schema
  DB, run the migration, assert row-count parity + idempotency (SC-004).
- **Alternatives**: unittest (replaced, FR-009); testcontainers (needs Docker;
  pytest-postgresql off host `pg_ctl` is lighter and matches the dev env);
  SQLite in-memory (not PostgreSQL — would not exercise the real SQL,
  forbidden by Principle II).

## R8. Linting/formatting → ruff (+ mypy)

- **Decision**: `ruff check` + `ruff format --check` as the gate; `mypy --strict`
  on `src/`. Configured in `pyproject.toml`.
- **Rationale**: FR-010 (mainstream lint; if not pylint, amend the Code Quality
  principle in the same change — already done in v1.3.0). ruff is the de-facto
  2026 standard, fast, replaces pylint+flake8+isort. mypy enforces Principle
  I's full-type-hint rule on `src/`.
- **Alternatives**: pylint (legacy default, slow, FR-010 permits replacing);
  flake8 (superseded by ruff); black+isort separately (ruff format subsumes).

## R9. Packaging → uv + hatchling, src-layout

- **Decision**: PEP 621 `[project]` metadata, `uv.lock`, `hatchling` build
  backend, `src/paper_sorts/` layout. Console script `pdbsearch` →
  `paper_sorts.cli.app:app`.
- **Rationale**: Constitution §Stack & Constraints (uv, hatchling). src-layout
  prevents accidental import of the un-installed package and is the modern
  default. uv is the toolchain in the dev environment.
- **Alternatives**: Poetry (the current tool; constitution moved off it);
  setuptools flat-layout (the legacy arrangement being replaced).

## R10. BibTeX parsing → pybtex (retained)

- **Decision**: Keep pybtex for parse + serialize; pylatexenc for `.tex` →
  text in the bulk importer. LaTeX accents/escapes (`\"o`, `\&`, `{Pino}`)
  must round-trip (Edge Case).
- **Rationale**: Assumption in spec: "BibTeX parsing remains a dedicated
  library (currently pybtex); switching is permitted if functionally
  equivalent." No equivalent buys anything here, so retain. Import service
  exposes `extract_papers_from_tex_bib(tex, bib) -> Iterator[PaperCreate]`,
  preserving `get_data.py`'s per-paper semantics.
- **Alternatives**: bibtexparser (viable but no functional gain; pybtex already
  handles the person-name splitting the code relies on).

## R11. Service/DTO boundary

- **Decision**: `db/repositories.py` exposes pydantic DTOs (`PaperSummary`,
  `PaperCreate`) and repository classes; services consume only DTOs.
  `update_field` uses `match`/`case` over a `Literal[...]` table arg with
  `assert_never(table)` for compile-time exhaustiveness.
- **Rationale**: Principle I driver/ORM isolation — services importing ORM
  types would leak persistence into the domain. DTOs are the contract.
  `assert_never` turns the legacy stringly-typed `update_entry(table=...)`
  into a statically-checked dispatch (mypy-verified exhaustiveness).
- **Alternatives**: returning ORM rows from repositories (leaks Session
  lifetime + ORM types into services — rejected); dataclasses instead of
  pydantic DTOs (pydantic already present for config; reuse for validation).

## R12. Constitution amendment scope (FR-016, SC-007)

- **Decision**: One MINOR bump 1.3.0 → 1.4.0 amending only the **Development
  Workflow & Quality Gates** section: "`pylint paper_sorts` and the unittest
  suite" → "ruff + pytest"; the `DatabaseConnector.create_tables()` schema-sync
  bullet → "Alembic migration under `migrations/versions/` + fixtures".
- **Rationale**: The four *principles* were already amended to v1.3.0 text
  describing the modern stack; only the Workflow section still names the legacy
  tools. FR-016 forbids silently violating it; SC-007 requires no silent
  deviation remains. No principle is removed/relaxed → MINOR, not MAJOR.
- **Alternatives**: leave it and "just not run pylint" (forbidden by FR-016 +
  SC-007); MAJOR bump (overstated — nothing is relaxed).

## R13. Behaviour-preservation fidelity (the acceptance bar)

- **Decision**: `docs/architecture.md` (FR-001) documents the legacy control
  flow, data model, config, quirks, and rollback semantics; the modern CLI is
  validated path-by-path against it (SC-002): search-by-title one/multiple
  matches, search-by-author, add inline, add from `.bib`, update each field
  with y/n confirm, delete, bulk import, abort dialogs, quit, empty-input
  re-prompt, plain-language error on failure.
- **Rationale**: FR-002/FR-003 + US2. The architecture doc is both deliverable
  and acceptance reference. Known legacy quirks documented and preserved:
  duplicate `Last, First` authors collapse to one (limitation); interrupted
  bulk import skips already-imported keys via BibTeX-key uniqueness; partial
  add rolls back its own bib/author rows.
- **Alternatives**: skip the doc (violates FR-001 + US1); "improve" behaviours
  silently (violates the no-observable-regression bar of US2).
