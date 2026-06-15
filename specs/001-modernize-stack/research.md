# Phase 0 Research: Modernize the Stack

All Technical Context unknowns were resolved during reverse-engineering of the legacy stack and
constitution review. The constitution already names most of the target frameworks (Stack &
Constraints section), so the research below documents *why* those picks satisfy the spec, plus
the few decisions the constitution leaves open (CLI framework, migration tool, BibTeX library).

## R1 — ORM / database toolkit

- **Decision**: SQLAlchemy 2.x (declarative ORM) over psycopg v3 (binary).
- **Rationale**: Constitution Stack & Constraints mandates psycopg v3 + SQLAlchemy 2.x isolated
  to the persistence layer. SQLAlchemy 2.x gives parameterised queries, joins, and transactions
  (FR-004) with mainstream-recognisable patterns. psycopg v3 is the actively maintained driver
  (psycopg2 is legacy). The 2.x `Session` + `select()` style is the current idiom.
- **Alternatives considered**: raw psycopg (status quo — rejected, that is exactly the bespoke
  SQL the spec removes); SQLModel (thin wrapper, less mainstream for a pure-DB tool); Django ORM
  (too heavy, brings a framework we do not need).

## R2 — Migration tool

- **Decision**: Alembic, migrations under `migrations/versions/`.
- **Rationale**: Alembic is SQLAlchemy's first-party migration tool — versioned, reversible
  migrations applied in order (FR-005), replacing the runtime `create_tables()` call. Revision
  001 ports the legacy *canonical* DDL verbatim. Revision 002 converges the legacy `bibtext_id`
  typo-column variant onto canonical and is written to be idempotent (FR-011).
- **Alternatives considered**: hand-rolled SQL scripts (no version graph, no idempotency
  guarantees); yoyo / sqitch (not SQLAlchemy-native).

## R3 — CLI framework

- **Decision**: Typer (built on Click), with `rich` for formatted output.
- **Rationale**: Typer maps subcommands to functions with type-hint-driven parsing (FR-006),
  recognisable to any 2026 Python dev. Subcommands (search/add/update/delete/migrate/import)
  map cleanly to the legacy top-level menu. With no subcommand, the app drops into the legacy
  four-option interactive menu so the existing UX is preserved verbatim. `rich` renders the
  "pretty print" output and menus.
- **Alternatives considered**: argparse (status quo — bespoke dialog loop, rejected); plain
  Click (Typer is the type-hint-native superset and more concise); cleo (less mainstream).

## R4 — Configuration

- **Decision**: pydantic-settings v2 `Settings` model, four sources in priority order: CLI flags
  > `PDBSEARCH_*` env > `.env` > Fernet-encrypted INI (custom settings source).
- **Rationale**: FR-007 requires a mainstream settings library with env + `.env` support, and
  the Fernet workflow preserved as one source. pydantic-settings gives typed validation and a
  documented source-priority chain. The Fernet INI is wrapped as a custom
  `PydanticBaseSettingsSource` so the legacy encrypted-config path stays available.
- **Alternatives considered**: dynaconf (heavier, less type-safe); bare configparser + os.environ
  (re-implements what pydantic-settings provides); environs (env-only, no Fernet hook).

## R5 — Test runner & ephemeral DB

- **Decision**: pytest + pytest-postgresql; pytest-cov for coverage.
- **Rationale**: Constitution Principle II mandates pytest with pytest-postgresql as the
  canonical ephemeral-DB mechanism, and forbids mocking the SQLAlchemy session/repositories in
  persistence tests. pytest-postgresql spins a real PG from the host `pg_ctl` per session
  (FR-008, SC-003). pytest-cov measures persistence-layer coverage (SC-008 ≥ 80 %).
- **Alternatives considered**: unittest (status quo — bare, no fixtures, rejected per FR-009);
  testcontainers (heavier, needs a container runtime; pytest-postgresql is lighter and is the
  named mechanism); mocking the DB (forbidden by Principle II).

## R6 — Lint / format

- **Decision**: ruff (`ruff check` + `ruff format`); mypy strict on `src/`.
- **Rationale**: FR-010 requires a mainstream lint tool and, if not pylint, a constitution
  amendment. The constitution is already at ruff (Principle I, v1.3.0). mypy strict enforces the
  full-type-hints rule. The residual `pylint paper_sorts` mention in the Development-Workflow
  section is stale and amended in this change set.
- **Alternatives considered**: pylint (status quo — slower, less mainstream in 2026); flake8 +
  black + isort (ruff subsumes all three).

## R7 — BibTeX parsing & LaTeX decoding

- **Decision**: keep pybtex (parse `.bib`, round-trip entries) and pylatexenc (LaTeX → text in
  the `.tex` import path).
- **Rationale**: Spec Assumptions permit keeping pybtex (or an equivalent). pybtex round-trips
  accents/escapes (`\"o`, `\&`, `{Pino}`) without corruption (edge case in spec). pylatexenc
  decodes the `.tex` literature overview as the legacy `get_data` did.
- **Alternatives considered**: bibtexparser (viable, but pybtex already works and preserves the
  exact author-name extraction `Last, First`); writing a parser (re-introduces bespoke glue).

## R8 — Schema preservation contract

- **Decision**: Replicate the legacy *canonical* DDL exactly in Revision 001 — four tables,
  `papers(id, title, contents, bibtex_id→bib.bibtex_id)`, `bib(bibtex_id PK, bibtex UNIQUE)`,
  `authors_id(id, author)`, `authors_papers(id, author_id, paper_id)` with **no DDL FKs** on the
  link table. Do not add NOT NULL outside PKs, do not add FKs to `authors_papers`, do not add
  indexes beyond the existing primary keys.
- **Rationale**: Principle IV forbids new tables/indexes/denormalisation without a
  Complexity-Tracking entry; the spec's Key Entities and edge cases assume the existing shape
  (e.g. duplicate `Last, First` authors treated as one). Tightening the schema would change
  behaviour and is out of scope.
- **Alternatives considered**: adding FKs/uniqueness to `authors_papers` (rejected — changes
  legacy semantics and trips the preservation contract); normalising author names (rejected —
  the duplicate-author behaviour is a documented limitation to preserve).

## R9 — Migration of historical schema variants

- **Decision**: The `migrate` command runs `alembic upgrade head`, and before stamping, detects
  whether the target DB uses the legacy `bibtext_id`/`bibtext` column names (from the old
  `get_data.py`/`add.py` path) and renames them to the canonical `bibtex_id`/`bibtex`. All steps
  run in one transaction; rerun is a no-op once converged (idempotent, FR-011, SC-004).
- **Rationale**: Two historical variants exist (the `DatabaseConnector` canonical `bibtex_id`
  and the procedural-module `bibtext_id` typo). A single user action must converge either with
  zero data loss and be re-runnable.
- **Alternatives considered**: separate commands per variant (worse UX, FR-011 says one action);
  dump/restore (risk of loss, not idempotent).

## R10 — Constitution amendment text (FR-016 / SC-007)

- **Decision**: Amend the **Development Workflow & Quality Gates** section via
  `/speckit-constitution` to a v1.3.1 PATCH bump: replace `pylint paper_sorts` → `ruff check src
  tests` + `ruff format --check`; replace "unittest suite (where the live development database is
  available)" → "pytest suite (ephemeral PostgreSQL via pytest-postgresql)"; replace the
  schema-change clause referencing `DatabaseConnector.create_tables()` → "land as Alembic
  migrations under `migrations/versions/` and update affected fixtures/assertions in the same
  change". Principles I–IV already reflect the target state in v1.3.0; this PATCH removes the
  last stale references so SC-007 ("no silent deviations") holds.
- **Rationale**: The principles were modernised in v1.3.0 but the workflow/governance prose was
  not fully swept; FR-016 forbids silently violating the stale text. A PATCH bump is correct
  because this is wording cleanup, not a new/redefined principle.
- **Alternatives considered**: leaving the stale text (rejected — SC-007 forbids silent
  deviation); MINOR/MAJOR bump (rejected — no principle is added or redefined).
