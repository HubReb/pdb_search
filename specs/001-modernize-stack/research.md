# Phase 0 Research: Modernize the Stack

All "mainstream framework" choices the spec deferred to `/speckit-plan` are resolved here. Each
decision records rationale and the alternatives weighed. The constitution (v1.3.0-b2-hardened in this
worktree) already names most of these, so several decisions are constitution-confirmations rather than
open choices.

## R1 — ORM / database toolkit

- **Decision**: SQLAlchemy 2.x (typed `Mapped[...]` declarative models) over the existing four-table schema.
- **Rationale**: Mainstream, actively maintained, recognisable in 2026; FR-004 demands parameterised queries, joins, transactions — SQLAlchemy gives all three with compile-time-typed models. Constitution Stack & Constraints names it explicitly.
- **Alternatives**: Raw psycopg v3 + SQL (status quo, the thing being removed); Peewee/Tortoise (smaller ecosystems, async-leaning — async is out of scope per Principle IV); Django ORM (drags in a web framework, violates CLI-only FR-017).

## R2 — Database driver

- **Decision**: psycopg v3 (`psycopg[binary]`), isolated to `db/`.
- **Rationale**: Legacy already half-migrated to psycopg v3 in `get_data.py`; constitution mandates v3 binary. SQLAlchemy 2.x speaks psycopg3 natively via `postgresql+psycopg://`.
- **Alternatives**: psycopg2 (legacy, named as a forbidden doc token — being removed); asyncpg (async, out of scope).

## R3 — Migration tool

- **Decision**: Alembic, two revisions. Rev 001 = verbatim port of the legacy canonical DDL (`bibtex_id` schema with the `bib.bibtex UNIQUE` constraint and the `papers→bib` FK). Rev 002 = idempotent converger that renames the legacy typo columns `bibtext_id`/`bibtext` (from `get_data.py`) to canonical `bibtex_id`/`bibtex` when present.
- **Rationale**: FR-005 (versioned, reversible), FR-011 (single-action, idempotent, both historical schemas). Alembic is the mainstream companion to SQLAlchemy. Idempotency achieved by guarding each step on `information_schema` column existence so a rerun is a no-op and a mid-run failure rolls back in a transaction (FR-011 / US4 scenario 3).
- **Schema-preservation contract**: Rev 001 reproduces the original DDL exactly — no NOT NULL added outside PKs, no FK added to `authors_papers`, no indexes beyond the original PKs/UNIQUE. This is a hard constraint from the constitution memory and the spec edge cases.
- **Alternatives**: Hand-rolled SQL migration script (status quo `create_tables()` — not versioned/reversible); `yoyo`/`sqitch` (smaller ecosystems).

## R4 — CLI framework

- **Decision**: Typer (on Click) with rich output.
- **Rationale**: FR-006 mainstream CLI framework; subcommands map cleanly to the legacy menu (`search`/`add`/`update`/`delete`/`import`) plus `migrate`. Typer's `CliRunner` (Click's) gives the end-to-end interface-layer coverage that gate G1 requires. Invoked with no subcommand, the app drops into the legacy four-option top-level menu (Search / Add / Update / Quit) to preserve the exact interactive UX of `UserInteraction.interact`.
- **Menu vs subcommand split**: `migrate` and `import` are admin/scripted one-shots — subcommand-only, deliberately absent from the four-option menu, matching the legacy reality where bulk import lived in a separate `get_data.py` script and there was no interactive migrate.
- **Alternatives**: argparse (status quo, bespoke loop — removed); plain Click (Typer is the thinner, type-hint-native layer over it); cleo (Poetry-coupled, niche).

## R5 — Settings library

- **Decision**: pydantic-settings v2 `Settings` model with a custom Fernet-INI source, four-source priority order: CLI flags (`--database-url`, `--log-level`) > env (`PDBSEARCH_*`) > `.env` > Fernet-encrypted INI (`--config`/`--key`).
- **Rationale**: FR-007 (env + `.env`, Fernet preserved as one source). pydantic-settings is the mainstream typed-settings library; a custom `PydanticBaseSettingsSource` wraps the legacy `ConfigReader` decrypt logic so the encrypted workflow survives. Constitution names it.
- **Lost-key handling**: missing key file or decrypt failure surfaces a clear, actionable message (not a stack trace) — spec edge case.
- **Alternatives**: dynaconf (heavier, config-file-centric); bare `os.environ` + configparser (status quo glue, not typed).

## R6 — Test runner & ephemeral DB

- **Decision**: pytest + pytest-postgresql (`postgresql_proc` session fixture spinning PG off host `pg_ctl`), pytest-cov for coverage. Seed dataset co-located in `tests/fixtures/seed_papers.py`.
- **Rationale**: FR-008/FR-009; constitution Principle II names pytest-postgresql as canonical and forbids mocking the persistence layer. Fresh-checkout, no `database.crypt`/`key` needed (US3). Coverage measured per layer (G1) by pointing pytest-cov at the four source subpackages.
- **Alternatives**: testcontainers-postgres (needs a container runtime; pytest-postgresql off local `pg_ctl` is lighter and matches the dev host); SQLite (not PostgreSQL — breaks parity, forbidden by Stack & Constraints).

## R7 — Linter / formatter

- **Decision**: ruff (`ruff check` + `ruff format`).
- **Rationale**: FR-010 (mainstream; if not pylint, amend constitution — already amended to ruff in v1.3.0). Replaces pylint. Fast, single-binary, lint+format in one.
- **Alternatives**: pylint (status quo, slow, being replaced); flake8+black+isort (three tools where ruff is one).

## R8 — Logging

- **Decision**: single `logging.config.dictConfig` in `logging_config.py` — RichHandler to stdout plus an **optional** FileHandler, called once from `cli/app.py`. Per-class log files dropped.
- **Rationale**: FR-013 (mainstream logging; per-class file output may remain a config option but must not be the only sink). Constitution Principle III: failures log AND surface a short plain-language message; raw exceptions never reach stdout.
- **Alternatives**: structlog (extra dep for no gain here); keeping the legacy per-class `create_logger` (the thing being removed).

## R9 — BibTeX parsing

- **Decision**: keep pybtex + pylatexenc for `.bib`/`.tex` parsing in the import service.
- **Rationale**: Spec Assumption — BibTeX parsing stays a dedicated library; switching permitted only if functionally equivalent. pybtex round-trips LaTeX accents/escapes (`\"o`, `\\&`, `{Pino}`) — an explicit edge case.
- **Alternatives**: bibtexparser (viable but no functional gain, churn risk on accent round-trip).

## R10 — Constitution amendments (FR-016 / SC-007)

- **Decision**: No `/speckit-constitution` run needed in this re-run. The live constitution (v1.3.0-b2-hardened) already encodes every amendment FR-016 demands: ruff replaces pylint (Principle I), pytest replaces unittest (Principle II), prompt routing references `paper_sorts.cli.prompts` not `helpers.get_user_input` (Principle III), and driver isolation is layer-level (`db/`) not `psycopg2`-in-`PsycopgDB` (Principle I). The three b2-hardened gates (G1 per-layer coverage, G2 executing baseline benchmark, G3 doc-currency token scan) are additional merge-blocking acceptance items folded into the task list.
- **Rationale**: The conflicting references the spec calls out do not exist in the live document, so there is nothing to silently violate. Recorded explicitly for SC-007 ("no silent deviations remain").

## R11 — Reverse-engineered architecture doc (FR-001 / US1)

- **Decision**: `docs/architecture.md` describing the **legacy** stack as it was before modernization: purpose, user journeys, the four-table data model + relationships, control flow (CLI dialog → `UserInteraction` → `DatabaseConnector` → `PsycopgDB` → DB), the encrypted-INI config approach, install/run, and known quirks (the `bibtex_id` vs `bibtext_id` schema split, duplicate-author identity collapse, rollback semantics of a mid-add failure).
- **Rationale**: FR-001 makes this the acceptance reference for the rebuild; US1 independent test requires a fresh reader to answer "what does it do / data model / where to add a field" without source. Written first, against the legacy modules, before they are deleted.

## R12 — DTO boundary (Principle I isolation)

- **Decision**: `db/repositories.py` exposes pydantic DTOs (`PaperSummary`, `PaperCreate`) consumed by services; ORM types never cross the `db/` boundary.
- **Rationale**: Principle I — "services depend on DTOs, never on ORM types" makes the ORM swappable as a single-package change and keeps `sqlalchemy` imports out of `services/` and `cli/`. Enforced structurally and checkable by grep.
- **Alternatives**: passing ORM rows up (leaks SQLAlchemy into services — violates isolation); dataclasses (pydantic gives validation + the config layer already uses pydantic).

## R13 — Legacy behaviour parity catalogue (SC-002 / US2)

Behaviours the rebuild must reproduce exactly, extracted from the legacy modules:

- **Search by title**: single match → pretty-print (title, authors joined by " and ", summary, bib entry); multiple distinct-title matches → 1-indexed disambiguation list, out-of-range re-prompts; author list rendered as `A and B and C`.
- **Search by author**: gather all papers, disambiguate if several, then resolve authors + bib for the chosen paper; absent author → "not found" message (legacy raised `KeyError` internally, surfaced as a plain message).
- **Add**: prompt author CSV → title → bibtex key → choice of inline-bib vs `.bib` file → summary; persist bib + paper + author links; bib-key uniqueness rejects duplicates; partial-failure rolls back (delete bib + any author links already made).
- **Update**: choose table (papers/bib/authors) → column → entry id → new value → confirm (numeric `1`/`2` **or** word `y`/`n`/`yes`/`no`); abort at any step writes nothing; `_id` columns refused.
- **Delete**: locate paper by title, delete author links (removing orphan authors), delete paper row, delete bib row.
- **Empty input**: any required prompt re-prompts until non-empty (legacy `get_user_input`).
- **Bulk import**: `.tex` + `.bib` → per-paper commit; citation key with no `.bib` match skipped with a logged warning; rerun skips already-imported keys (bib-key uniqueness).
