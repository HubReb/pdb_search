# Phase 0 Research: Modernize the Stack

**Feature**: 001-modernize-stack
**Date**: 2026-04-26

This document records framework-choice decisions and the constitution-amendment text that the plan depends on. Each block uses the format **Decision / Rationale / Alternatives considered**, plus a **Constraint check** noting which constitution principle (v1.1.0) the choice intersects.

---

## R1. ORM and database driver

**Decision**: SQLAlchemy 2.x (sync, typed `Mapped[]` API) with `psycopg` v3 as the driver (`postgresql+psycopg://` URL scheme).

**Rationale**:

- SQLAlchemy is the dominant Python ORM/SQL toolkit; new contributors recognise it without explanation.
- The 2.x typed API (`Mapped[str]`, `mapped_column(...)`) integrates with type checkers and matches the constitution's type-hint requirement.
- Sync API matches the constitution's "no async drivers" rule (Principle IV).
- `psycopg` v3 is the actively-maintained successor to psycopg2; the existing `pyproject.toml` already pins it (alongside psycopg2). Modernization keeps psycopg v3 and drops psycopg2.

**Alternatives considered**:

- *SQLModel* — Pydantic-flavoured wrapper over SQLAlchemy. Adds a layer that does not save code in this project's size; the constitution's "preserve layered architecture" point favours fewer layers, not more.
- *Peewee* — simpler API, smaller community, less ecosystem. Not "mainstream" by 2026 standards.
- *Raw SQL via psycopg v3 only* — would partially modernize (drop psycopg2, drop bespoke `PsycopgDB` wrapper) but not satisfy spec FR-004 ("ORM/database toolkit … replacing hand-written SQL").

**Constraint check**: Triggers Principle I amendment (driver-isolation rule references `psycopg2`). The amendment rewrites the rule at the layer level: SQLAlchemy session and engine isolated to `paper_sorts/db/`.

---

## R2. Schema migrations

**Decision**: Alembic, standard layout at repo root (`migrations/env.py`, `migrations/versions/*.py`).

**Rationale**:

- Alembic is the SQLAlchemy-native migration tool; using anything else against SQLAlchemy is non-mainstream.
- Replaces the runtime `DatabaseConnector.create_tables()` call (idempotent `CREATE TABLE IF NOT EXISTS`) with versioned, reversible migrations (spec FR-005).
- Carries the legacy-schema migration (FR-011, US4): a `002_legacy_bibtext_to_bibtex.py` revision detects the historical `bibtext_id` column (sic) and renames it to `bibtex_id` if found, idempotently.

**Alternatives considered**:

- *yoyo-migrations* — language-agnostic, but unusual to combine with SQLAlchemy.
- *Hand-written migration scripts* — defeats the modernization purpose.

**Constraint check**: Principle I amendment also touches this — replaces "create_tables()" wording with "Alembic upgrade head."

---

## R3. CLI framework + interactive prompts

**Decision**: Typer for subcommand structure; `rich.prompt` (`Prompt.ask`, `Confirm.ask`, `IntPrompt.ask`) and `rich.console` for interactive dialogs and pretty output. A thin wrapper `paper_sorts/cli/prompts.py` enforces the constitution's prompt grammar (1-indexed menus, mandatory abort/quit option, empty-input re-prompt, dual `1`/`y`/`yes` confirmations).

**Rationale**:

- Typer is type-annotation driven and built on Click; recognisable to anyone who has used either.
- The Typer subcommand surface is `pdbsearch search` / `add` / `update` / `delete` / `import` / `migrate`. The interactive top-level menu (when `pdbsearch` is run with no subcommand) preserves the **original four entries verbatim**: search / add / update / quit. Delete, import, and migrate are reachable only as subcommands — adding them to the menu would be a UX-surface expansion that spec FR-002 ("preserve existing CLI feature set") does not authorise. See `contracts/cli-commands.md` § "Why only four options" for the rationale.
- `rich` is the de-facto Python pretty-output and prompt library. `rich.prompt` covers every interactive primitive the existing app uses.
- The wrapper module is small (one file) and makes the UX-consistency principle testable: a unit test can assert that an empty input re-prompts, that `0` on a 1-indexed menu re-prompts, that `n` and `2` both abort confirmations.

**Alternatives considered**:

- *Click directly* — fine; Typer is just a more modern surface over Click. Picking Typer reduces the boilerplate around argument types.
- *prompt_toolkit* — powerful (full TUI) but heavyweight. Not needed for menu-and-prompt UX. Would also push toward a TUI, which spec FR-017 explicitly bans.
- *argparse + bare `input()`* — what the codebase has today. Modernization spec rules it out.

**Constraint check**: Triggers Principle III amendment — references to `helpers.get_user_input()` / `helpers.get_user_choice()` are replaced by `paper_sorts.cli.prompts`. Grammar rules (1-indexed menus, mandatory abort, empty-input re-prompt, dual confirmation) carry forward verbatim.

---

## R4. Configuration

**Decision**: pydantic-settings v2. Sources, in priority order: explicit constructor args → env vars → `.env` file → Fernet-encrypted INI file (custom source).

**Rationale**:

- pydantic-settings is the mainstream answer in 2026 for typed, layered configuration in Python.
- Custom sources are a documented pydantic-settings extension point; the existing Fernet+INI path becomes one such source, satisfying spec FR-007 (encrypted config preserved).
- Validation is automatic (database URL parsing, password presence) — replaces the bespoke `ConfigReader.has_section` check.
- Adds `.env` and env-var support that the current codebase lacks, simplifying CI and Docker workflows.

**Alternatives considered**:

- *python-dotenv alone* — covers env vars but no validation, no encrypted source.
- *Dynaconf* — feature-rich but not as widely adopted.
- *Custom dict-based config* (current approach) — what we are leaving behind.

**Constraint check**: No principle amendment needed. The personal/offline scope from the Stack & Constraints section continues to hold; encrypted credentials remain a supported source.

---

## R5. Test framework + ephemeral test database

**Decision**: pytest as the test runner; pytest-postgresql for ephemeral test databases; pytest-cov for coverage. Test seed data lives under `tests/fixtures/` and is referenced explicitly from each integration test.

**Rationale**:

- pytest is universal; `unittest` is fine but not "mainstream" in the same sense — almost no new Python projects start on bare unittest in 2026.
- pytest-postgresql spins up an isolated Postgres instance per test session (or per test, configurable) using the host's `pg_ctl` binary, then drops it. No Docker required, fast.
- Co-locating seed data with the tests that use it satisfies constitution Principle II's rule that "any integration test that asserts on specific seeded rows MUST reference … the seed data."

**Alternatives considered**:

- *testcontainers-python with a Postgres container* — equally mainstream; requires a running Docker daemon, which is a heavier dependency for CI/dev. Ruled out for the same reason the constitution rules out connection pooling: extra moving parts that don't pay for themselves at this scale.
- *Mocking the persistence layer* — explicitly forbidden by Principle II ("Mocking psycopg or PsycopgDB in these tests is forbidden — the value of the test is verifying the emitted SQL").
- *SQLite in-memory* — non-equivalent SQL dialect; would mask issues that only show in Postgres.

**Constraint check**: Triggers Principle II amendment — "Tests run via `python -m unittest discover tests`" becomes "Tests run via `pytest`." The "no mocking" rule is unchanged but updated to reference the SQLAlchemy session rather than `PsycopgDB`.

---

## R6. Lint, format, type-check

**Decision**: ruff for both lint and format (`ruff check` + `ruff format`). mypy for static type checking (additive, not replacing ruff). Drop pylint, drop bare-formatter ad-hoc style.

**Rationale**:

- ruff has displaced pylint+flake8+isort+black in mainstream Python projects since ~2024 — single tool, much faster, configurable.
- `ruff format` is a black-compatible formatter, removing the need for a separate formatter dependency.
- mypy is the mainstream static type checker; pairing it with constitution Principle I's "type hints on all public surfaces" gives mechanical enforcement instead of review-time enforcement.
- Both ruff and mypy are configured via `pyproject.toml`, so configuration is centralised.

**Alternatives considered**:

- *Keep pylint* — explicitly listed in spec FR-010 as the thing to replace.
- *flake8 + black + isort* — the pre-ruff stack; effectively superseded.
- *pyright instead of mypy* — equally mainstream, but mypy integrates more cleanly with SQLAlchemy 2.x's type plugin (`sqlalchemy.ext.mypy.plugin`).

**Constraint check**: Triggers Principle I amendment (replace `pylint` with `ruff`). mypy is a pure addition that the existing principle text accommodates ("type hints on parameters and return values" — mypy verifies they are correct, not just present).

---

## R7. Logging

**Decision**: stdlib `logging` configured once at startup via a `dictConfig` in `paper_sorts/logging_config.py`. Per-class file logging is no longer special-cased; sinks (file, stdout, both) are driven by the configuration.

**Rationale**:

- stdlib logging is mainstream by definition; no library is needed.
- The current pattern of every class running `helpers.create_logger("its_own_log_file.log", "its_own_logger_name", DEBUG)` produces six log files for one app run and ties the log filename to a class instance. Centralised dict-config is the standard way to fix this.
- `rich.logging.RichHandler` can render console output prettily without additional dependency since `rich` is already in the stack.

**Alternatives considered**:

- *structlog* — popular for structured/JSON logging; overkill for a single-user CLI.
- *loguru* — popular, single-line setup; non-stdlib and less neutral than the dictConfig approach.

**Constraint check**: No principle amendment required. Spec FR-013 explicitly permits keeping per-class file output as a configuration option but not as the only mechanism.

---

## R8. BibTeX / LaTeX parsing

**Decision**: Keep `pybtex` for BibTeX parsing (already in use, FR-002 requires preserved feature parity). Keep `pylatexenc` for LaTeX-to-text in the `.tex` overview parser. No replacement.

**Rationale**:

- Both libraries are still maintained and mainstream-ish for their specific niches.
- Replacing them is risk without benefit: `import_service.py` is the only consumer.

**Alternatives considered**:

- *bibtexparser* — alternative BibTeX library; no decisive advantage.

**Constraint check**: None.

---

## R9. Packaging and Python version

**Decision**: Poetry (already in use), raised `requires-python = ">=3.11"`. Move source under `src/paper_sorts/` (PEP 517/518 src-layout). Console script `pdbsearch = "paper_sorts.cli.app:main"` registered in `[tool.poetry.scripts]`.

**Rationale**:

- Poetry is mainstream; no reason to switch tooling at the same time as the rest of the modernization (would make the diff harder to review).
- 3.11 is the minimum that gives full `Self`-type and `tomllib` support and that SQLAlchemy 2.x targets cleanly.
- src-layout is the recommended Python packaging layout post-2020 — prevents accidental imports from the working directory and matches what new contributors expect.
- A registered console script replaces `python paper_sorts/run.py` with `pdbsearch`, fixing the README's incorrect path.

**Alternatives considered**:

- *uv* / *hatch* — both rising; switching at the same time as the framework change would conflate two changes. Defer to a future spec if desired.
- *Stay 3.10* — possible, but raises edge-case maintenance cost on libraries that are already 3.11+. Spec FR-015 explicitly permits raising the minimum.

**Constraint check**: None.

---

## R10. Constitution amendment text (v1.1.0 → v1.2.0)

The four amendments below are committed as the **first** implementation step (a single `/speckit-constitution` invocation) before any framework-bearing code is added. Bump rationale: MINOR — testable predicates redefined; no principle removed; no rule materially loosened beyond renaming.

### Principle I — Code Quality

Replace:

> All code under `paper_sorts/` MUST pass `pylint paper_sorts` without new warnings before merge.
> Database driver isolation MUST be preserved: only `paper_sorts/psycopg_db.py` may import `psycopg2`. Domain code (notably `DatabaseConnector`) MUST go through `PsycopgDB`. Replacing the driver MUST be a single-file change.
> The legacy procedural modules (`paper_sorts/add.py`, `paper_sorts/search.py`, `paper_sorts/get_data.py`) MUST NOT be extended. New functionality goes through the OO stack: `UserInteraction → DatabaseConnector → PsycopgDB`. Reconciling or removing the legacy modules is permitted; growing them is not.

With:

> All code under `src/paper_sorts/` MUST pass `ruff check` and `ruff format --check` without new warnings before merge. Disabling a check requires an inline justification.
> Persistence-layer isolation MUST be preserved: only modules under `src/paper_sorts/db/` may import `sqlalchemy` or any database driver (`psycopg`, etc.). Domain code under `src/paper_sorts/services/` interacts with the database only via the repository classes exposed by `db/`. Replacing the driver or the ORM MUST be a single-package change.

(The legacy-modules clause is removed — the modules no longer exist after this work.)

### Principle II — Testing Standards

Replace:

> Tests live under `tests/` and run via `python -m unittest discover tests`.
> Tests covering `DatabaseConnector` MUST be integration tests against a real PostgreSQL instance. Mocking `psycopg2` or `PsycopgDB` in these tests is forbidden — the value of the test is verifying the emitted SQL, which a mock erases.

With:

> Tests live under `tests/` and run via `pytest`. Test discovery follows pytest's defaults (`tests/test_*.py`).
> Tests covering the persistence layer (repositories, migrations) MUST be integration tests against a real PostgreSQL instance, provisioned ephemerally per test session by `pytest-postgresql`. Mocking the SQLAlchemy session, the repositories, or the database driver in these tests is forbidden — the value of the test is verifying the emitted SQL, which a mock erases.

(All other rules in Principle II carry forward unchanged.)

### Principle III — User Experience Consistency

Replace:

> All user-facing prompts MUST route through `helpers.get_user_input()` or `helpers.get_user_choice()`. Bare `input()` calls anywhere in `paper_sorts/` outside of `helpers.py` are a violation.

With:

> All user-facing prompts MUST route through `paper_sorts.cli.prompts`. Bare `input()`, bare `rich.prompt.Prompt.ask`, or bare `typer.prompt` calls anywhere in `src/paper_sorts/` outside of `cli/prompts.py` are a violation.

(Grammar rules — 1-indexed numeric menus, mandatory abort/quit option, empty-input re-prompt, dual `1`/`y`/`yes` confirmations, no raw exceptions on stdout — carry forward verbatim.)

### Principle IV — Performance Requirements

Replace:

> Connections opened in `PsycopgDB` MUST be closed in a `finally` block; long-lived connections are not a permitted optimisation.
> Search paths (`search_by_title`, `search_by_author`) MUST use parameterised queries and JOINs over the existing four-table schema. …
> Bulk import paths (`DatabaseConnector.add_data_from_dict`, `get_data.load_data_into_db`) MAY exceed the interactive baseline …

With:

> Database sessions opened by the persistence layer MUST be closed deterministically (context-managed `with Session(...)` or equivalent). Long-lived sessions are not a permitted optimisation.
> Search paths (the repository methods backing `pdbsearch search`) MUST use parameterised queries and joins over the existing four-table schema. …
> Bulk import paths (the import service backing `pdbsearch import`) MAY exceed the interactive baseline …

(The non-regression-vs.-baseline criterion from v1.1.0 carries forward unchanged.)

---

## Summary of resolved unknowns

No `[NEEDS CLARIFICATION]` markers remained in `plan.md` after Technical Context was filled — every choice above was made by informed selection from the mainstream Python ecosystem. The single clarification in the spec (FR-017, deployment surface) was resolved before plan time as CLI-only.
