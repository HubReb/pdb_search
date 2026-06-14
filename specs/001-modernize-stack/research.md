# Research: Modernize the Stack

**Feature**: 001-modernize-stack | **Date**: 2026-06-14

## R1: ORM Choice — SQLAlchemy 2.x

**Decision**: SQLAlchemy 2.x with psycopg v3 (binary) as the driver.

**Rationale**: SQLAlchemy 2.x introduces the "2.0-style" API (`Session.execute(select(...))`) which is fully typed, composable, and idiomatic. It integrates naturally with Alembic for migrations, and the psycopg v3 binary adapter is already listed as a dependency in the legacy code (get_data.py, add.py). The constitution Stack & Constraints section explicitly names both.

**Alternatives considered**:
- Raw psycopg v3 (no ORM): retains hand-written SQL, does not satisfy FR-004 ("mainstream ORM/database toolkit, replacing hand-written SQL strings").
- SQLModel: wraps SQLAlchemy + pydantic but adds a second moving part and is less stable; unnecessary given we already use pydantic-settings for config.
- Django ORM: requires Django runtime; out of scope for a CLI tool.

---

## R2: CLI Framework — Typer

**Decision**: Typer (built on Click), with Rich for output formatting.

**Rationale**: Typer provides subcommand routing, type-annotated arguments, automatic `--help`, and integrates with Rich for pretty output. It is the mainstream Python CLI framework in 2026. The constitution names it implicitly via "mainstream CLI framework". The existing user-interaction patterns (menus, confirmations, re-prompting) are easily expressed as Typer commands + `prompts.py` helpers.

**Alternatives considered**:
- Click directly: Typer is Click with type annotations — Typer is strictly more ergonomic; no reason to use raw Click.
- argparse: already in use in legacy; does not satisfy FR-006 ("mainstream CLI framework").
- Textual: TUI, not CLI; FR-017 forbids non-CLI surfaces without a separate constitution amendment.

---

## R3: Configuration — pydantic-settings v2

**Decision**: `pydantic-settings` v2 `BaseSettings` with a custom `FernetIniSettingsSource`.

**Rationale**: pydantic-settings v2 natively reads from environment variables (prefixed `PDBSEARCH_`), `.env` files, and programmatic overrides. A custom `PydanticBaseSettingsSource` subclass can decrypt the Fernet-encrypted INI file on demand, satisfying FR-007. Priority order: CLI flags > env vars > .env > encrypted INI.

**Alternatives considered**:
- `dynaconf`: heavyweight, adds unnecessary complexity for a personal CLI tool.
- `python-dotenv` alone: does not provide type validation or the four-source priority chain.
- Keeping ConfigReader as-is: violates FR-007 (must add env-var support) and FR-016 (must not silently violate constitution).

---

## R4: Test Runner — pytest + pytest-postgresql

**Decision**: pytest with pytest-postgresql for ephemeral PostgreSQL provisioning.

**Rationale**: pytest-postgresql spins up a real PostgreSQL process per test session using the host's `pg_ctl`, seeding and tearing it down automatically. This satisfies FR-008, FR-009, and constitution Principle II ("no mocking SQLAlchemy session"). The host `pg_ctl` is confirmed at `/usr/bin/pg_ctl` (PostgreSQL 18).

**Alternatives considered**:
- `pytest-docker`: requires Docker daemon; heavier setup, blocks offline use.
- Mocking the session: explicitly forbidden by constitution Principle II.
- `testcontainers-python`: also requires Docker.

---

## R5: Linter/Formatter — ruff

**Decision**: ruff (lint + format) + mypy (strict type checking).

**Rationale**: ruff is the mainstream Python linter/formatter in 2026, already named in constitution v1.3.0 replacing pylint (FR-010, FR-016). mypy strict mode enforces the type hints required by constitution Principle I.

**Alternatives considered**:
- pylint: legacy; already replaced in constitution v1.3.0.
- flake8 + black + isort: three tools; ruff subsumes all of them.

---

## R6: Migration Tool — Alembic

**Decision**: Alembic with `env.py` wired to SQLAlchemy metadata + psycopg v3.

**Rationale**: Alembic is the standard migration tool for SQLAlchemy. It supports reversible, versioned migrations (FR-005) and idempotent `--autogenerate` or hand-written scripts. Two revisions are needed: 001 (verbatim DDL port) and 002 (converge `bibtext_id` typo schema).

**Alternatives considered**:
- Flyway/Liquibase: JVM-based, incompatible with Python ecosystem.
- Hand-written SQL scripts: not a mainstream migration tool (FR-005).
- Single revision: cannot handle both historical schema variants idempotently.

---

## R7: BibTeX Parsing — pybtex (retained)

**Decision**: Retain pybtex for BibTeX parsing. Retain pylatexenc for LaTeX-to-text conversion.

**Rationale**: Both libraries are already present, working, and maintain correct round-trip of LaTeX accents/escapes (edge case from spec). The spec permits "switching to an alternative library if functionally equivalent" but provides no reason to switch.

**Alternatives considered**:
- `bibtexparser` v2: newer but still less mature for the round-trip accent case.

---

## R8: Project Packaging — uv + hatchling + PEP 621

**Decision**: uv as package manager (replaces poetry), hatchling as build backend, PEP 621 `[project]` metadata in `pyproject.toml`.

**Rationale**: Constitution v1.3.0 Stack & Constraints explicitly names uv + PEP 621 + hatchling. The entry point is declared as `pdbsearch = "paper_sorts.cli.app:app"`.

**Alternatives considered**:
- Poetry: legacy; replaced in constitution v1.3.0.
- setuptools: older build backend; hatchling is the constitution's choice.

---

## R9: Architecture Document (US1)

**Decision**: Write `docs/architecture.md` as the first deliverable (FR-001), covering purpose, user journeys, data model, control flow, configuration, install/run, and known limitations.

**Rationale**: FR-001 is the acceptance reference for FR-002 through FR-006. It must be produced from the legacy code before any modernization begins (or in parallel with the first tasks) so that regressions can be detected.

---

## R10: Constitution Amendments

**Decision**: No further amendments required. Constitution v1.3.0 (amended 2026-04-27) already incorporates:
- Principle I: ruff replaces pylint; isolation rule rewritten to layer-level.
- Principle II: pytest + pytest-postgresql; no mocking SQLAlchemy session.
- Principle III: prompt routing moved to `paper_sorts.cli.prompts`.
- Stack & Constraints: Python >= 3.11; uv; psycopg v3; pydantic-settings.

FR-016 is therefore satisfied by the existing constitution state.
