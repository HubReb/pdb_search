# Phase 0 Research: Modernize the Stack

All Technical Context unknowns are resolved below. Framework picks were deferred to plan by the spec; the constitution body (v1.3.0-b2-hardened Stack & Constraints) already fixes most of them, so research here is confirmation + rationale rather than open exploration.

## R1 — ORM / database toolkit (FR-004)

**Decision**: SQLAlchemy 2.x (Core + ORM, `Mapped`/`mapped_column` declarative style), driver psycopg v3 (binary), `create_engine("postgresql+psycopg://…")`.
**Rationale**: Mandated by the constitution. Supports parameterised queries, joins, transactions. The 2.x typed-ORM style gives mypy-checkable models. psycopg v3 is the modern driver; the legacy procedural modules already used it, so it is the natural converge target.
**Alternatives considered**: SQLModel (thin SQLAlchemy wrapper — adds a dependency without buying anything here); raw psycopg (would re-create the hand-SQL problem FR-004 removes); Django ORM (web-framework weight, out of scope).

## R2 — CLI framework (FR-006)

**Decision**: Typer (built on Click), with a Rich console for output. Subcommands `search/add/update/delete/import/migrate`. Bare `pdbsearch` invocation drops into a four-option interactive menu (Search / Add / Update / Quit) mirroring the legacy `UserInteraction.interact` loop.
**Rationale**: Mandated. Typer maps subcommands cleanly to the legacy top-level menu (FR-006). `CliRunner` makes the interface layer testable end-to-end (Principle II G1). Rich gives the "pretty print" parity the spec requires.
**Alternatives considered**: argparse (the thing being replaced); plain Click (Typer adds type-hint-driven ergonomics for free); cleo/fire (less mainstream in 2026).

## R3 — Migration tool (FR-005, FR-011)

**Decision**: Alembic. Revision 0001 is a verbatim port of the original DDL (canonical `bibtex_id` column, `bib.bibtex UNIQUE`, FK `papers.bibtex_id → bib.bibtex_id`, **no FKs on `authors_papers`**). Revision 0002 converges a legacy database that has the `bibtext_id` (sic) typo column onto the canonical schema, idempotently.
**Rationale**: Mandated; replaces lazy `create_tables()`. Idempotency (US4 AS3) achieved by inspecting the live schema before acting (rename column only if `bibtext_id` present and `bibtex_id` absent) so a rerun is a no-op and a fresh DB is unaffected.
**Schema-preservation contract**: do not add NOT NULL outside PKs, do not add FKs to `authors_papers`, do not add indexes the original DDL lacked. The original `papers.bibtex_id` is nullable, `bib.bibtex` is UNIQUE, `authors_papers` is a bare link table.

## R4 — Configuration (FR-007)

**Decision**: pydantic-settings v2 `BaseSettings` (`Settings`) with four sources in priority order: CLI flags (`--database-url`, `--log-level`, `--config`, `--key`) > env (`PDBSEARCH_*`) > `.env` > Fernet-encrypted INI (custom `PydanticBaseSettingsSource`). The encrypted source decrypts the INI with the key file and maps `[postgresql] dbname/user/password/host/port` into a `database_url`.
**Rationale**: Mandated; preserves the Fernet workflow (US4 edge case: missing key → clear actionable error, not a stack trace) while adding `.env`/env support. Plaintext creds/keys never logged or committed.
**Alternatives considered**: dynaconf (heavier, less type-safe); hand-rolled (FR-007 wants a mainstream settings library); environ-config (less adopted than pydantic-settings).

## R5 — Test runner & ephemeral DB (FR-008, FR-009)

**Decision**: pytest + pytest-postgresql. `postgresql_proc` session fixture spins an ephemeral cluster from the host `pg_ctl`; an `ephemeral_db_url` fixture yields a `postgresql+psycopg://` URL; a `seeded_db` fixture runs Alembic to head and loads `SEED_PAPERS`. No developer-local `database.crypt`/`key`.
**Rationale**: Mandated. Real-DB integration (Principle II) — mocking the session/repos/driver is forbidden. Seed data co-located in `tests/fixtures/seed_papers.py` so row assertions are reviewable (Principle II).
**Alternatives considered**: testcontainers (needs a container runtime; pytest-postgresql off host `pg_ctl` is lighter and is the constitution's named mechanism); SQLite in-memory (not PostgreSQL; would not exercise the real SQL — forbidden by the testing principle).

## R6 — Lint/format (FR-010)

**Decision**: ruff (`ruff check` + `ruff format`). The constitution Code Quality principle already references ruff, so FR-010's "if not pylint, amend the constitution" is pre-satisfied in the principle text; the stale `pylint paper_sorts` line in *Development Workflow & Quality Gates* is reconciled in R10.
**Rationale**: Mandated; one tool for lint+format, fast, mainstream in 2026.

## R7 — Logging (FR-013)

**Decision**: stdlib `logging` configured once via `logging.config.dictConfig` in `logging_config.py`, called from `cli/app.py` at startup. RichHandler to stdout (level from config); optional FileHandler when a log-file path is configured. Per-class `*.log` files are dropped (spec Assumptions permit this).
**Rationale**: Mandated "mainstream logging"; file sink retained as an option (FR-013) but no longer the only sink. Tracebacks go to the log; stdout gets plain-language messages (Principle III).

## R8 — BibTeX / LaTeX parsing

**Decision**: keep pybtex (`parse_file`/`parse_string`, `entries[k].to_string("bibtex")`, `persons["author"]` → `Last, First`) and pylatexenc (`LatexNodes2Text`) for the bulk-import `.tex` walk. Encapsulated in `import_service.py`.
**Rationale**: Spec Assumptions keep BibTeX parsing a library responsibility; pybtex round-trips LaTeX accents (`\"o`, `\&`, `{Pino}`) — edge case in the spec. No functional reason to switch.

## R9 — Packaging (Stack & Constraints)

**Decision**: uv with PEP 621 `[project]` metadata in `pyproject.toml`, `uv.lock` for reproducibility, hatchling build backend. Console script `pdbsearch = "paper_sorts.cli.app:app"`. Dev extras (`--all-extras`) carry pytest, pytest-postgresql, pytest-cov, ruff, mypy.
**Rationale**: Mandated. Replaces Poetry/^3.10.

## R10 — Constitution amendment scope (FR-016, SC-007)

**What is already modern in the constitution body** (no change needed): Principle I (ruff, `src/paper_sorts/db/` isolation, doc-currency G3), Principle II (pytest, pytest-postgresql, per-layer G1, no placeholder), Principle III (`cli.prompts` routing), Principle IV (session isolation, G2 benchmark), Stack & Constraints (Python ≥ 3.11, uv, psycopg v3, pydantic-settings).

**What is stale and MUST be amended** via `/speckit-constitution`: the *Development Workflow & Quality Gates* section still says "MUST pass `pylint paper_sorts` and the unittest suite" and "Schema changes MUST update `DatabaseConnector.create_tables()`". These reference the deleted legacy stack and contradict Principles I/II. Amendment (PATCH/MINOR): replace with "MUST pass `ruff check`, `ruff format --check`, `mypy src`, and the `pytest` suite" and "Schema changes MUST land as Alembic migrations under `migrations/versions/` and update affected models/fixtures". No principle is removed or relaxed, so this is a PATCH-level wording fix landing alongside the code.

**Decision**: perform this amendment in the Polish phase (after the modern stack exists), bumping the constitution and recording the rationale in its sync-impact header, so SC-007 ("no silent deviations remain in the final state") holds at merge.

## R11 — Behaviour-preservation catalogue (US2, FR-002)

Mapping legacy → modern, to guarantee no observable regression:

| Legacy | Modern home | Notes |
|--------|-------------|-------|
| `UserInteraction.interact` four-option menu | `cli/app.py` bare-invocation menu | `1) Search 2) Add 3) Update 4) (Q)uit` |
| `UserInteraction.search` (author/title sub-menu) | `cli/search.py` + `services.paper_service.search_by_*` | disambiguation on duplicate titles preserved |
| `DatabaseConnector.search_by_title/author` (joins) | `PaperRepository.search_by_title/author` | parameterised joins over four tables |
| `pretty_print_results` | Rich rendering in `cli/search.py` | title/authors/summary/bib parity |
| `UserInteraction.add` (inline or `.bib` file) | `cli/add.py` + `services.add_paper` | both input paths |
| `add_entry_to_db` multi-step + rollback | `PaperRepository.add_paper` in one `with_session` transaction | DB transaction replaces manual rollback |
| `update_entry` match/case over table+column | `services.update_field` `match`/`case` on `Literal[...]` table + `assert_never` | compile-time exhaustiveness |
| `delete_paper_entry_from_database` | `services.delete_paper` + repo | cascade authors with no remaining papers |
| `get_data.py` tex+bib bulk import | `services.import_service.extract_papers_from_tex_bib` + `cli/importer.py` | per-paper commit (Principle IV, US5 AS3) |
| `get_user_input` empty re-prompt | `cli/prompts.py` ask helpers | non-empty re-prompt preserved |
| `cast` (safe int, -1 sentinel) | folded into prompt parsing | no module-level constant |

## R12 — DTO boundary (Principle I)

**Decision**: pydantic DTOs `PaperSummary` (author string, id, title, bibtex_id, contents) and `PaperCreate` (title, contents, bibtex_id, bibtex, authors list) defined in `db/repositories.py` and imported by services. ORM model instances never leave `db/`.
**Rationale**: enforces "services depend on DTOs, never on ORM types" so an ORM swap is a single-package change (Principle I rationale). `PaperSummary` mirrors the legacy result tuple shape so CLI rendering parity is mechanical.
