# Phase 0 Research: Modernize the Stack

All Technical-Context unknowns are resolved here. The spec deliberately defers
concrete framework picks to this phase ("mainstream framework" = widely adopted,
actively maintained, recognisable to a 2026 Python developer). The constitution
(v1.3.0-b2-hardened) already *names* most of the target stack in its principle
bodies and Stack & Constraints section, so most "decisions" below are
confirmations that the named choice satisfies the requirement, plus the
integration patterns.

## R1 — ORM / database toolkit (FR-004, Principle I/IV)

- **Decision**: SQLAlchemy 2.x (typed `Mapped[...]` declarative models) over
  the `psycopg` v3 driver (binary extra).
- **Rationale**: Constitution Stack & Constraints names exactly this. SQLAlchemy
  2.x is the de-facto mainstream Python ORM; the 2.0 typed API gives full type
  hints (Principle I) and parameterised queries + joins for free (Principle IV).
  psycopg v3 replaces the legacy psycopg2.
- **Alternatives**: raw psycopg v3 (loses ORM/relationship modelling, keeps
  hand-written SQL the spec wants gone); Django ORM (drags in a web framework,
  out of scope); Tortoise/async (Principle IV bans async drivers).
- **Isolation**: only `db/` imports `sqlalchemy`/`psycopg`. Services consume
  pydantic DTOs returned by repositories, never ORM instances.

## R2 — Migration tool (FR-005, FR-011)

- **Decision**: Alembic, two revisions.
  - `001_initial_schema` — verbatim port of the original DDL (the canonical
    `bibtex_id` schema).
  - `002_converge_legacy` — idempotent convergence that renames the legacy
    `bibtext_id` (sic) columns on `papers`/`bib` to `bibtex_id` **only if** the
    legacy column exists, using PostgreSQL catalog checks so a fresh/canonical
    DB is a no-op.
- **Rationale**: Alembic is the mainstream companion to SQLAlchemy. Idempotency
  (FR-011, Edge: rerun) is met by guarding each step on `information_schema`
  inspection, so rerunning either completes cleanly or is a no-op — never
  half-migrated.
- **Migration command flow** (`pdbsearch migrate`): inspect current columns →
  if legacy `bibtext_id` present, `alembic upgrade head` applies 002's rename;
  if already canonical, ensure schema at head; row counts (papers, authors,
  authorships, bib) are unchanged by a rename, satisfying SC-004 zero-loss.
- **Alternatives**: hand-rolled `ALTER` script (the bespoke surface the spec
  removes); `yoyo`/`migra` (less mainstream than Alembic in the SQLAlchemy
  ecosystem).

## R3 — CLI framework (FR-006)

- **Decision**: Typer, one `typer.Typer()` app in `cli/app.py` with subcommands
  `search`, `add`, `update`, `delete`, `import`, `migrate`. Invoked with **no**
  subcommand, `app.py` drops into the legacy four-option top-level menu
  (Search / Add / Update / Quit) to preserve the interactive experience.
- **Rationale**: Typer is the mainstream modern CLI framework (Click-based,
  type-hint-driven). Subcommands map cleanly to the legacy top-level menu
  (FR-006 SHOULD). `migrate` and `import` are subcommand-only admin/scripted
  operations, deliberately absent from the four-option interactive menu.
- **Alternatives**: argparse (the bespoke thing being replaced); click directly
  (Typer wraps it more ergonomically with type hints); cleo (less mainstream).

## R4 — Prompt routing & UX (Principle III, FR-003)

- **Decision**: `cli/prompts.py` is the single module permitted to import
  `rich.prompt`. It exposes `ask_text` (non-empty re-prompt = legacy
  `get_user_input`), `ask_choice` (1-indexed menu with mandatory abort, returns
  selection or abort sentinel), and `confirm` (accepts `1`/`2`/`y`/`n`/`yes`/`no`).
- **Rationale**: Principle III requires all prompts to route through this module;
  legacy `get_user_input` re-prompts on empty input, `get_user_choice` is a
  1-indexed numbered list, and `match_proceed_with_change` accepts numeric+word
  confirmation — all carried forward verbatim into these three helpers.
- **Error UX**: services raise typed domain errors; the CLI catches them, logs
  full detail via the stdlib logger, and prints a short plain-language message.
  No stack trace or driver error object reaches stdout (Principle III, FR-006
  acceptance #6).

## R5 — Configuration (FR-007, Stack & Constraints)

- **Decision**: `config.py` defines a pydantic-settings v2 `Settings` model with
  a four-source priority chain, highest first:
  1. CLI flags (`--database-url`, `--log-level`, `--config`, `--key`)
  2. environment variables (`PDBSEARCH_*`)
  3. `.env` file
  4. Fernet-encrypted INI file (custom `PydanticBaseSettingsSource`)
- **Rationale**: pydantic-settings is the mainstream settings library with
  built-in env + `.env` support. The legacy `ConfigReader` (Fernet-decrypted
  INI) is preserved as the lowest-priority custom source so existing encrypted
  configs keep working (FR-007). The DB URL is assembled into a SQLAlchemy URL
  (`postgresql+psycopg://...`).
- **Lost-key UX** (Edge case): a missing key file or decrypt failure raises a
  clear, actionable `ConfigError`, surfaced as a plain message — not a traceback.
- **Alternatives**: dynaconf (heavier, less type-first); bare `os.environ`
  (loses validation + the layered chain).

## R6 — Logging (FR-013, Principle III)

- **Decision**: `logging_config.py` with a single `logging.config.dictConfig` —
  a `RichHandler` to stdout (default) plus an **optional** `FileHandler` enabled
  by config. Called once from `cli/app.py` at startup.
- **Rationale**: Mainstream stdlib logging. Per-class log files
  (`db_connector.log`, `interaction.log`, etc.) are replaced by configurable
  sinks; file output remains available but is no longer the only sink
  (FR-013).

## R7 — Lint / format (FR-010, Principle I)

- **Decision**: ruff (`ruff check` + `ruff format`).
- **Rationale**: ruff is the mainstream 2026 linter/formatter, already named in
  Principle I. FR-010 requires amending the constitution's Code Quality
  principle if the tool isn't pylint — Principle I's body already says ruff; the
  remaining stale `pylint paper_sorts` reference lives in the *workflow* section
  and is amended via `/speckit-constitution` (see plan FR-016 amendments).

## R8 — Test framework & ephemeral DB (FR-008, FR-009, Principle II, G1)

- **Decision**: pytest + pytest-postgresql; pytest-cov for per-layer coverage.
  - `conftest.py`: `postgresql_proc` (session) spins up PG from host `pg_ctl`;
    `ephemeral_db_url` yields a fresh URL; a `seeded_session` fixture runs
    Alembic to head and loads `tests/fixtures/seed_papers.SEED_PAPERS`.
  - Persistence tests hit the real DB (no mocking session/repos/driver).
  - **G1**: coverage is asserted per layer — `db/`, `services/`, `cli/`,
    `config.py` each ≥80%. The interface layer is covered by `test_cli.py`
    driving every subcommand through Typer's `CliRunner`.
- **Rationale**: pytest-postgresql is the mainstream way to get a real ephemeral
  PG with no developer-local state (FR-008, SC-003). Real-DB tests verify the
  emitted SQL, which mocks would erase (Principle II).
- **Seed fingerprints carried from legacy tests**: `Pino, J.` /
  `Wang2021LargeScaleSA` / "Large-scale Self- an[d] Semi-Supervised learning for
  speech translation"; the multi-author "Direct speech-to-speech translation
  with discrete units" row. The seed dataset reproduces these so historical
  assertions remain meaningful and co-located (Principle II).

## R9 — Baseline benchmark (G2, Principle IV, SC-006)

- **Decision**: `tests/benchmarks/bench_baseline.py` — an **executing** harness
  (a pytest test, not a permanent skip) that times the five interactive ops
  (search-by-title, search-by-author, add, update, delete) against the seeded
  ephemeral DB and writes/asserts `tests/benchmarks/baseline.json`.
- **Rationale**: G2 is explicit that "no measurable regression" may not be
  claimed vacuously; the harness must exist and execute. It runs as part of the
  default suite (fast on a personal-library dataset) and records the baseline so
  the non-regression criterion is verifiable rather than asserted.
- **Note**: Because legacy code is removed in the same change set (FR-012), the
  "baseline" is the modern implementation's own measured timing recorded on
  first run; the harness guards against *future* regression. This is the honest
  reading of Principle IV in a worktree where the legacy stack no longer exists
  to time side-by-side.

## R10 — BibTeX / LaTeX parsing (FR-002 import, Assumptions)

- **Decision**: keep pybtex for BibTeX parse + round-trip; keep pylatexenc for
  `.tex`→text extraction in the bulk-import path.
- **Rationale**: Assumptions permit retaining pybtex (and switching only if
  functionally equivalent). They already handle the LaTeX-accent round-trip edge
  case. `import_service.extract_papers_from_tex_bib(tex, bib)` yields
  `PaperCreate` DTOs one at a time so the CLI can commit per-paper (FR per-paper
  commit, US5 acceptance #3).

## R11 — Schema preservation (Principle IV, schema contract)

- **Decision**: models declare exactly the legacy four tables with their
  original column nullability and keys — `papers(id PK, title, contents,
  bibtex_id FK→bib.bibtex_id)`, `bib(bibtex_id PK, bibtex UNIQUE)`,
  `authors_id(id PK, author)`, `authors_papers(id PK, author_id, paper_id)` with
  **no** DDL FKs on `authors_papers`.
- **Rationale**: schema-preservation contract — do not add NOT NULL outside PKs,
  do not add FKs to `authors_papers`, do not add indexes the original DDL
  lacked. The many-to-many link is modelled in the ORM relationship layer, not
  via new DDL constraints, to keep Revision 001 a verbatim port.

## R12 — FR-016 constitution amendment mechanics (SC-007)

- **Decision**: run `/speckit-constitution` to amend the two stale legacy
  references in the **Development Workflow & Quality Gates** section
  (`pylint paper_sorts`/unittest → ruff+pytest; `create_tables()` →
  Alembic migrations). PATCH bump.
- **Rationale**: The principle *bodies* (I–IV) already describe the modern
  stack; only the older workflow section lags. FR-016/SC-007 require these be
  amended via the constitution tool, not silently violated. Keeping it PATCH
  reflects wording-alignment, not a semantic principle change.
