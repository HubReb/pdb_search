# Phase 0 Research: Modernize the Stack

All Technical-Context unknowns are resolved here. Each decision records what was
chosen, why, and the alternatives rejected.

## R1 — ORM / database toolkit (FR-004)

**Decision**: SQLAlchemy 2.x (declarative `Mapped[...]` models + Core for the
migration converge step), on the `psycopg` v3 binary driver.

**Rationale**: SQLAlchemy is *the* mainstream Python ORM; a 2026 contributor
recognises it immediately. The 2.x typed `Mapped[...]` API gives us full type
hints (Principle I) and works cleanly with mypy strict. The constitution
already names "SQLAlchemy 2.x" and "`psycopg` v3 (binary)" in Stack &
Constraints. Parameterised queries, joins, and transactions — all FR-004
requirements — are first-class. Driver imports stay inside `db/` (Principle I).

**Alternatives rejected**: raw `psycopg` (what we're replacing — no ORM, hand
SQL); Django ORM (drags in a web framework, violates CLI-only); Peewee /
SQLModel (smaller mindshare; SQLModel re-exports SQLAlchemy anyway but pins us
to pydantic-in-the-ORM, blurring the DTO boundary).

## R2 — Migrations (FR-005, FR-011)

**Decision**: Alembic, two revisions. **001** is a verbatim port of the legacy
DDL (the canonical `bibtex_id` schema, including the FK `papers.bibtex_id →
bib.bibtex_id` and the `bib.bibtex` UNIQUE). **002** converges legacy variants:
if a `bibtext_id` (typo) column exists on `papers`/`bib`, rename it to
`bibtex_id`; if `bib.bibtext` exists, rename to `bibtex`. Guarded by
`information_schema` checks so it is idempotent and a no-op on an
already-canonical database.

**Rationale**: Alembic is the mainstream companion to SQLAlchemy and the tool
the constitution names (`migrations/versions/`). Splitting "create canonical"
from "converge legacy typo" keeps revision 001 a clean fresh-install path and
isolates the historical-data concern in 002. Idempotency (FR-011, AC-3) is met
by conditioning every DDL statement on a catalog probe and wrapping the upgrade
in Alembic's transactional DDL (PostgreSQL supports transactional DDL, so a
mid-migration failure rolls the whole revision back — never half-migrated).

**Alternatives rejected**: hand-written SQL migration scripts (not versioned/
reversible — FR-005 violation); `yoyo`/`sqitch` (less mainstream in the
SQLAlchemy world); a single mega-revision (couples fresh-install DDL to
legacy-typo handling, harder to reason about idempotency).

### Schema-preservation contract (binding)

Revision 001 reproduces the *exact* legacy DDL. It MUST NOT tighten it:
- **No NOT NULL** outside primary keys. Legacy `papers.title/contents/bibtex_id`,
  `bib.bibtex`, `authors_id.author` are all nullable — keep them nullable.
- **No FK on `authors_papers`**. The link table has `author_id`/`paper_id` as
  plain `INT` with no DDL foreign keys. Do not add them.
- **No new indexes** beyond the original primary keys and the `bib.bibtex`
  UNIQUE. ORM `relationship()` navigation does not require extra DDL indexes.
- The only declared FK is `papers.bibtex_id → bib.bibtex_id` (legacy
  `fk_bibtex_id`).

## R3 — CLI framework (FR-006)

**Decision**: Typer. One `typer.Typer()` app in `cli/app.py` registers
subcommands `search`, `add`, `update`, `delete`, `migrate`, `import`. Invoked
with **no** subcommand, a `@app.callback(invoke_without_command=True)` drops
into the legacy four-option top-level menu (Search / Add / Update / Quit) to
preserve the existing interactive UX (FR-002, FR-003). `migrate` and `import`
are subcommand-only (admin/scripted), deliberately absent from the four-option
menu.

**Rationale**: Typer is the mainstream modern CLI framework (Click-based, type-
hint-driven), recognised on sight. Subcommands map cleanly to the legacy menu
options (FR-006 SHOULD). The callback preserves the interactive dialog for
users who just run `pdbsearch`.

**Alternatives rejected**: bare `argparse` (what we're replacing); Click
directly (Typer wraps it with less boilerplate and native type hints);
`cleo`/`fire` (smaller mindshare).

## R4 — Prompt routing & UX grammar (FR-003, Principle III)

**Decision**: A single module `cli/prompts.py` is the *only* place under
`src/paper_sorts/` permitted to import `rich.prompt`. It exposes:
- `ask_nonempty(prompt) -> str` — re-prompts on empty input (legacy
  `get_user_input` behaviour; edge case "press Enter").
- `ask_choice(prompt, options) -> str` — 1-indexed numbered menu, always
  includes an explicit abort/quit option, re-prompts on out-of-range.
- `ask_confirm(summary) -> bool` — destructive-op confirmation accepting both
  numeric (`1`/`2`) and word (`y`/`n`/`yes`/`no`) forms.
- `pick_from(prompt, rows) -> T` — disambiguation when search returns multiple
  same-title papers (legacy `get_user_choice`); out-of-range re-prompts.
- `display_paper(...)` — the legacy "pretty print" (title / authors / summary /
  bib entry).

**Rationale**: Principle III makes `cli/prompts.py` the single prompt-routing
seam. Centralising the grammar (1-indexed, mandatory abort, empty re-prompt,
dual-form confirm) lets every menu inherit it and keeps bare `input()` /
`rich.prompt` out of the rest of the tree. These are pure helpers → unit-tested
for empty/malformed/success (Principle II).

**Alternatives rejected**: scattering `typer.prompt`/`input()` at call sites
(Principle III violation); `questionary`/`InquirerPy` (extra dep; rich is
already present via Typer).

## R5 — Configuration (FR-007, Principle Stack)

**Decision**: `config.py` exposes a pydantic-settings v2 `Settings` model with a
**four-source priority chain** (highest first): CLI flags (`--database-url`,
`--log-level`) → environment variables (`PDBSEARCH_*`) → `.env` file →
Fernet-encrypted INI file (a custom `PydanticBaseSettingsSource`). The encrypted
source decrypts `--config <path>` with `--key <path>` (Fernet) and reads the
`[postgresql]` section, exactly as the legacy `ConfigReader`/`read_config` did,
then maps `host/port/dbname/user/password` to a SQLAlchemy URL.

**Rationale**: pydantic-settings is the mainstream settings library and is named
in the constitution. The custom encrypted source keeps the legacy Fernet
workflow as *one supported source* (FR-007) while adding `.env`/env-var support
alongside it. Lost-key → a clear, actionable error (edge case), never a
stack trace.

**Alternatives rejected**: `python-decouple`/`dynaconf` (less type-safe, less
mainstream in 2026); keeping `ConfigReader` only (fails FR-007's `.env`/env
requirement).

## R6 — Test runner & ephemeral DB (FR-008, FR-009, Principle II)

**Decision**: `pytest` + `pytest-postgresql`. `conftest.py` defines a
session-scoped `postgresql_proc` (off the host `pg_ctl`) and an
`ephemeral_db_url`; a function- or module-scoped fixture creates the schema (via
the ORM `metadata.create_all` or by running the Alembic migrations) and seeds
`tests/fixtures/seed_papers.SEED_PAPERS`. Persistence tests use the real
session — no mocking. `pytest-cov` measures coverage (SC-008 ≥ 80% on the
persistence layer).

**Rationale**: Exactly the constitution's named mechanism. Fresh-checkout,
no developer-local DB (FR-008, SC-003). Seed data co-located with assertions
(Principle II).

**Alternatives rejected**: `unittest` (what we're replacing; FR-009);
testcontainers (heavier — needs Docker; `pytest-postgresql` off host `pg_ctl`
is lighter and is what the constitution names); mocking psycopg (forbidden by
Principle II).

## R7 — Lint / format (FR-010, Principle I)

**Decision**: `ruff` for both `ruff check` and `ruff format`. Replaces `pylint`.

**Rationale**: ruff is the mainstream 2026 linter/formatter; fast, single-tool.
FR-010 requires that if the tool is not pylint the constitution's Code Quality
principle reference it — Principle I already says `ruff` (v1.3.0), so the
Core-Principles side is done; the stale **Development Workflow** gate that still
says `pylint paper_sorts` is fixed in T001 (see R10).

**Alternatives rejected**: keeping pylint (slow, being phased out); black+flake8
(two tools where ruff is one); leaving the workflow section stale (FR-016
violation).

## R8 — Logging (FR-013)

**Decision**: A single `logging.config.dictConfig` in `logging_config.py`,
called once from `cli/app.py` at startup. Default sink: `rich.logging.RichHandler`
to stdout. Optional `FileHandler` enabled by config (`--log-file` / setting).
Per-class log files (`db_connector.log`, `interaction.log`) are **not** the only
sink anymore (FR-013) — structured stdlib logging with configurable sinks
supersedes them.

**Rationale**: Mainstream stdlib approach. Keeps the per-class file option as a
*configuration option* (FR-013 MAY) without making it mandatory. Failure paths
log technical detail here while the CLI surfaces a short plain-language message
(Principle III).

**Alternatives rejected**: bespoke `create_logger` per class (legacy; couples
file naming to class names); structlog/loguru (extra deps for no gain on a
single-user tool).

## R9 — DTO boundary (FR-014, Principle I)

**Decision**: pydantic v2 DTOs live in `db/repositories.py`: `PaperSummary`
(read model returned by searches — title, authors-joined string, summary,
bibtex key, bibtex source) and `PaperCreate` (write model — title, summary,
authors list, bibtex key, bibtex source). Services consume/produce DTOs and
never import ORM types; repositories translate ORM ↔ DTO.

**Rationale**: This is the seam that keeps `services/` free of `sqlalchemy`
(Principle I, FR-014). A future ORM swap is a single-package change.

**Alternatives rejected**: passing ORM objects up to services (leaks driver
types across layers); plain dataclasses (lose validation; pydantic is already a
dep via pydantic-settings).

## R10 — Constitution amendment (FR-016, SC-007)

**Decision**: Amend the **Development Workflow & Quality Gates** section (only)
via `/speckit-constitution`, bumping **1.3.0 → 1.3.1 (PATCH)**. Three stale
bullets are corrected to match the already-modern Core Principles:
- `pylint paper_sorts` + `unittest suite` → `ruff check` / `ruff format --check`
  + the `pytest` suite (against ephemeral `pytest-postgresql`).
- "live development database is available" caveat → removed (tests are now
  fresh-checkout, no local DB).
- `DatabaseConnector.create_tables()` schema-update bullet → "Alembic
  migrations under `migrations/versions/` plus affected fixtures/assertions".

**Rationale**: PATCH because no *principle* changes meaning — the Core
Principles (I–IV) already encode ruff/pytest/SQLAlchemy/cli-prompts since
v1.3.0; only a stale workflow section lagged. FR-016 and SC-007 require the
conflicting references be amended, not silently violated. The Sync Impact Report
at the top of `constitution.md` is updated in the same edit.

**Alternatives rejected**: MINOR/MAJOR bump (overstates the change — no
principle is added or redefined); silently writing ruff/pytest code while the
workflow gate still says pylint/unittest (FR-016 violation, SC-007 fail).

## R11 — Behaviour-preservation map (FR-002, SC-002)

Legacy → modern, to guarantee no observable regression:

| Legacy | Modern |
|--------|--------|
| `UserInteraction.interact` 4-option menu | `cli/app.py` callback menu (Search/Add/Update/Quit) |
| `UserInteraction.search` (author/title sub-menu) | `cli/search.py` + `services.search_by_author/title` |
| `get_user_choice` disambiguation | `prompts.pick_from` |
| `pretty_print_results` | `prompts.display_paper` |
| `DatabaseConnector.add_entry_to_db` | `services.add_paper` → `PaperRepository.add` |
| `add` (inline or `.bib` file) | `cli/add.py` (inline or `--bib-file`) |
| `update_entry` match/case over table | `services.update_field` match/case over `Literal[...]` + `assert_never` |
| `delete_paper_entry_from_database` | `services.delete_paper` → repos |
| `get_data` + `get_bibtex_information` + `load_data_into_db` | `import_service.extract_papers_from_tex_bib` + per-paper commit |
| `get_user_input` empty re-prompt | `prompts.ask_nonempty` |
| `match_proceed_with_change` dual-form confirm | `prompts.ask_confirm` |

Searchable fields, the disambiguation-on-duplicate-title flow, the
authors-joined-with-" and " display string, and per-paper bulk-import commit are
all preserved.

## R12 — `update_field` exhaustiveness (Principle I)

**Decision**: `services.update_field` takes `table: Literal["papers", "bib",
"authors_id"]` and uses `match`/`case` with a final `case _ as unreachable:
assert_never(unreachable)` so mypy proves exhaustiveness at compile time.
Attempting `authors_papers` (the link table) or an unknown table is rejected
before reaching the persistence layer, mirroring legacy `update_entry`'s guard
("Table authors_papers has no changeable column").

**Rationale**: `assert_never` turns the legacy runtime `ValueError` table guard
into a compile-time guarantee for the supported set, satisfying Principle I's
"statically analysable" mandate while keeping the runtime rejection for the
forbidden link table.
