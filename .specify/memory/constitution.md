<!--
SYNC IMPACT REPORT
==================
Version change: 1.3.0 → 1.3.1
Bump rationale: PATCH — aligns the stale "Development Workflow & Quality
Gates" section with the already-ratified v1.3.0 Core Principles. No principle
changes meaning. Three workflow bullets that still referenced the legacy stack
are corrected: `pylint paper_sorts` + the `unittest` suite (with its
"live development database" caveat) → `ruff check`/`ruff format --check` +
the `pytest` suite against ephemeral `pytest-postgresql`; the
`DatabaseConnector.create_tables()` schema-update bullet → Alembic migrations
under `migrations/versions/`. Required by spec 001-modernize-stack FR-016 /
SC-007 (amend conflicts, never silently violate). The Core Principles
(I Code Quality → ruff; II Testing → pytest/pytest-postgresql; III UX →
cli/prompts; IV Performance) and Stack & Constraints already encoded the
modern stack at v1.3.0; only this workflow section lagged.

Modified sections:
  Development Workflow & Quality Gates — pylint→ruff, unittest→pytest,
    create_tables()→Alembic migrations.

Templates / docs reviewed for propagation:
  ✅ specs/001-modernize-stack/plan.md — Constitution Check flags this
    amendment as T001 (FR-016).
  ✅ specs/001-modernize-stack/research.md § R10 — carries the amendment text.

Deferred / TODO: None.

---

Version change: 1.1.0 → 1.3.0
Bump rationale: MINOR — five testable predicates redefined to layer/role
names; uv replaces Poetry; psycopg v3 replaces psycopg2; Python ≥ 3.11.
No principle removed. The version jump 1.1.0 → 1.3.0 reflects two
cumulative MINOR-level amendment groups (originally targeted at v1.2.0,
plus the Stack & Constraints amendment added before drafting); both
land in this single application to keep "what changed when" honest.

Modified principles:
  I. Code Quality — driver-isolation rule rewritten to layer-level;
    pylint → ruff; "frozen legacy modules" clause removed (modules
    deleted in the same change set per FR-012).
  II. Testing Standards — unittest → pytest; pytest-postgresql named as
    the canonical ephemeral-DB mechanism; "no mocking persistence
    layer" rule rephrased to reference the SQLAlchemy session.
  III. UX Consistency — prompt-routing reference moved from
    helpers.get_user_input to paper_sorts.cli.prompts; grammar rules
    (1-indexed menus, mandatory abort, empty-input re-prompt, dual
    confirmations) carry forward verbatim.
  IV. Performance — function-level references (PsycopgDB,
    search_by_title, add_data_from_dict, etc.) replaced with
    layer-level references; non-regression criterion (v1.1.0) carried
    forward unchanged.

Modified sections:
  Stack & Constraints — Python ^3.10 → ≥ 3.11; Poetry → uv (PEP 621
    metadata, uv.lock, hatchling build backend); psycopg2 → psycopg v3;
    ConfigReader (deleted in T026) → paper_sorts.config (pydantic-settings)
    with four-source priority order documented.

Templates / docs reviewed for propagation:
  ✅ specs/001-modernize-stack/* — drafted against v1.3.0 text;
    research.md § R10 carries the verbatim amendment text; plan.md
    Constitution Check table reflects all five amendments.
  ✅ CLAUDE.md — references v1.3.0; uv command examples updated.

Deferred / TODO: None.

---

Version change: 1.0.0 → 1.1.0
Bump rationale: MINOR — Principle IV (Performance Requirements) is materially
redefined. The "1 s interactive / 10 000 papers" budget inserted at
ratification was not grounded in measurement; it is replaced by a
non-regression-vs.-current-baseline criterion on a personal-library-sized
dataset. No principle is removed; the testable predicate changes from an
absolute bound to a relative bound.

Modified principles:
  IV. Performance Requirements — interactive-latency rule rewritten;
  bulk-import bullet wording updated to match ("interactive baseline");
  rationale rewritten to drop fabricated numerics.

Sections: unchanged.
Templates / docs reviewed for propagation:
  ✅ specs/001-modernize-stack/spec.md SC-006 — already updated in same
     change to match (non-regression criterion).
  ✅ CLAUDE.md — quoted the now-removed "1 s / ≤10 k" numerics in its
     SpecKit section; updated to the non-regression phrasing.
  ✅ .specify/templates/* — no embedded numerics; no edit needed.

Deferred / TODO: None.

---

Version change: (uninitialised template) → 1.0.0
Bump rationale: Initial ratification — the prior file was a 100% unfilled
template with no semantic content. First concrete fill is treated as 1.0.0.

Principles (template placeholder → concrete):
  [PRINCIPLE_1_NAME] → I. Code Quality (NON-NEGOTIABLE)
  [PRINCIPLE_2_NAME] → II. Testing Standards
  [PRINCIPLE_3_NAME] → III. User Experience Consistency
  [PRINCIPLE_4_NAME] → IV. Performance Requirements
  [PRINCIPLE_5_NAME] → REMOVED (user requested 4 principles)

Sections:
  [SECTION_2_NAME] → Stack & Constraints (added)
  [SECTION_3_NAME] → Development Workflow & Quality Gates (added)
  Governance → filled

Templates / docs reviewed for propagation:
  ✅ .specify/templates/plan-template.md — Constitution Check gate is
     reference-by-link, no principle names hard-coded; no edit needed.
  ✅ .specify/templates/spec-template.md — no constitution references; no edit.
  ✅ .specify/templates/tasks-template.md — no constitution references; no edit.
  ✅ .specify/templates/checklist-template.md — no constitution references; no edit.
  ⚠ CLAUDE.md — contained the line "constitution.md … is still the unfilled
     template … do not treat it as authoritative." That statement becomes stale
     with this ratification and is updated in the same change.

Deferred / TODO: None.
-->

# Paper Sorts Constitution

## Core Principles

### I. Code Quality (NON-NEGOTIABLE)

The codebase MUST remain readable, statically analysable, and self-documenting.

- All code under `src/paper_sorts/` MUST pass `ruff check` and
  `ruff format --check` without new warnings before merge. Disabling a
  check requires an inline justification.
- Every public function, method, and class MUST carry full type hints on
  parameters and return values, and a docstring that accurately describes
  behaviour, parameters, return values, and raised exceptions. Out-of-date
  docstrings are bugs and MUST be corrected in the same change that breaks
  them — not deferred.
- Persistence-layer isolation MUST be preserved: only modules under
  `src/paper_sorts/db/` may import `sqlalchemy` or any database driver
  (`psycopg`, etc.). Domain code under `src/paper_sorts/services/`
  interacts with the database only via the repository classes exposed by
  `db/`. Replacing the driver or the ORM MUST be a single-package change.

**Rationale**: Type hints and docstrings are an active concern in commit
history — the rule reflects practice, not aspiration. The persistence-layer
isolation rule is what makes future driver or ORM swaps tractable; without
it, swap costs leak across the codebase (this is exactly what happened with
the original psycopg2/psycopg v3 divergence in the now-deleted legacy
procedural modules).

### II. Testing Standards

Tests are the only mechanism that protects SQL correctness in this project,
since the SQL is hand-written strings. They MUST be honest about what they
exercise.

- Tests live under `tests/` and run via `pytest`. Test discovery follows
  pytest's defaults (`tests/test_*.py`).
- Tests covering the persistence layer (repositories, migrations) MUST
  be integration tests against a real PostgreSQL instance, provisioned
  ephemerally per test session by `pytest-postgresql`. Mocking the
  SQLAlchemy session, the repositories, or the database driver in these
  tests is forbidden — the value of the test is verifying the emitted
  SQL, which a mock erases.
- Any integration test that asserts on specific seeded rows (titles, BibTeX
  keys, author names) MUST reference, in a comment or fixture file, the seed
  data that produces those rows. Hidden coupling to a developer-local
  database is a defect.
- Pure helpers (e.g. `src/paper_sorts/cli/prompts.py`,
  `src/paper_sorts/config.py`) SHOULD have unit tests covering at least:
  empty input, malformed input, and the documented success path.
- Placeholder tests that intentionally fail (`assertEqual(True, False)`) MUST
  NOT remain in the suite. Either implement the test, omit the file, or skip
  it with `@pytest.mark.skip(reason="<reason>")` and a documented reason.
- Schema changes MUST land as Alembic migrations under `migrations/versions/`
  and MUST update any affected fixtures or test assertions in the same change.

**Rationale**: The current state — one integration test silently dependent
on a live DB plus one always-failing placeholder — is a trap for any new
contributor. These rules turn the implicit setup into explicit setup.

### III. User Experience Consistency

The CLI is the entire product surface. Prompt, menu, and error patterns MUST
be uniform so users can build correct expectations after seeing one screen.

- All user-facing prompts MUST route through `paper_sorts.cli.prompts`.
  Bare `input()`, bare `rich.prompt.Prompt.ask`, or bare `typer.prompt`
  calls anywhere in `src/paper_sorts/` outside of `cli/prompts.py` are a
  violation.
- Numbered menus MUST be 1-indexed in display and MUST always include an
  explicit abort/quit option (e.g. `4) (Q)uit`, `3) abort`). Menus
  without an exit are a violation.
- Destructive operations (update, delete) MUST present a confirmation
  step that summarises the exact change before it is applied.
  Confirmation MUST accept both numeric (`1`/`2`) and word
  (`y`/`n`/`yes`/`no`) forms.
- Failure paths MUST log via the configured stdlib logger AND surface a
  short, plain-language message to the user. Raw exceptions, stack
  traces, or driver error objects MUST NOT reach stdout.

**Rationale**: This is a single-user offline tool used in low-attention
contexts (the README cites "traveling by train"). Predictability of the
prompt grammar is the only thing that prevents misclicks on destructive
operations.

### IV. Performance Requirements

Performance constraints exist to bound scope, not to optimise. The target
profile is a single user, a local PostgreSQL instance, and a
personal-library-sized dataset (the current corpus is the reference).

- All interactive operations (search by title, search by author, add a
  single paper, update a single field, delete a single paper) MUST show
  no measurable regression versus the current implementation on equivalent
  operations, measured on the same data with wall-clock timing on
  commodity hardware.
- High-throughput, multi-user, or network-tier optimisations are explicitly
  OUT OF SCOPE. Connection pooling beyond SQLAlchemy's default, async
  drivers, caching layers, and read replicas MUST NOT be introduced.
  Database sessions opened by the persistence layer MUST be closed
  deterministically (context-managed `with Session(...)` or equivalent).
  Long-lived sessions are not a permitted optimisation.
- Search paths (the repository methods backing `pdbsearch search`) MUST
  use parameterised queries and joins over the existing four-table
  schema. Introducing a new table, denormalisation, or an index beyond
  the existing primary keys requires an entry in the plan's Complexity
  Tracking section explaining why baseline-parity cannot otherwise be met.
- Bulk import paths (the import service backing `pdbsearch import`) MAY
  exceed the interactive baseline but MUST commit per-paper, so a partial
  failure leaves the database in a consistent state recoverable on rerun.

**Rationale**: The genuine risk in this codebase is *not* throughput — it
is leaked connections, partial-write inconsistency, and speculative
complexity added under a "performance" banner. The criterion is framed as
"non-regression vs. the current baseline on a personal-library-sized
dataset" rather than as fabricated absolute numbers, because the current
implementation has not been benchmarked and any specific bound would be a
guess. Refactors are evaluated against measured baseline, not against a
fictional target.

## Stack & Constraints

- Language: Python >= 3.11, dependencies managed by uv (PEP 621
  `[project]` metadata, `uv.lock` for reproducibility, `hatchling` build
  backend).
- Database: PostgreSQL only. Driver is `psycopg` v3 (binary).
  SQLAlchemy 2.x sits on top of the driver and is isolated to the
  persistence layer per Principle I.
- Configuration: Loaded by `paper_sorts.config` (pydantic-settings v2)
  from four sources in priority order — CLI args > environment
  variables (`PDBSEARCH_*`) > `.env` file > Fernet-encrypted INI file
  (custom pydantic-settings source). Plaintext credentials, decryption
  keys, and encrypted config files MUST NOT be committed to the
  repository, and MUST NOT be written to logs.
- Scope: personal, offline, single-user. Multi-tenant, network-exposed,
  authentication, authorisation, and concurrent-user concerns are out of
  scope and MUST NOT be added without a constitution amendment.

## Development Workflow & Quality Gates

- Every change MUST pass `ruff check` and `ruff format --check` on
  `src/paper_sorts/` and the `pytest` suite (run against the ephemeral
  PostgreSQL provisioned by `pytest-postgresql`, requiring no developer-local
  database) before being merged.
- Every change that modifies a function signature MUST update the
  corresponding type hints and docstring in the same commit. Reviewers MUST
  reject changes that desynchronise these.
- Plans generated via `/speckit-plan` MUST include a Constitution Check
  section that lists which of the four principles the feature touches and
  flags any required waivers. Waivers MUST appear in the plan's Complexity
  Tracking table with a concrete justification.
- Schema changes (new table, renamed column, changed FK) MUST land as Alembic
  migrations under `migrations/versions/` and MUST update any affected test
  fixtures and any assertions referencing column names — all in the same
  change.

## Governance

- This constitution supersedes ad-hoc preferences expressed elsewhere
  (CLAUDE.md, README, commit history). Where they conflict, this document
  wins, and the conflicting source MUST be updated.
- Amendments are performed via `/speckit-constitution`. Version bumps
  follow semantic versioning:
  - **MAJOR**: a principle is removed, or redefined in a backward-incompatible
    way (e.g. relaxing a NON-NEGOTIABLE rule).
  - **MINOR**: a new principle or governance section is added, or existing
    guidance is materially expanded.
  - **PATCH**: clarifications, wording, typo fixes, non-semantic refinements.
- All PRs MUST be reviewable against the live constitution. Deviations MUST
  be explicit (in the plan's Complexity Tracking table) — silent deviations
  are defects.
- The constitution is a living document. If a principle proves wrong in
  practice (false positives in review, blocking legitimate work), amend it
  rather than ignore it.

**Version**: 1.3.1 | **Ratified**: 2026-04-26 | **Last Amended**: 2026-06-15
