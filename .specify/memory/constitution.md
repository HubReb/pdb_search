<!--
SYNC IMPACT REPORT
==================
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

- All code under `paper_sorts/` MUST pass `pylint paper_sorts` without new
  warnings before merge. Disabling a check requires an inline justification.
- Every public function, method, and class MUST carry full type hints on
  parameters and return values, and a docstring that accurately describes
  behaviour, parameters, return values, and raised exceptions. Out-of-date
  docstrings are bugs and MUST be corrected in the same change that breaks
  them — not deferred.
- Database driver isolation MUST be preserved: only `paper_sorts/psycopg_db.py`
  may import `psycopg2`. Domain code (notably `DatabaseConnector`) MUST go
  through `PsycopgDB`. Replacing the driver MUST be a single-file change.
- The legacy procedural modules (`paper_sorts/add.py`,
  `paper_sorts/search.py`, `paper_sorts/get_data.py`) MUST NOT be extended.
  New functionality goes through the OO stack:
  `UserInteraction → DatabaseConnector → PsycopgDB`. Reconciling or removing
  the legacy modules is permitted; growing them is not.

**Rationale**: Recent commit history is dominated by pylint, type-hint, and
docstring fixes — these are an active concern, not aspirational. The
driver-isolation rule is what makes the psycopg2-vs-psycopg confusion
tractable; without it, swap costs leak across the codebase.

### II. Testing Standards

Tests are the only mechanism that protects SQL correctness in this project,
since the SQL is hand-written strings. They MUST be honest about what they
exercise.

- Tests live under `tests/` and run via `python -m unittest discover tests`.
- Tests covering `DatabaseConnector` MUST be integration tests against a real
  PostgreSQL instance. Mocking `psycopg2` or `PsycopgDB` in these tests is
  forbidden — the value of the test is verifying the emitted SQL, which a
  mock erases.
- Any integration test that asserts on specific seeded rows (titles, BibTeX
  keys, author names) MUST reference, in a comment or fixture file, the seed
  data that produces those rows. Hidden coupling to a developer-local
  database is a defect.
- Pure helpers in `paper_sorts/helpers.py` and `paper_sorts/config_reader.py`
  SHOULD have unit tests covering at least: empty input, malformed input,
  and the documented success path.
- Placeholder tests that intentionally fail (`assertEqual(True, False)`) MUST
  NOT remain in the suite. Either implement the test, omit the file, or skip
  it with `@unittest.skip("<reason>")` and a documented reason.
- Schema changes MUST update `DatabaseConnector.create_tables()` and any
  affected fixtures or test assertions in the same change.

**Rationale**: The current state — one integration test silently dependent
on a live DB plus one always-failing placeholder — is a trap for any new
contributor. These rules turn the implicit setup into explicit setup.

### III. User Experience Consistency

The CLI is the entire product surface. Prompt, menu, and error patterns MUST
be uniform so users can build correct expectations after seeing one screen.

- All user-facing prompts MUST route through `helpers.get_user_input()` or
  `helpers.get_user_choice()`. Bare `input()` calls anywhere in
  `paper_sorts/` outside of `helpers.py` are a violation.
- Numbered menus MUST be 1-indexed in display, parsed via `helpers.cast()`,
  and MUST always include an explicit abort/quit option (e.g.
  `4) (Q)uit`, `3) abort`). Menus without an exit are a violation.
- Destructive operations (update, delete) MUST present a confirmation step
  that summarises the exact change before it is applied. Confirmation MUST
  accept both numeric (`1`/`2`) and word (`y`/`n`/`yes`/`no`) forms,
  matching the pattern in `UserInteraction.match_proceed_with_change`.
- Failure paths MUST log via `helpers.create_logger()` AND surface a short,
  plain-language message to the user. Raw exceptions, stack traces, or
  psycopg error objects MUST NOT reach stdout.

**Rationale**: This is a single-user offline tool used in low-attention
contexts (the README cites "traveling by train"). Predictability of the
prompt grammar is the only thing that prevents misclicks on destructive
operations.

### IV. Performance Requirements

Performance constraints exist to bound scope, not to optimise. The target
profile is a single user, a local PostgreSQL instance, and a personal
library-sized dataset.

- All interactive operations (search by title, search by author, add a
  single paper, update a single field, delete a single paper) MUST complete
  in under 1 second on a database of up to 10 000 papers on commodity
  hardware. This is the user-perceived latency budget.
- High-throughput, multi-user, or network-tier optimisations are explicitly
  OUT OF SCOPE. Connection pooling, async drivers, caching layers, and
  read replicas MUST NOT be introduced. Connections opened in `PsycopgDB`
  MUST be closed in a `finally` block; long-lived connections are not a
  permitted optimisation.
- Search paths (`search_by_title`, `search_by_author`) MUST use parameterised
  queries and JOINs over the existing four-table schema. Introducing a new
  table, denormalisation, or an index beyond the existing primary keys
  requires an entry in the plan's Complexity Tracking section explaining
  why the 1-second budget cannot otherwise be met.
- Bulk import paths (`DatabaseConnector.add_data_from_dict`,
  `get_data.load_data_into_db`) MAY exceed the interactive budget but MUST
  commit per-paper, so a partial failure leaves the database in a
  consistent state recoverable on rerun.

**Rationale**: The genuine risk in this codebase is *not* throughput — it
is leaked connections, partial-write inconsistency, and speculative
complexity added under a "performance" banner. Naming the budget
explicitly (1 s, 10 k papers, single user) shuts down those rationalisations.

## Stack & Constraints

- Language: Python ^3.10, dependencies managed by Poetry.
- Database: PostgreSQL only. Driver is `psycopg2` (binary). The newer
  `psycopg` (v3) imports that exist in the legacy modules are technical debt,
  not an alternative supported driver.
- Configuration: Database credentials live in a Fernet-encrypted INI file
  read by `ConfigReader`. Plaintext credentials, decryption keys, and
  encrypted config files MUST NOT be committed to the repository, and MUST
  NOT be written to logs.
- Scope: personal, offline, single-user. Multi-tenant, network-exposed,
  authentication, authorisation, and concurrent-user concerns are out of
  scope and MUST NOT be added without a constitution amendment.

## Development Workflow & Quality Gates

- Every change MUST pass `pylint paper_sorts` and the unittest suite (where
  the live development database is available) before being merged.
- Every change that modifies a function signature MUST update the
  corresponding type hints and docstring in the same commit. Reviewers MUST
  reject changes that desynchronise these.
- Plans generated via `/speckit-plan` MUST include a Constitution Check
  section that lists which of the four principles the feature touches and
  flags any required waivers. Waivers MUST appear in the plan's Complexity
  Tracking table with a concrete justification.
- Schema changes (new table, renamed column, changed FK) MUST update
  `DatabaseConnector.create_tables()`, any affected test fixtures, and any
  assertions referencing column names — all in the same change.

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

**Version**: 1.0.0 | **Ratified**: 2026-04-26 | **Last Amended**: 2026-04-26
