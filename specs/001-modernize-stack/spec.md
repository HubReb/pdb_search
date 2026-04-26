# Feature Specification: Modernize the Stack

**Feature Branch**: `001-modernize-stack`
**Created**: 2026-04-26
**Status**: Draft
**Input**: User description: "Reverse engineer this code base. Modernize it to main stream frameworks"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Reverse-Engineered Architecture Documentation (Priority: P1)

A maintainer or new contributor needs to understand what the application does, how its components interact, and how data flows — without reading source. After this story is delivered, a single architecture document captures: purpose, user journeys, data model, control flow (CLI dialog → domain layer → DB layer), configuration approach, install/run instructions, and the system's known limitations and quirks.

**Why this priority**: Modernization without a written baseline is risky — drift between "what we have" and "what we think we have" hides regressions during refactoring. This document is also the acceptance reference for User Story 2 (the rebuilt system must do everything the document describes).

**Independent Test**: Hand the document to a Python developer who has never seen the project. They must be able to answer "What does it do? What is the data model? Where would I add a new field?" without opening any source file.

**Acceptance Scenarios**:

1. **Given** a fresh reader with Python experience, **When** they read the architecture document end-to-end, **Then** they can describe each of the four database tables and the relationships between them.
2. **Given** the architecture document and the running CLI, **When** the reader picks one user-facing operation (e.g. "search by author"), **Then** they can trace it through the document from prompt to data lookup without ambiguity.
3. **Given** the architecture document, **When** the reader asks "what happens if a partial add fails midway?", **Then** the document answers it (rollback semantics in the current `DatabaseConnector`).

---

### User Story 2 - Modernized Codebase, Same User-Facing Behavior (Priority: P1) 🎯 MVP

An end user runs the rebuilt CLI and gets the same search-by-title, search-by-author, add, update, and delete operations they had before — same prompts, same outputs, same data, against the same personal database. The internals are rebuilt on widely-used Python libraries (a mainstream ORM, a mainstream CLI framework, a mainstream test runner, a mainstream linter), so a future contributor recognises the patterns immediately and can extend the system using stock library documentation rather than reading bespoke glue code.

**Why this priority**: This is the core deliverable. A user with an existing personal database must be able to keep using their tool with no observable regression in features, while a maintainer benefits from the reduced bespoke surface area.

**Independent Test**: Run a scripted dialog through every existing CLI path (search by title with one match, search by title with multiple matches, search by author, add new entry from inline input, add new entry from `.bib` file, update title, update bibtex, abort an update at the confirmation step, delete entry, quit). Each path must produce equivalent or improved output to the current version against the same seeded data.

**Acceptance Scenarios**:

1. **Given** a database seeded with the test fixture, **When** the user searches by title for a paper that exists once, **Then** the system displays title, authors, summary, and BibTeX entry, matching the current "pretty print" format.
2. **Given** a database with two papers sharing a title, **When** the user searches by that title, **Then** the system asks the user to disambiguate by selecting from a numbered list and proceeds with the chosen paper.
3. **Given** a fresh empty database, **When** the user adds a new entry by typing author/title/key/summary and pointing at a `.bib` file, **Then** the entry is persisted and is retrievable by both author and title.
4. **Given** an existing entry, **When** the user updates its title and confirms `y`, **Then** the new title is persisted; **When** the user updates and confirms `n`, **Then** no change is written.
5. **Given** any user input that is empty (just pressing Enter), **When** the system would otherwise accept it, **Then** the prompt is repeated until non-empty input is provided (current `get_user_input` behaviour).
6. **Given** a failed database operation, **When** the user is shown the failure, **Then** the message is plain-language and the technical details are written to a log file — no raw exception or stack trace appears on stdout.

---

### User Story 3 - Reproducible Test Suite Without Developer-Local State (Priority: P2)

A contributor checks out the repo on a fresh machine and runs the test suite. Integration tests spin up an ephemeral test database (via container or fixture), seed it with a known dataset, run, and tear down. There is no dependency on a personal Postgres instance configured at `../../database.crypt` or hand-curated rows like `"Pino, J."` / `"Wang2021LargeScaleSA"`.

**Why this priority**: The current test suite silently depends on developer-local state, which means CI, new contributors, and even the original author on a new machine cannot run it without manual setup. This blocks contribution and refactoring confidence.

**Independent Test**: Clone the repo on a machine that has never had the project's personal database. Run the documented install + test commands. The suite passes.

**Acceptance Scenarios**:

1. **Given** a fresh clone with no `database.crypt` and no `key` file present anywhere, **When** the developer runs the test command, **Then** all integration tests pass against an automatically-managed test database.
2. **Given** a CI environment with only Python and a container runtime available, **When** CI runs the suite, **Then** it passes deterministically across reruns.
3. **Given** the test fixture, **When** a test asserts on specific rows, **Then** the corresponding seed data is co-located with the test so the relationship is visible at review time.

---

### User Story 4 - One-Shot Migration of Existing Personal Database (Priority: P2)

A user who already has a personal database from the current version runs a single migration command and ends up on the modernized schema, with all rows preserved, ready to use the rebuilt CLI.

**Why this priority**: The original author's existing data is the entire point of this tool. Any modernization that loses data or requires manual SQL surgery is unacceptable.

**Independent Test**: Take a snapshot of the current personal database. Run the migration command. Compare paper count, author count, and authorship link count against the snapshot — they must match exactly. Spot-check a few rows for content equality.

**Acceptance Scenarios**:

1. **Given** a database in the current schema (column `bibtex_id`), **When** the user runs the migration command, **Then** all papers, authors, BibTeX entries, and authorship links are present after migration with identical content.
2. **Given** a database in the legacy schema (column `bibtext_id`, sic, used by the older procedural modules), **When** the user runs the migration command, **Then** the same preservation guarantee holds.
3. **Given** the migration fails partway, **When** the user reruns it, **Then** the migration is idempotent and either completes cleanly or leaves the database in its pre-migration state — never half-migrated.

---

### User Story 5 - Bulk Import from LaTeX/BibTeX Preserved (Priority: P3)

A user with a `.tex` literature overview and a corresponding `.bib` file imports all entries via a single command, just as the current `get_data.py` allows.

**Why this priority**: This is how the original author bootstrapped the personal database. It is used rarely (one-shot per literature overview) but losing it would mean re-typing dozens of entries by hand.

**Independent Test**: Run the bulk-import command against a fixture pair (`literature_overview.tex` + `bib.bib`) with N entries. Verify N papers, their authors, and their BibTeX entries are present in the database afterward.

**Acceptance Scenarios**:

1. **Given** a `.tex` file with citations and a matching `.bib` file, **When** the user runs the bulk-import command, **Then** every cited entry that has a matching BibTeX record is inserted into the database.
2. **Given** a `.tex` entry whose citation key has no matching `.bib` record, **When** the import runs, **Then** the entry is skipped with a logged warning rather than failing the whole import.
3. **Given** a bulk import that partially fails, **When** the failure occurs, **Then** entries inserted before the failure are preserved (per-paper commit).

---

### Edge Cases

- User has data in either historical schema: `bibtex_id` (current `DatabaseConnector`) or `bibtext_id` (sic, legacy `add.py` / `get_data.py` / `search.py`). Migration must handle both.
- User has no existing database — fresh install creates schema via migrations from scratch.
- BibTeX entry contains LaTeX accents/escapes (`\"o`, `\\&`, `{Pino}`) — must round-trip through the new BibTeX parser without corruption when displayed.
- Encrypted config file present but key file missing (lost key) — must produce a clear, actionable error, not a stack trace.
- Search returns multiple papers with the same title — disambiguation prompt remains; selecting an out-of-range option re-prompts.
- User presses Enter on a required prompt — re-prompt until non-empty (preserve current `get_user_input` behaviour).
- User hits Ctrl+C mid-dialog — process exits without leaving the database in an inconsistent partial state.
- Two authors with identical `Last, First` strings exist — system continues to treat them as the same author (current behaviour; documented limitation).
- A bulk import is interrupted and rerun — already-imported entries are skipped, not duplicated (current behaviour via BibTeX-key uniqueness).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST produce a single architecture document covering purpose, user journeys, data model, control flow, configuration, install/run, and known limitations of the current (pre-modernization) codebase. This document is the acceptance reference for FR-002 through FR-006.
- **FR-002**: System MUST preserve the existing CLI feature set: interactive search by author, interactive search by paper title, add new entry (inline or from a `.bib` file), update an existing entry's title/contents/bibtex/author, delete an entry, and bulk import from a `.tex` + `.bib` pair.
- **FR-003**: System MUST present prompts, menus, and confirmations consistent with the project constitution: 1-indexed numeric menus, an explicit abort/quit option on every menu, plain-language error messages on stdout, technical detail in logs.
- **FR-004**: Database access MUST be implemented via a mainstream Python ORM/database toolkit, replacing hand-written SQL strings. The toolkit MUST support parameterised queries, joins, and transactions.
- **FR-005**: Schema changes MUST be expressed as versioned, reversible migrations applied through a mainstream migration tool, replacing the current runtime `create_tables()` call.
- **FR-006**: The CLI entry point MUST be implemented using a mainstream CLI framework, replacing bespoke `argparse` + manual dialog loop. Subcommands SHOULD map cleanly to the current top-level menu options (search / add / update / delete / import).
- **FR-007**: Configuration MUST be loaded via a mainstream settings library that supports environment variables and `.env` files. The existing Fernet-encrypted config workflow MUST remain available as one supported source.
- **FR-008**: Test suite MUST run on a fresh checkout with no developer-local database. Integration tests MUST manage an ephemeral test database — created, seeded, and torn down by fixtures.
- **FR-009**: Test runner MUST be a mainstream framework, replacing bare `unittest`. Test discovery MUST work via the standard convention of the chosen framework.
- **FR-010**: Linting and formatting MUST be performed by a mainstream tool. If the chosen tool is not `pylint`, the constitution's Code Quality principle MUST be amended in the same change to reference the new tool.
- **FR-011**: A migration command MUST upgrade a personal database in either historical schema (`bibtex_id` or `bibtext_id`) to the modernized schema in a single user action with zero data loss. The command MUST be idempotent.
- **FR-012**: Legacy procedural modules (`paper_sorts/add.py`, `paper_sorts/search.py`, `paper_sorts/get_data.py`) MUST be removed once their functionality is fully covered by the modernized stack.
- **FR-013**: Logging MUST use a mainstream logging approach. Per-class log file output MAY be retained as a configuration option but MUST NOT be the only supported sink.
- **FR-014**: Layered architecture MUST be preserved: a presentation layer (CLI), a domain/service layer, a persistence layer (ORM/repository), and configuration. Database driver imports MUST remain isolated to the persistence layer.
- **FR-015**: System MUST run on Python 3.11 or later (raising the current 3.10 minimum is permitted to gain mainstream-framework support).
- **FR-016**: Project constitution principles that conflict with this modernization (driver isolation rule referencing `psycopg2`, mandatory `pylint`, mandatory `unittest`, prompt routing through `helpers.get_user_input`) MUST be amended via `/speckit-constitution` as part of this work — not silently violated.
- **FR-017**: System deployment surface MUST be CLI only. Adding any non-CLI surface (web UI, REST API, TUI, GUI) requires a separate constitution amendment and a separate spec — not part of this modernization.

### Key Entities

- **Paper**: a publication record. Attributes: title, summary, BibTeX key (unique). Related to one or more Authors and to exactly one BibTeX Entry.
- **Author**: a person credited on one or more papers. Attribute: name in `"Last, First"` form. Related to zero or more Papers.
- **BibTeX Entry**: the full BibTeX source string for a Paper, keyed by BibTeX key.
- **Authorship**: the link between a Paper and an Author (many-to-many).
- **Configuration**: database connection settings (host, port, database name, user, password) plus optional credential decryption inputs (encrypted config file + key file).
- **Migration**: a versioned, idempotent change to the schema, applied in order.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A Python developer unfamiliar with the project can read the architecture document and answer the questions "What does it do? How is the data modeled? Where would I add a new field?" within 30 minutes, without opening source.
- **SC-002**: All currently working CLI flows (search by title, search by author, add inline, add from `.bib`, update each updatable field, delete, bulk import, abort dialogs, quit) pass an explicit end-to-end acceptance script run against a seeded test database. 100% of flows pass before merge.
- **SC-003**: A fresh-checkout test run completes successfully on a machine that has never had the project's personal database. `git clone && <install> && <test>` succeeds in under 5 minutes wall-clock on commodity hardware.
- **SC-004**: An existing personal database upgrades to the modernized schema via a single command with zero data loss, verified by row counts (papers, authors, authorships, BibTeX entries) matching exactly before and after.
- **SC-005**: Project-authored Python lines under `paper_sorts/` (excluding tests and migration scripts) decrease by at least 30% relative to the current ~2 000 lines, with the difference replaced by mainstream-library imports rather than feature loss.
- **SC-006**: Interactive operations (search-by-title, search-by-author, single add, single update, single delete) show no measurable regression versus the current implementation, measured on the same seeded test database with wall-clock timing on commodity hardware.
- **SC-007**: All four constitution principles are either upheld or explicitly amended via `/speckit-constitution` before merge — no silent deviations remain in the final state.
- **SC-008**: Test coverage for the new persistence layer (CRUD, search, migration) is at least 80% of statements, measured by a mainstream coverage tool.

## Assumptions

- The personal/offline / CLI-only scope from the constitution holds: this work does not introduce multi-user, network-exposed, authenticated, or non-CLI access modes.
- Modernization is permitted to require constitution amendments (e.g. relaxing "psycopg2 isolated to PsycopgDB" to "ORM session isolated to a persistence module"). Amendments are made via `/speckit-constitution` in the same change set, not after the fact.
- The work is an in-place refactor on the `001-modernize-stack` branch, with frequent commits, not a parallel rewrite.
- A one-time data migration is acceptable as long as no row is lost. The migration command is idempotent and re-runnable.
- "Mainstream framework" means: widely adopted, actively maintained, recognisable to a working Python developer in 2026. Specific framework choices (ORM, CLI, settings, test, lint) are deferred to `/speckit-plan`.
- The encrypted-config (Fernet) approach is preserved as a supported configuration source. The modernized config layer adds plain `.env` and environment-variable support alongside it.
- Per-class log file naming (e.g. `db_connector.log`, `interaction.log`) does not need to be preserved verbatim; structured logging with configurable sinks supersedes it.
- BibTeX parsing remains the responsibility of a dedicated library (currently `pybtex`). Switching to an alternative library is permitted if functionally equivalent.
- The Python minimum is raised to 3.11 (or 3.12 if a chosen mainstream library requires it).
- "Reverse engineer" produces a written architecture document; it does not require diagram-quality assets unless trivially produced (e.g. ER diagrams generated from the ORM models post-migration).
