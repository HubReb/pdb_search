# Feature Specification: UX Polish — Letter Aliases & Search-then-Act

**Feature Branch**: `002-ux-polish`
**Created**: 2026-05-04
**Status**: Draft
**Input**: User description: "Letter aliases on every menu (extend `paper_sorts.cli.prompts.ask_choice` so each option's first letter is accepted case-insensitively, in addition to the digit). Search-then-update replaces the raw-`id` prompt with the existing search-by-title/-by-author flow plus `_disambiguate`. Search-then-delete adds the same flow on the delete subcommand for symmetry. Both items are spec-level: `contracts/cli-commands.md` grammar updates, a constitution Principle III amendment (likely v1.4.0), and new tests for letter aliases and search-then-update."

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Letter aliases on every menu (Priority: P1)

The user is driving the interactive CLI in a low-attention context (the README cites "traveling by train"). On every numbered menu, they want to type a single letter — the first letter of the option label, case-insensitive — instead of locating and typing the right digit. Today only the top-level menu's `4) (Q)uit` accepts a `q` shortcut; every other menu (the update table picker, the update field picker, the search-axis picker, the abort/confirmation prompts) is digit-only.

**Why this priority**: This is the most pervasive friction in the CLI — every prompt that currently shows a numbered list inherits it. Fixing the prompt helper itself fixes every callsite at once with no per-screen redesign work, so it returns the highest UX yield per unit of code change. It also has the lowest risk profile: digit input remains valid, so existing scripted/typed flows keep working unchanged.

**Independent Test**: Can be fully verified by running each menu in the application interactively, typing the alias letter (uppercase and lowercase) for each option, and confirming the same dispatch happens as if the corresponding digit had been typed. Unit tests on `ask_choice` cover the helper itself; integration tests on the existing subcommands cover dispatch.

**Acceptance Scenarios**:

1. **Given** the top-level menu is displayed as `1) (S)earch the database / 2) (A)dd an entry / 3) (U)pdate an entry / 4) (Q)uit`, **When** the user types `s` (or `S`), **Then** the search interactive flow is dispatched.
2. **Given** the update table-picker menu is displayed as `1) (P)apers / 2) (B)ib / 3) (A)uthors / 4) (q)uit`, **When** the user types `B`, **Then** the bib-field menu is shown next.
3. **Given** any numbered menu, **When** the user types the digit instead of the letter, **Then** the same dispatch happens (no regression).
4. **Given** any numbered menu with a single-letter alias, **When** the user types an unrelated character (e.g., `z` on a menu without a `(Z)` option), **Then** the helper re-prompts without dispatching.
5. **Given** a menu where two option labels start with the same letter (e.g. a hypothetical "Search" and "Summary"), **When** the menu is constructed, **Then** the caller is required to disambiguate (supply an explicit alias for one of them) and a missing disambiguation surfaces as a developer-facing error at construction time, not as a silent first-letter-wins behaviour at runtime.

---

### User Story 2 — Search-then-update for papers (Priority: P2)

The user wants to update a field on a paper (its title, its summary, its BibTeX entry) but does not know — and should not have to know — the row's internal numeric `id`. Today the update interactive flow ends with `Please enter the respective id:` and a free-text integer prompt, which forces the user to either remember the id from a prior search session or run a search, scribble the number down, and come back. The fix is to replace that prompt with the existing search-by-title / search-by-author dialog, reuse the disambiguation helper from the search subcommand to pick a row, and then continue to the value prompt as today. A non-interactive `--id <N>` flag is preserved on the subcommand for scripted use.

**Why this priority**: This removes the single biggest pain point in the legacy update flow that survived the modernization (the raw-id prompt was preserved verbatim by the schema-preservation contract in 001-modernize-stack). It depends conceptually on the helper-promotion that User Story 3 also needs, but it is independently shippable: it exercises the existing search service methods unchanged, only re-routing the input collection.

**Independent Test**: Can be fully verified by seeding a known paper, invoking `pdbsearch update`, choosing the "papers" table, choosing the "title" field, typing a substring of the seeded paper's title at the search prompt, picking it from the disambiguation list, supplying a new title, confirming, and observing the row updated — all without ever typing or seeing the paper's numeric id.

**Acceptance Scenarios**:

1. **Given** a database seeded with one paper titled "Direct speech-to-speech translation", **When** the user invokes `pdbsearch update`, picks the papers table, picks the title field, types `speech` at the search prompt, accepts the single match, and supplies a new title, **Then** the title is updated and no raw-id prompt was shown.
2. **Given** a database with three papers whose titles all contain "translation", **When** the user runs the same flow and types `translation`, **Then** the disambiguation list is shown with a numbered entry per paper plus a trailing abort, and the chosen row's id is used as the update target.
3. **Given** the user types a search query that matches no rows, **When** the search returns empty, **Then** the existing "Paper was not found" plain-language message is shown and the update flow exits without writing anything.
4. **Given** scripted use, **When** `pdbsearch update --id 42 --table papers --field title --value "New Title"` (or the equivalent non-interactive surface) is invoked, **Then** the search step is bypassed and the row with id 42 is updated directly. The non-interactive surface MUST remain a viable path so existing scripts and tests do not break.
5. **Given** the user reaches the search step but the legacy raw-id prompt is removed, **When** an integration test asserts on the prompt sequence, **Then** the assertion confirms the new prompt sequence (search-axis menu → query → optional disambiguation → new-value prompt → confirmation) and confirms no `Please enter the respective id` string is emitted on the interactive papers-table path.

---

### User Story 3 — Search-then-delete for symmetry (Priority: P3)

The same friction exists on `pdbsearch delete`: today the only viable path is `pdbsearch delete --id 42`, with the interactive form prompting for a raw id. Delete should offer the same search-then-pick flow as update, both for parity with User Story 2 and because deleting a paper without first being sure you've selected the right one is exactly the failure mode that confirmation prompts exist to prevent — and a search-then-pick flow makes that confirmation step strictly more informative (the user has actively recognised the row, not just typed an id from memory).

**Why this priority**: Lower than User Story 2 because there is already a working escape hatch (`--id`); the friction is the same shape but lower volume in practice (delete is invoked less often than update). It is also strictly additive over User Story 2 — the disambiguation helper that becomes shared in US2 is reused here verbatim.

**Independent Test**: Can be fully verified by seeding a paper, invoking `pdbsearch delete` with no flags, typing a substring of the title at the search prompt, picking from the disambiguation list, confirming the deletion, and observing that the paper, its `authors_papers` rows, and (if no other paper references it) the bib row are removed.

**Acceptance Scenarios**:

1. **Given** a seeded paper, **When** the user runs `pdbsearch delete` with no flags and types a title substring at the search prompt, **Then** the disambiguation list is shown (or the single match is auto-selected), the existing confirmation prompt is shown using the picked row's title, and on `y` the paper is deleted.
2. **Given** scripted use, **When** `pdbsearch delete --id 42` is invoked, **Then** the search step is bypassed (no behavioural change to existing scripts).
3. **Given** the user types a search query that matches no rows, **When** the search returns empty, **Then** the existing "Paper was not found" message is shown and no delete is attempted.

---

### Edge Cases

- **Alias collision** — Two options on the same menu whose labels begin with the same letter. The helper MUST refuse to construct the menu (raising a developer-facing error) unless the caller supplied an explicit per-option alias to disambiguate. Silent first-letter-wins is forbidden because it would route to whichever option was registered first, which is a category of bug the digit-indexed legacy menus could not have.
- **Pure-digit input on an aliased menu** — Typing `2` continues to mean "second option" even when the second option's letter alias is `B`. Digits and aliases are both valid input forms; neither is preferred over the other.
- **Mixed-case input** — Both `b` and `B` MUST be accepted equivalently. The helper lower-cases input before matching.
- **Whitespace** — Leading/trailing whitespace on input MUST be tolerated (consistent with how `Prompt.ask` already strips it) but interior whitespace remains rejected.
- **Search returns zero results in update or delete flow** — The flow MUST emit the existing plain-language not-found message and return to the caller without attempting a write or a confirmation. No retry loop.
- **Search returns exactly one result** — Auto-select (mirrors the existing search subcommand's behaviour at `cli/search.py:76`); the disambiguation list is only shown for ≥ 2 matches.
- **User aborts at the disambiguation list** — The trailing abort entry returns control to the caller; no write is attempted; the existing exit code contract is preserved.
- **`--id` pointing at a non-existent row** — Existing behaviour preserved: plain-language `Error: no paper with id <N>` and a non-zero return where applicable. The search-then-act flow does not change this.

## Requirements *(mandatory)*

### Functional Requirements

#### Letter aliases on every menu

- **FR-001**: `paper_sorts.cli.prompts.ask_choice` MUST accept a single-letter alias for every option in addition to the option's 1-indexed digit.
- **FR-002**: By default, each option's alias is the first alphabetic character of its label (case-insensitive). The caller MAY override this default by supplying an explicit alias per option (to disambiguate collisions or to choose a non-leading letter when the leading letter is unhelpful, e.g. an option labelled "(Y)es" whose alias is `y` even though the label begins with `(`).
- **FR-003**: Aliases MUST be matched case-insensitively. The helper lower-cases the input before comparing.
- **FR-004**: Menu rendering MUST mark the alias character on each option label so the shortcut is discoverable, e.g. `1) (P)apers`, `4) (q)uit`. The exact rendering convention is "the alias character wrapped in parentheses inside the label" (matching the existing top-level `(Q)uit` precedent).
- **FR-005**: Two options on the same menu MUST NOT resolve to the same alias. The helper MUST raise a developer-facing error at construction time when an unresolved collision is detected, rather than silently routing to whichever option appeared first.
- **FR-006**: Digit input MUST continue to work for every menu (no regression). The new alias mechanism is strictly additive.
- **FR-007**: The existing top-level `q` alias MUST integrate with the general mechanism (not remain a special-case parameter on the helper). The `quit_alias=` keyword on `ask_choice` either disappears or becomes equivalent to "the last option's explicit alias".
- **FR-008**: Out-of-range digits and non-matching letters MUST continue to re-prompt rather than silently dispatch — the existing `Prompt.ask(choices=...)` behaviour applies to the union of digits and aliases.

#### Search-then-update

- **FR-009**: The `pdbsearch update` interactive flow on the `papers` table MUST replace the legacy `Please enter the respective id` prompt with the existing search dialog (search-axis pick → query → disambiguation), reusing the disambiguation helper from `cli/search.py` (which becomes shared rather than module-private).
- **FR-010**: `pdbsearch update` MUST continue to expose a non-interactive surface that takes the row identifier directly (e.g. `--id <N>` or equivalent flags), bypassing the search step. Existing scripted callers and tests MUST keep working unchanged.
- **FR-011**: The confirmation prompt before the write MUST keep its current contract — summarising the field name, target identifier, and new value — and MUST continue to accept `1`/`y`/`yes` / `2`/`n`/`no`. The change is to the input collection, not to the confirmation grammar.
- **FR-012**: An empty search result in the update flow MUST show the existing plain-language not-found message and return without attempting a write.

#### Search-then-delete

- **FR-013**: The `pdbsearch delete` interactive flow MUST replace its raw-id prompt with the same search-then-pick dialog used in FR-009.
- **FR-014**: `pdbsearch delete --id <N>` MUST continue to work exactly as today (no behavioural change to the non-interactive path).
- **FR-015**: The existing confirmation step (showing paper id and title) MUST be preserved verbatim; once a row is picked via search, its title is already known and is shown in the confirmation as today.

#### Documentation and governance

- **FR-016**: `specs/001-modernize-stack/contracts/cli-commands.md` MUST be updated (in this feature's change set) so its grammar section reflects the letter-alias rule and so the `update` and `delete` subcommand sections show the new search-then-act prompt sequences.
- **FR-017**: The constitution's Principle III (User Experience Consistency) MUST be amended via `/speckit-constitution` to authorise the broader letter-alias rule. The current text constrains aliases to "an optional `q` on the top menu only"; the amendment generalises this to "any menu MAY accept single-letter aliases per option, derived deterministically and case-insensitively, with caller-supplied disambiguation on collision". This is a MINOR-level amendment (new permissive rule, no existing rule relaxed in a backward-incompatible way), expected to land as v1.4.0.
- **FR-018**: New tests MUST be added covering: every alias-matching path on `ask_choice` (default first-letter, caller-supplied override, case-insensitivity, collision rejection); the search-then-update path including the zero-results case; the search-then-delete path including the zero-results case. Tests live alongside existing tests under `tests/unit/` (for `ask_choice`) and `tests/integration/` (for the subcommand flows) — no new top-level `tests/cli/` directory is required.

### Key Entities

This feature is a UX-and-prompt-grammar change; no schema entities are added or modified.

- **Menu option** *(in-memory only)*: A label that the helper renders as `<digit>) <label>`, plus a derived or caller-supplied alias character. No persistence.
- **Constitution Principle III** *(governance artefact)*: The text in `.specify/memory/constitution.md` § Principle III. The amendment authorising the broader alias rule is a deliverable of this feature.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A user can complete every interactive flow in the CLI by typing single-character inputs only (digits, alias letters, or confirmation tokens) — no flow forces the user to type a multi-digit number except where they have explicitly opted into the non-interactive `--id` path.
- **SC-002**: On the interactive update flow for the papers table, zero raw paper ids are typed by the user end-to-end. Verified by an integration test that drives the flow with only menu choices, search queries, and a new value, and asserts the correct row is updated.
- **SC-003**: On the interactive delete flow, zero raw paper ids are typed by the user end-to-end. Verified equivalently to SC-002.
- **SC-004**: Existing scripted callers using `pdbsearch delete --id <N>` (and the equivalent non-interactive update surface, however it is shaped after FR-010) keep working with zero edits. Verified by retaining and re-running every existing integration test that exercises a non-interactive path.
- **SC-005**: After this feature ships, the contract document (`specs/001-modernize-stack/contracts/cli-commands.md`) describes the letter-alias rule and the search-then-act flows, and a search of the document finds no remaining references to the obsolete `Please enter the respective id` prompt as part of the interactive update path on the papers table.
- **SC-006**: The constitution's recorded version reflects the amendment (v1.4.0) and the SYNC IMPACT REPORT entry at the top of `.specify/memory/constitution.md` describes the rule change. The amendment is ratified before the first code change covered by this spec is merged.
- **SC-007**: The test suite (`uv run pytest`) gains new cases covering the letter-alias paths and the search-then-act flows, and the suite passes end-to-end on `main` with no skips introduced for this feature.
- **SC-008**: No regression on existing menus' digit-input contract — every existing integration test that drives a menu by digit continues to pass without modification.

## Assumptions

- **Scope of search-then-update is limited to the `papers` table.** The `bib` and `authors` tables are reachable as update targets in the legacy two-step menu (the first menu picks `papers` / `bib` / `authors`). The user description explicitly says to "reuse `_disambiguate` from `cli/search.py:82`", and that helper returns a `PaperSummary` — there is no analogous search-by-bibtex-key or search-by-author-row helper today, and building one is out of scope for this feature. For `bib` and `authors` field updates, the legacy raw-id prompt is preserved (with the new letter aliases on the table-pick and field-pick menus). A follow-up spec can extend search-then-update to those tables once the corresponding search axes exist.
- **Disambiguation helper becomes shared.** `cli/search._disambiguate` is currently a module-private helper. To be reused by update and delete, it is promoted to a non-private location (e.g. moved into a shared module under `cli/`, or re-exported from `cli/search`). This is an internal refactor with no external surface change.
- **Alias for the abort/quit slot is at the caller's discretion.** Callers MAY label the last option `abort` (alias `a`), `(q)uit` (alias `q`), or some other consistent term; the helper does not impose a single name. The top-level menu retains `(Q)uit` as today; the update menus may switch from `abort` to `(q)uit` or `(a)bort` as the user prefers — that is a label choice, not a helper rule.
- **No-op on confirmation prompts' grammar.** The existing `1`/`2`/`y`/`n`/`yes`/`no` confirmation grammar (`ask_confirm`) is untouched — it already accepts both letter and digit forms. Letter aliases as described here apply to numbered menus (`ask_choice`), not to two-token confirmations.
- **No new dependency.** The change is implemented inside `cli/prompts.py` using `rich.prompt.Prompt`'s existing `choices=` validation; no new library is needed.
- **Constitution amendment lands first.** The constitution amendment (FR-017) is ratified before the prompts.py change merges, to avoid the new helper behaviour briefly violating the live constitution text. Sequence: `/speckit-constitution` → spec ratification of v1.4.0 → implementation PR.
