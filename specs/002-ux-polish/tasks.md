# Tasks: UX Polish — Letter Aliases & Search-then-Act

**Input**: Design documents from `/specs/002-ux-polish/`
**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md), [contracts/cli-grammar.md](./contracts/cli-grammar.md), [data-model.md](./data-model.md)

**Tests**: Required (per spec FR-018). Test tasks appear before implementation tasks within each user story for TDD-friendly sequencing, but the constitution does not mandate strict red-green-refactor — running tests against a partially-staged change set is acceptable as long as the final commit is green.

**Organization**: Tasks are grouped by user story. Each story is independently testable per the spec (US1 ships letter aliases on every menu without changing update/delete flows; US2 ships search-then-update on papers; US3 ships search-then-delete).

## Format

`- [ ] [TaskID] [P?] [Story?] Description`

- **[P]**: Parallelizable — different files, no dependency on an incomplete task in the same phase.
- **[Story]**: `[US1]` / `[US2]` / `[US3]` — maps to spec.md user stories. Setup, Foundational, and Polish tasks have no story label.

## Constitution prerequisite (already complete)

- [X] T000 Apply constitution v1.4.0 amendment via `/speckit-constitution` per spec FR-017 — DONE, commit `8ebfa9d` (2026-05-04). Principle III gains a fourth bullet authorising letter aliases on every numbered menu (deterministic first-alpha-char default, case-insensitive, unique within a menu, additive over digit input). All implementation work below operates against the live v1.4.0 text.

---

## Phase 1: Setup

**Purpose**: Confirm the branch is in a state where implementation can start.

- [ ] T001 Confirm working tree is on branch `002-ux-polish` and clean; verify `uv sync --all-extras` succeeds and `uv run pytest` passes the existing 001-era suite end-to-end (baseline parity check before adding any 002 changes).

---

## Phase 2: Foundational (Blocking Prerequisites for US2 + US3)

**Purpose**: Promote the disambiguation helper that both US2 and US3 will import. This is a no-behavior-change rename — the only existing caller (search.py itself) is updated in lockstep.

**⚠️ Note**: US1 does **not** depend on this phase — US1 can start immediately after Phase 1 if work is parallelised. US2 and US3 do depend on it.

- [ ] T002 Rename `_disambiguate` to public `disambiguate` in `src/paper_sorts/cli/search.py` — drop the leading underscore from the function definition (line 82) and from the only existing call site (line 76). No signature change, no behaviour change. Verify the existing `tests/integration/test_search.py` cases still pass.

**Checkpoint**: `disambiguate` is importable as `from paper_sorts.cli.search import disambiguate` from update.py and delete.py.

---

## Phase 3: User Story 1 — Letter aliases on every numbered menu (Priority: P1) 🎯 MVP

**Goal**: Every `ask_choice` menu in the CLI accepts a single-letter alias in addition to its 1-indexed digit. Aliases derive deterministically (parenthesised single-alpha char wins; otherwise first-alpha char of label), case-insensitive, unique within a menu, with caller-supplied disambiguation on collision (explicit alias or `None` opt-out).

**Independent Test**: Run `uv run pdbsearch` and walk through every interactive menu using only letter shortcuts (the alias char rendered in parens on each option label). Every menu dispatches correctly. Typing the digit also still works on every menu (no regression). Unit tests on `ask_choice` cover the helper paths in isolation.

**⚠️ Atomic-landing note**: T004, T005, T006, T007 should land in the same commit (or strictly sequential commits with no intermediate `uv run pytest`) — T004 introduces collision rejection, and T005–T007 contain the call-site fixes for the two real collisions catalogued in [research.md § R5](./research.md). An interim state with T004 only is broken.

### Tests for User Story 1

- [ ] T003 [P] [US1] Add unit tests to `tests/unit/test_prompts.py` covering `ask_choice` with the new alias mechanism. Cases: (a) plain-`str` option list — alias auto-derived as first alpha char (`["papers", "bib"]` → typing `p` returns 1, `b` returns 2); (b) parens-prefixed labels — alias picked from inside parens (`["(Q)uit"]` → typing `q` returns 1); (c) explicit tuple alias — `[("Search by author", "a"), ("Search by paper title", "t")]` accepts `a`/`t`; (d) `None` opt-out — `[("title: A", None), ("title: B", None), "abort"]` rejects `a` only as the index for `abort` and accepts only digits for the title rows; (e) case-insensitive — both `q` and `Q` map to the `(Q)uit` slot; (f) digit input non-regression — `["papers", "bib", "authors", "(q)uit"]` accepts `1`/`2`/`3`/`4` exactly as before; (g) collision rejection at construction — `["authors", "abort"]` raises `ValueError` mentioning `a`; (h) length-1 alias rule — explicit `("foo", "ab")` raises `ValueError`; (i) auto-derive failure — plain `"12345"` (no alpha chars, no parens) raises `ValueError`. **Tests are expected to FAIL until T004 lands.**

### Implementation for User Story 1

- [ ] T004 [US1] Extend `ask_choice` in `src/paper_sorts/cli/prompts.py`. Change the signature from `options: list[str], *, quit_alias: str | None = None` to `options: Sequence[str | tuple[str, str | None]]` (drop `quit_alias`). Implement: (a) two-step alias derivation — if the label matches `(<single-alpha-char>)`, alias is that char lower-cased; otherwise first alpha char of label, lower-cased; (b) explicit-tuple form `(label, alias)` overrides auto-derive; `(label, None)` opts the option out (digit-only); (c) construction-time collision detection — two non-`None` aliases compare equal (case-insensitive) → `ValueError` naming the colliding alias and both labels; (d) construction-time validation — empty options list, length-1 alias rule, non-derivable label → `ValueError` with the messages catalogued in [contracts/cli-grammar.md § Part 1 — Construction-time errors](./contracts/cli-grammar.md); (e) rendering — for each option print `<digit>) <rendered_label>` where labels containing `(<single-alpha-char>)` render verbatim, plain-str labels with auto-derived aliases get `(<alias>)` inserted before the alias char, and `None`-opt-out options render verbatim with no parens; (f) input matching — pass the union `[str(i) for i in range(1, n+1)] + [alias for alias in aliases if alias is not None]` (with both lower- and upper-case alias variants) as `choices=` to `Prompt.ask` so re-prompt is automatic. Update the docstring to describe the new contract.

- [ ] T005 [P] [US1] Update the top menu call site at `src/paper_sorts/cli/app.py` line ≈ 123 — drop the `quit_alias="q"` kwarg from the `ask_choice` call. Labels (`"Search the database"`, `"Add an entry"`, `"Update an entry"`, `"(Q)uit"`) auto-derive `s`/`a`/`u`/`q` (no collision). No other change to app.py needed.

- [ ] T006 [US1] Update `src/paper_sorts/cli/search.py` two call sites: (a) axis-pick at line ≈ 50 — rename labels from `["Search by author", "Search by paper title"]` to `["Search by (a)uthor", "Search by (t)itle"]` (auto-derive picks the parenthesised char per the two-step rule, resolving the leading-`s`/leading-`s` collision); (b) disambig list at line ≈ 86 — convert title rows to `(label, None)` tuples so they are digit-only on the menu, while keeping the trailing `"abort"` as a plain `str` (auto-derives `a`). The signature change is `options=[(f"title: {r.title}", None) for r in results] + ["abort"]`.

- [ ] T007 [P] [US1] Update `src/paper_sorts/cli/update.py` menu labels: (a) the table-pick options at line ≈ 67 change from `["papers", "bib", "authors", "abort"]` to `["papers", "bib", "authors", "(q)uit"]` (resolves the leading-`a` collision between `authors` and `abort`; matches spec rendering); (b) the field-pick options at line ≈ 88 — for each table, replace the trailing `"abort"` with `"(q)uit"`. Adjust the `_pick_field` and `_pick_table` early-return logic to treat the last option as the abort-out (existing logic uses `len(options)` index — unchanged). Confirm via local run that typing `q`/`Q` aborts correctly on every update menu.

- [ ] T008 [US1] Run `uv run pytest tests/unit/test_prompts.py` (T003 cases now pass against T004's implementation) plus the full `uv run pytest` for non-regression. Run `uv run ruff check src tests` and `uv run mypy src` — both must be clean. If anything fails, fix in place under the appropriate task.

**Checkpoint**: User Story 1 is fully functional. Every interactive menu accepts both digit and letter input. `pdbsearch update` and `pdbsearch delete` still use the legacy raw-id prompt at this point — that's deliberate; US2/US3 replace it.

---

## Phase 4: User Story 2 — Search-then-update for papers (Priority: P2)

**Goal**: On the `papers`-table interactive update path, replace the raw-id prompt with the existing search dialog plus `disambiguate()`. Add a `--id <N>` flag on the `update` subcommand that bypasses only the search step. The confirmation summary on the papers path echoes both title and id; bib/authors paths retain the legacy summary wording.

**Independent Test**: With a seeded DB, run `pdbsearch update`, pick `(p)apers`, pick `(t)itle`, search for a substring, pick a row from the disambig list, supply a new value, confirm — assert the chosen row was updated and that no `Please enter the respective id` prompt was emitted on the papers path. Then run `pdbsearch update --id 42` and assert the search step is skipped while the table/field/value/confirmation prompts run as before. Then run the same flow on `(b)ib` and `(a)uthors` and assert the legacy raw-id prompt and confirmation wording are still in place.

**⚠️ Depends on**: T002 (`disambiguate` public name).

### Tests for User Story 2

- [ ] T009 [P] [US2] Add integration tests to `tests/integration/test_update.py` covering: (a) papers-table search-then-update happy path — drive through `(p)apers` → `(t)itle` → search-axis menu → query → single-result auto-pick → new value → `y`; assert the row's title is updated and that `"Please enter the respective id"` is **not** in the captured stdout; (b) papers-table multi-result disambig — three rows match the query, assert the disambig list is shown with `(label, None)` tuples (typing `1` selects, typing `t` does not), the third option `abort` is selectable via `a`; (c) `--id 42` non-interactive entry — invoke `update --id 42`, drive through `(p)apers` → `(t)itle` → new value → `y`; assert the row is updated and that no search-axis menu was emitted; (d) bib-table raw-id retained — `(b)ib` → `(b)ibtex` → `Please enter the respective id` prompt still appears; (e) authors-table raw-id retained — equivalent assertion; (f) zero-results case — search query matches nothing, assert the existing `"Paper was not found in db_connector."` message and that no update was performed; (g) confirmation summary on papers path — capture stdout and assert it contains `"of the paper '<exact title>' (id <N>)"`; (h) confirmation summary on bib/authors paths — assert it still contains the legacy `"of the entry '<id>'"` wording.

### Implementation for User Story 2

- [ ] T010 [US2] Modify `src/paper_sorts/cli/update.py`. Changes: (a) add `paper_id: Annotated[int | None, typer.Option("--id", help="Paper id; bypasses search-then-pick on the papers table.")] = None` to the `update` signature; (b) on the `papers` table, replace the legacy `id_str = ask_text("Which entry...")` block with: if `paper_id is not None`, use it directly; otherwise, run the existing search dialog from `cli.search._run` (or factor out the search-pick portion into a helper) and call `disambiguate(results)` to pick a row, returning early with the existing not-found message when results are empty; (c) extract the chosen row's `id` and `title` for the confirmation step; (d) branch the confirmation summary on `table == "papers"`: papers path renders `Please verify: You wish to change '<field>' of the paper '<title>' (id <N>) to '<value>'.`; bib/authors paths render the legacy `Please verify: You wish to change '<field>' of the entry '<id>' to '<value>'.`; (e) do **not** add `--table`/`--field`/`--value` flags (per spec clarification — `--id` is the only new flag in this feature). Update the module docstring to describe the new search-then-update flow on the papers table.

- [ ] T011 [US2] Run `uv run pytest tests/integration/test_update.py` until T009 passes. Run `uv run ruff check src tests` and `uv run mypy src` clean. Run the full `uv run pytest` for non-regression.

**Checkpoint**: User Story 2 is fully functional. Interactive papers updates use the search-then-pick flow; `--id` is available for scripting; bib/authors paths are unchanged.

---

## Phase 5: User Story 3 — Search-then-delete (Priority: P3)

**Goal**: When `pdbsearch delete` is invoked without `--id`, run the same search-then-pick flow as `update` and use the picked row for the existing confirmation step. The `--id` non-interactive path is unchanged.

**Independent Test**: With a seeded DB, run `pdbsearch delete` (no flags), search for a substring, pick a row, confirm — assert the paper, its `authors_papers` rows, and (where unreferenced) the bib row are deleted. Then run `pdbsearch delete --id 42` and assert the search step is skipped while the existing confirmation runs.

**⚠️ Depends on**: T002 (`disambiguate` public name).

### Tests for User Story 3

- [ ] T012 [P] [US3] Add integration tests to `tests/integration/test_delete.py` covering: (a) interactive search-then-delete happy path — invoke `delete` (no flags), drive through search-axis menu → query → single-result auto-pick → confirmation `y`; assert the row is deleted and that no `Please enter the paper id to delete` prompt was emitted; (b) multi-result disambig — three rows match, assert the disambig list is shown and the chosen row is deleted; (c) `--id 42` path unchanged — invoke `delete --id 42`, drive through confirmation, assert deletion (this is the existing test path; verify it still passes); (d) zero-results case — search query matches nothing, assert the existing not-found message and that no deletion occurred; (e) cascade behaviour preserved — the deleted row's `authors_papers` rows are gone, authors with no remaining papers are removed, and the bib row is removed iff no other paper references it.

### Implementation for User Story 3

- [ ] T013 [US3] Modify `src/paper_sorts/cli/delete.py`. Changes: (a) when `paper_id is None`, replace the legacy `id_str = ask_text("Please enter the paper id to delete")` block with: run the existing search dialog (search-axis menu → query → results) and call `disambiguate(results)` to pick a row, returning early with the existing not-found message when results are empty; (b) extract `paper_id` and `title` from the picked row; (c) the existing confirmation block (`Please verify: You wish to DELETE paper id <N> ('<title>'). This cannot be undone.`) is **unchanged** — the picked row already supplies both `id` and `title`; (d) `paper_id is not None` (i.e. `--id 42` path) is unchanged. Update the module docstring to describe the new search-then-delete flow.

- [ ] T014 [US3] Run `uv run pytest tests/integration/test_delete.py` until T012 passes. Run the full `uv run pytest` for non-regression. Run `uv run ruff check src tests` + `uv run mypy src` clean.

**Checkpoint**: User Story 3 is fully functional. All three user stories are now complete and testable independently.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Spec-mandated documentation deltas (FR-016) and final sweep before merging.

- [ ] T015 [P] Update `specs/001-modernize-stack/contracts/cli-commands.md` per spec FR-016 — apply the verbatim deltas catalogued in [specs/002-ux-polish/contracts/cli-grammar.md § Part 2](./contracts/cli-grammar.md): (a) the grammar bullet under § "Top-level interactive menu (default command)" gains the v1.4.0 letter-alias rule; (b) § "Subcommand: `update`" replaces its legacy raw-id prompt sequence with the new papers-table search-then-update walkthrough and adds the `--id` flag entry; (c) § "Subcommand: `delete`" replaces its interactive form with the search-then-pick walkthrough. Bib/authors raw-id paths in § "Subcommand: `update`" stay; the section header adds `[--id N]` to the `update` invocation line.

- [ ] T016 [P] Run the full quality sweep on the feature branch: `uv run pytest` (full suite green, no skips introduced for this feature), `uv run ruff check src tests` (clean), `uv run mypy src` (clean). Document any unexpected warnings inline rather than suppressing them silently.

- [ ] T017 Manual sanity walkthrough using [specs/002-ux-polish/quickstart.md](./quickstart.md) — run `uv run pdbsearch` against a local DB and exercise: top-menu letter alias, update on papers via search-then-pick (single match auto-select), update on bib via raw-id (legacy), delete via search-then-pick, delete via `--id`, abort on every menu via `q` (or `a` on the disambig list). Note any UX surprises in a follow-up issue.

- [ ] T018 Self-review the diff one last time against the spec's Success Criteria SC-001..SC-008 and confirm each is verifiably met. SC-006 (constitution v1.4.0 ratified before code merges) is already satisfied by commit `8ebfa9d`; the remaining seven are verified by the test suite + the contract doc update.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Phase 1 (Setup)**: No dependencies — start immediately.
- **Phase 2 (Foundational)**: Depends on Phase 1. **Blocks US2 and US3 only** — US1 does not depend on T002.
- **Phase 3 (US1)**: Depends on Phase 1. Independent of Phase 2.
- **Phase 4 (US2)**: Depends on Phase 1 + Phase 2 (T002).
- **Phase 5 (US3)**: Depends on Phase 1 + Phase 2 (T002). Independent of Phase 4 — does not need US2 to ship.
- **Phase 6 (Polish)**: Depends on US1 + US2 + US3 (whichever stories are being shipped).

### User Story Dependencies

- **US1 (P1)**: Independent of US2/US3. Ships letter-alias UX without touching update/delete prompt sequences. **Recommended MVP scope.**
- **US2 (P2)**: Depends on T002 (`disambiguate` public name). Independent of US1 in principle (US2 does not require US1's helper change), but typically shipped after US1 so the disambig list it invokes already has US1's `(label, None)` opt-out applied. If US2 ships before US1, the disambig list is broken (every title aliases to `t`) — therefore **US1 must ship before US2**, even though they are conceptually independent stories.
- **US3 (P3)**: Same dependency structure as US2 — depends on T002 and effectively requires US1 first for the disambig-list opt-out. Independent of US2.

### Within Each User Story

- Tests (when present) MUST FAIL initially and pass after implementation. The tasks list sequences them test-first.
- T011-T014 within Phase 3 (US1) **must land atomically** — see the atomic-landing note above. Specifically: T004 (helper extension) introduces collision rejection; T005, T006, T007 (call-site updates) contain the fixes for the two real collisions catalogued in research § R5. An interim state with only T004 applied is broken on `search.py:50` (axis pick) and `update.py:67` (table pick).

### Parallel Opportunities

- **Within Phase 3 (US1)**: T003 (tests) [P] runs in parallel with T004 (helper) work. After T004, T005 (app.py), T006 (search.py), T007 (update.py) can proceed in parallel since they touch different files. T008 (run-and-verify) is sequential.
- **Within Phase 4 (US2)**: T009 (tests) [P] runs in parallel with T010 (impl) work; T011 sequential.
- **Within Phase 5 (US3)**: T012 (tests) [P] runs in parallel with T013 (impl); T014 sequential.
- **Across user stories**: US2 and US3 are independent given T002, so a paired implementer could split US2 and US3 if useful. In practice the diffs are small enough that one implementer takes both.
- **Within Phase 6 (Polish)**: T015 (contract doc) [P] and T016 (quality sweep) [P] are independent.

---

## Parallel Example: User Story 1

```bash
# After T002 lands and Phase 3 begins, three call-site updates can be
# drafted in parallel by different implementers (or by one implementer
# in any order — the helper extension T004 must land before any of them
# is committed alongside its file's changes):
Task: "T005 Update top menu in src/paper_sorts/cli/app.py"
Task: "T006 Update search.py axis-pick + disambig list"
Task: "T007 Update update.py menu labels"

# T003 (unit tests) can be written entirely in parallel with the impl,
# since tests/unit/test_prompts.py is a separate file from prompts.py:
Task: "T003 Add unit tests for ask_choice alias mechanism"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Phase 1 (T001) — confirm baseline.
2. Phase 3 (T003-T008) — letter aliases on every menu.
3. **STOP and VALIDATE**: walk through the CLI manually using letter shortcuts on every menu; run the integration test suite to confirm no regression on existing update/delete tests (those still use raw-id prompts at this point).
4. Open a PR titled "002-ux-polish: letter aliases on every menu (US1)" — independently mergeable.

### Incremental Delivery (Recommended)

1. Phase 1 → confirm baseline.
2. Phase 3 (US1) → ship MVP — open PR, merge.
3. Phase 2 (T002) + Phase 4 (US2) → ship search-then-update — open PR, merge.
4. Phase 5 (US3) → ship search-then-delete — open PR, merge.
5. Phase 6 (Polish) → contract doc update + final sweep — open PR, merge.

This produces 4 small PRs — each independently reviewable, each shipping value. The user's two-commit-splits preference (per memory) maps cleanly: a typical PR here is 1–2 commits.

### Compressed Delivery (One PR for the whole feature)

If the user prefers to ship everything in a single PR (also viable given the small diff size — ≈ 6 source files + 3 test files + 1 contract doc):

1. Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6 in sequence.
2. Single PR titled "002-ux-polish: letter aliases & search-then-act (US1+US2+US3)".
3. Two-commit split per the user's preference: commit 1 = US1 (helper + call-site label fixes + unit tests); commit 2 = US2 + US3 + contract doc update.

---

## Notes

- `[P]` tasks = different files, no dependencies on incomplete tasks within the same phase.
- `[Story]` label maps task to a specific user story for traceability — only set on Phase 3/4/5 tasks.
- Each user story (US1, US2, US3) is independently mergeable into `main` (with the dependency-ordering caveat for US2/US3 noted above).
- All call-site changes preserve the constitution-mandated layer boundaries — no new module imports `rich.prompt` or `sqlalchemy`; `cli/prompts.py` remains the single prompt-helper module.
- Commit at task boundaries when the tree is green, or at logical groups (e.g. "all of US1" as one commit + "all of US2" as another).
- Constitution v1.4.0 is already in effect (commit `8ebfa9d`) — no further amendment work in this task list.
