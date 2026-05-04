# Research — UX Polish

Phase 0 output. Resolves the design questions that the spec deliberately left to planning, and catalogues the existing-call-site changes the implementation phase will touch.

## R1. Alias collision in disambiguation lists (and other "uniform-prefix" menus)

**Problem**: `cli/search._disambiguate` builds options as `[f"title: {r.title}" for r in results] + ["abort"]`. Every paper-row option starts with `title:` so first-alpha-char alias derivation yields `t` for every row → FR-005's collision rule would reject the menu on every multi-result search.

**Decision**: `ask_choice` accepts a per-option alias opt-out. Each option in the `options` list is either:

- `str` — auto-derive alias from the first alphabetic character of the label
- `tuple[str, str | None]` — `(label, explicit_alias)`. `alias=None` means "no alias on this option, digit-only".

The disambig list passes title rows as `(label, None)` tuples (digit-only) and the abort row as plain `"abort"` (auto-derives `a`).

**Rationale**: Per-option opt-out keeps the abort/quit affordance even on menus where most entries share a prefix. Whole-menu opt-out (a single boolean kwarg) was rejected because it would force the disambig list's abort entry to lose its alias too — an unnecessary regression in a feature whose entire point is broader alias coverage.

**Alternatives considered**:

- Whole-menu `aliases: bool = True` kwarg — rejected (loses abort alias on prefix-uniform menus).
- Strip `title: ` prefix and derive from the title's first letter — rejected (titles routinely start with the same letter; just relocates the collision and doesn't generalise to other uniform-prefix menus).
- Auto-suffix collisions with disambiguating digits ("(t)1", "(t)2") — rejected as overengineered. The constitution's rule is "MUST be unique within a menu — collisions MUST be resolved at construction time by the caller supplying an explicit alias, never by silent first-letter-wins"; the helper does not invent disambiguation policy.

## R2. Where the shared `disambiguate` helper lives

**Problem**: `cli/search._disambiguate` is module-private today. Update and delete need to reuse it.

**Decision**: Promote in place — rename to `cli/search.disambiguate` (drop the leading underscore). Import from `update.py` / `delete.py` via `from paper_sorts.cli.search import disambiguate`. **No new module.**

**Rationale**: The helper's dependencies are already pulled into `cli/search.py` — the `PaperSummary` DTO and `ask_choice`. Splitting it into a fresh shared module just creates a third import surface for zero reduction in coupling. The helper is a ≤ 10-line function; an in-place rename is the smallest viable change.

**Alternatives considered**:

- New `cli/disambiguate.py` module — rejected (extra import surface; one helper does not justify a new module).
- Move into `cli/prompts.py` — rejected. `cli/prompts.py` is constitutionally constrained to be the only `rich.prompt` consumer; it must stay layer-thin. `disambiguate` is a domain helper that consumes `ask_choice` and renders DB DTOs — it sits in the wrong layer.
- Move into `services/paper_service.py` — rejected. Services don't render or interact with the user; this helper does both.

## R3. ask_choice signature evolution (backwards compatibility)

**Problem**: Existing callers pass `options: list[str]`. The new mechanism needs per-option alias control. How does the type evolve without breaking those call sites?

**Decision**: New signature `options: Sequence[str | tuple[str, str | None]]`. Plain `str` → auto-derive; tuple → explicit alias, `None` opts out. The `quit_alias=` keyword is **removed** — its only caller (`app.py`) had `quit_alias="q"` for the `(Q)uit` option, which auto-derives correctly under the new mechanism.

**Rationale**: Backwards-compatible for every existing caller — they all pass plain `str` lists today. New callers that need opt-out or explicit override use the tuple form for the affected options only. Removing `quit_alias=` instead of leaving it as a deprecated alias keeps the helper's signature small (constitution Principle I, "readable, statically analysable").

**Alternatives considered**:

- New `aliases: list[str | None] | None = None` parallel kwarg — rejected. Decouples label and alias, so adding/removing an option in one list and forgetting the other is an off-by-one waiting to happen.
- `NamedTuple` `MenuOption(label, alias)` — rejected. Forces every caller to import the type; the tuple form is more idiomatic and equally type-checked under modern type checkers.
- Keep `quit_alias=` as a deprecated parameter — rejected. The single caller is in this same change set and is trivially updatable; deprecation cost > removal cost.

## R4. Confirmation summary text on the search-then-update papers path

**Problem**: Spec clarification says the confirmation echoes both title and id. Need exact wording.

**Decision**: For the papers-table search-then-update path:

```text
Please verify: You wish to change '<field>' of the paper '<title>' (id <N>) to '<new value>'.
 Proceed?
1) (Y)es
2) (N)o
```

For the bib/authors raw-id paths, retain the legacy wording verbatim:

```text
Please verify: You wish to change '<field>' of the entry '<id>' to '<new value>'.
 Proceed?
1) (Y)es
2) (N)o
```

The branch is `if table == "papers": <new wording> else: <legacy wording>`.

**Rationale**: Two different summaries because they have access to different information. Papers path has the full `PaperSummary` with title and id in scope (just returned by `disambiguate`); bib/authors paths only know the user-typed identifier and have no canonical title to echo. Forcing a unified template would either drop the title (regression) or render `'<id>'` redundantly with the legacy id-only line.

**Alternatives considered**:

- Always show the id even on the papers path with title-only labelling — rejected per spec clarification ("Both"; id retained for traceability).
- Use a single template with `{title or '<id>'}` — rejected. Hides the conditional behind a string-format trick; the per-path divergence is real and worth being explicit about in the code.

## R5. Existing call sites — what changes

Catalogued for the implementation phase. Every existing `ask_choice` call site reviewed for collisions and label updates.

| Call site | Today | After |
|-----------|-------|-------|
| `cli/app.py:123` (top menu) | `ask_choice("Your choice", ["Search the database", "Add an entry", "Update an entry", "(Q)uit"], quit_alias="q")` | `ask_choice("Your choice", ["Search the database", "Add an entry", "Update an entry", "(Q)uit"])` — auto-derives `s`/`a`/`u`/`q`; the `(Q)uit` parens are honoured by the helper. **No collision.** |
| `cli/search.py:50` (axis pick) | `ask_choice("Please choose a method:", ["Search by author", "Search by paper title"])` | unchanged — auto-derives `s` and `s`! **Collision.** Caller renames to `["(A)uthor", "(T)itle"]` or supplies explicit `[("Search by author", "a"), ("Search by paper title", "t")]`. **Decision: rename labels to `"Search by (a)uthor"` and `"Search by (t)itle"`** — auto-derive then picks the parenthesised letter, not the leading "S". (Alternative: use the explicit-tuple form. The label-rename is shorter and more readable for the user.) |
| `cli/search.py:86` (disambig) | `ask_choice("Choose paper to extract:", [f"title: {r.title}" for r in results] + ["abort"])` | `ask_choice("Choose paper to extract:", [(f"title: {r.title}", None) for r in results] + ["abort"])` — title rows digit-only; abort gets alias `a`. |
| `cli/update.py:67` (table pick) | `ask_choice("Which information do you want to update?", ["papers", "bib", "authors", "abort"])` | **Collision** — `authors` and `abort` both `a`. Resolution: rename `"abort"` → `"(q)uit"` on this menu. New options: `["papers", "bib", "authors", "(q)uit"]` — `p`/`b`/`a`/`q`. Matches the spec's example rendering verbatim. |
| `cli/update.py:88` (field pick — papers) | `["title", "contents", "abort"]` | `["title", "contents", "(q)uit"]` — `t`/`c`/`q`. No collision. |
| `cli/update.py:88` (field pick — bib) | `["bibtex", "abort"]` | `["bibtex", "(q)uit"]` — `b`/`q`. No collision. |
| `cli/update.py:88` (field pick — authors) | `["author", "abort"]` | `["author", "(q)uit"]` — `a`/`q`. No collision. |

**Net call-site delta**:

- 1 collision needs label disambiguation by tuple/rename (`search.py:50` axis pick).
- 1 collision needs the `abort → (q)uit` rename (`update.py:67` table pick).
- 1 menu (`search.py:86` disambig) needs the per-option `None` opt-out tuple form.
- 4 menus (top, papers field, bib field, authors field) work unchanged with auto-derive once the new helper lands.

The existing `_disambiguate`'s `"abort"` slot is left as-is (alias `a`, uncontested on that menu).

## R6. Performance — does adding a search step to update regress anything?

**Problem**: Constitution Principle IV requires no measurable regression on equivalent operations. The new search-then-update flow inserts a search query into the path where there used to be none.

**Decision**: No regression. Principle IV measures **operations**, not flows. The search query itself runs against the same SQL paths as `pdbsearch search` already does — those are baseline-matched. The update operation is unchanged. The composite (search + disambiguate + update) is naturally longer than the legacy (raw-id + update) because it does more work, but the per-operation budget is unchanged.

**Rationale**: This is exactly the framing Principle IV's rationale paragraph anticipates: "the criterion is framed as 'non-regression vs. the current baseline on a personal-library-sized dataset' rather than as fabricated absolute numbers". The composite-flow latency is a UX choice the user explicitly opted into via the spec — the alternative (legacy raw-id prompt) remains available via `--id`.

**Alternatives considered**:

- Cache search results across the flow to avoid a second roundtrip — rejected. Caching is explicitly excluded by Principle IV. The single search query already fetches the row's id; no second roundtrip exists.
- Add a benchmark for the composite flow — rejected for now. The component operations are baseline-matched; adding a flow-level benchmark would be a fresh measurement target without an existing baseline to non-regress against. If a regression is observed in practice, a benchmark can be added then as a measurement-driven decision.

## Summary

All five clarifications resolved (R1–R4 design, R5 catalog, R6 constitutional). No NEEDS CLARIFICATION items remain for the implementation phase. The implementation can proceed with:

- A new per-option-alias signature on `ask_choice` (R3).
- An in-place rename of `_disambiguate` (R2).
- A handful of label changes at existing call sites — `Search by (a)uthor` / `(t)itle` and `abort → (q)uit` on the update menus (R5).
- A table-aware confirmation summary on `update` (R4).
- No new dependencies, no schema changes, no new constitutional concerns (R6).
