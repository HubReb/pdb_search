# Data Model — UX Polish

This feature adds **no schema entities**. The four-table schema (`papers`, `bib`, `authors_id`, `authors_papers`) and its constraints are preserved verbatim — no Alembic migration is required.

Two in-memory shapes are involved:

## MenuOption (in-memory only, helper-internal)

A label that `ask_choice` renders as `<digit>) <label>`, plus a derived or caller-supplied alias character.

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| `label` | `str` | caller | Display text. The helper auto-wraps the alias char in parens at render time unless the label already contains a parenthesised single character (e.g. `"(Q)uit"`). |
| `alias` | `str \| None` | caller (explicit) or helper (derived) | Single lowercase character. `None` opts the option out of letter aliasing — that option is digit-only. When the caller passes a plain `str`, alias is auto-derived by the two-step rule: (a) if the label contains exactly one parenthesised single alphabetic character, that's the alias (`(Q)uit` → `q`, `Search by (a)uthor` → `a`); (b) otherwise the first alphabetic character of the label, lower-cased (`papers` → `p`). |

**Construction shapes** the caller passes into `ask_choice(options=...)`:

- Plain `str` → `MenuOption(label=str, alias=<auto-derived>)`
- `tuple[str, str]` → `MenuOption(label=tuple[0], alias=tuple[1])`
- `tuple[str, None]` → `MenuOption(label=tuple[0], alias=None)` — digit-only

**Validation** (raised at construction time as `ValueError`):

| Rule | Trigger |
|------|---------|
| Non-empty options | `len(options) == 0` |
| Single-char alias | Explicit alias whose `len() != 1` |
| Unique aliases within a menu | Two non-`None` aliases compare equal (case-insensitive) |
| Alphabetic auto-derivation source | Plain-`str` option whose label has neither a parenthesised single alphabetic char nor any alphabetic char at all |

**Lifecycle**: constructed inside `ask_choice` on each call; lives only for the duration of one prompt. No persistence.

## PaperSummary (existing — reused unchanged)

Defined in `paper_sorts/db/repositories.py`. Reused by the search-then-update path:

- `disambiguate(results: list[PaperSummary]) -> PaperSummary | None` returns the chosen row.
- The chosen row's `id` is the update target (passed to `service.update_field(table, field, identifier=id, value=...)`).
- The chosen row's `title` is echoed in the confirmation summary alongside `id`, per the spec's clarification on confirmation wording.

No fields are added; this DTO is consumed exactly as it is today.

## Out of scope

- No new Alembic migration. Schema unchanged.
- No new repository methods. The existing `PaperRepository.search_by_title` / `.search_by_author` / `.update_field` / `.delete` cover every operation this feature needs.
- No new service methods. `PaperService` is consumed unchanged from `update.py` and `delete.py`.
