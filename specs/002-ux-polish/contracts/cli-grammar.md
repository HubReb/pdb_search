# CLI Grammar Contract — UX Polish

**Feature**: 002-ux-polish
**Component**: `paper_sorts/cli/prompts.py` (helper) + `cli/{app,search,update,delete}.py` (callers)
**Audience**: Test authors for the new alias and search-then-act paths; reviewers verifying the constitution-v1.4.0 letter-alias rule is implemented as described.

This document specifies (1) the new behaviour contract for `ask_choice`, and (2) the **delta** that this feature applies to `specs/001-modernize-stack/contracts/cli-commands.md` (the canonical CLI surface contract). Per spec FR-016, the delta is applied directly to that file as part of this implementation — this contract is the change-record.

---

## Part 1 — `ask_choice` contract

### Signature

```python
def ask_choice(
    prompt: str,
    options: Sequence[str | tuple[str, str | None]],
) -> int:
```

The `quit_alias` keyword parameter is **removed** in v1.4.0; its only caller (`app.py` top menu) auto-derives `q` from the `(Q)uit` label under the new mechanism.

### Per-option alias rules

Each entry of `options` is one of:

| Form | Alias derivation |
|------|------------------|
| `"plain label"` (str) | Auto-derive (two-step rule): **(a)** if the label contains exactly one substring matching `(<single-alphabetic-char>)`, the alias is that character lower-cased — `(Q)uit` → `q`, `Search by (a)uthor` → `a`. **(b)** Otherwise, the alias is the first alphabetic character of the label, lower-cased — `papers` → `p`, `bibtex` → `b`. |
| `("plain label", "x")` (tuple, alias is single char) | Use the supplied `x` (lower-cased) as the alias, overriding any auto-derivation. Useful when the desired alias is neither the parenthesised char nor the leading char (rare). |
| `("plain label", None)` (tuple, alias is None) | Option is **digit-only** — typing the alias for some other option does not match this row. |

### Rendering

For each option, the helper prints `<digit>) <rendered_label>` where:

- If the label already contains a parenthesised single character (e.g. `"(Q)uit"`, `"Search by (a)uthor"`), the label is rendered verbatim.
- Otherwise, the helper inserts `(<alias_uppercase_or_lowercase>)` immediately before the first occurrence of the alias character in the label. If the alias is the leading character of the label, that becomes `(P)apers` style. If the alias was supplied explicitly and does not appear in the label, the helper prepends `(<alias>) ` to the label.
- If the option's alias is `None`, the label is rendered verbatim with no parens inserted.

### Input matching

After printing the menu, the helper reads a single line and matches it against the union of:

- `"1"`, `"2"`, ..., `str(len(options))` — digit input maps to the 1-indexed option position.
- All non-`None` aliases (case-insensitive) — alias input maps to its option's position.

Non-matching input re-prompts via `rich.prompt.Prompt.ask(choices=...)`'s built-in re-prompt loop. Whitespace is stripped (existing behaviour); interior whitespace fails.

### Construction-time errors (`ValueError`)

| Trigger | Message style |
|---------|---------------|
| `len(options) == 0` | `"ask_choice requires at least one option"` (existing message preserved). |
| Two non-`None` aliases compare equal (case-insensitive) | `"alias collision on menu: 'a' resolves both 'authors' and 'abort'"` (or equivalent — names the colliding alias and both labels). |
| Explicit alias of length ≠ 1 | `"alias must be a single character"`. |
| Plain-`str` option whose label contains no alphabetic characters | `"cannot auto-derive alias from label '12345'; supply an explicit alias or None"`. |

These errors fire at construction time (before the menu is rendered or any input is read). They are developer-facing — `ask_choice` callers under `src/paper_sorts/cli/` are expected to satisfy them at code-review time.

---

## Part 2 — Delta on `specs/001-modernize-stack/contracts/cli-commands.md`

Per spec FR-016, the existing CLI command contract is updated in this same change set. The deltas below are applied verbatim.

### § "Top-level interactive menu (default command)" — Grammar bullet update

**Before** (v1.3.0):

> Grammar (constitution Principle III, v1.3.0):
> - 1-indexed numeric.
> - Mandatory abort/quit at the bottom.
> - Empty input re-prompts.
> - `q` is accepted in addition to `4`.
> - Selecting an option dispatches to the corresponding subcommand's interactive flow, then returns to the menu when that flow completes.

**After** (v1.4.0):

> Grammar (constitution Principle III, v1.4.0):
> - 1-indexed numeric.
> - Mandatory abort/quit at the bottom.
> - Empty input re-prompts.
> - **Single-letter aliases are accepted on every numbered menu in addition to the digit.** Aliases are derived deterministically (first alphabetic character of the option label by default, case-insensitive), rendered with parens on the option label (e.g. `1) (S)earch the database`, `4) (Q)uit`), and unique within a menu. Callers disambiguate collisions by passing an explicit alias for at least one of the colliding options or by opting an option out (`alias=None`).
> - Selecting an option dispatches to the corresponding subcommand's interactive flow, then returns to the menu when that flow completes.

### § "Subcommand: `update`" — Replace prompt sequence

**Before** (v1.3.0): the dialog's last steps prompted for the raw row id:

```text
Which entry do you want to update?
Please enter the respective id: 42

Enter the new information: <new title>
```

**After** (v1.4.0):

```text
Which information do you want to update?
1) (P)apers
2) (B)ib
3) (A)uthors
4) (q)uit
Your choice: p

Which information do you want to update?
1) (T)itle
2) (C)ontents
3) (q)uit
Your choice: t

Search interface
Please choose a method:
1) Search by (a)uthor
2) Search by (t)itle
Your choice: t

Please enter the paper title: speech

Following papers found:
1) title: Direct speech-to-speech translation with discrete units
2) title: ...
3) abort
Choose paper to extract: 1

Enter the new information: <new title>

Please verify: You wish to change 'title' of the paper 'Direct speech-to-speech translation with discrete units' (id 42) to '<new title>'.
 Proceed?
1) (Y)es
2) (N)o
Your choice: y
```

For the `bib` and `authors` tables, the legacy raw-id prompt and confirmation wording are preserved verbatim (the `Please enter the respective id` line stays for those two tables).

### § "Subcommand: `update`" — Add `--id` flag entry

**Add to the section header**:

```text
pdbsearch update [--id N]
```

**Behaviour clause to add**:

> When `--id <N>` is supplied, the search step is skipped on the `papers` table — the user proceeds directly into the existing table/field/value/confirmation prompt sequence with the row identified by `N`. The flag has no effect on the `bib`/`authors` raw-id paths (they were never search-driven). No `--table` / `--field` / `--value` flags are added in this feature.

### § "Subcommand: `delete`" — Replace interactive prompt

**Before** (v1.3.0): the interactive form prompted for the raw row id.

**After** (v1.4.0):

```text
pdbsearch delete [--id N]
```

> Without `--id`, the interactive form runs the same search-then-pick dialog as `update`:
>
> ```text
> Search interface
> Please choose a method:
> 1) Search by (a)uthor
> 2) Search by (t)itle
> Your choice: t
>
> Please enter the paper title: speech
>
> Following papers found:
> 1) title: ...
> 2) abort
> Choose paper to extract: 1
>
> Please verify: You wish to DELETE paper id 42 ('<title>'). This cannot be undone.
> 1) (Y)es
> 2) (N)o
> Your choice: y
> ```
>
> With `--id <N>`, the existing non-interactive path runs unchanged (search step bypassed; confirmation still shown).

The cascade behaviour (`authors_papers` rows for the deleted paper, and unreferenced authors / bib rows) is unchanged.

---

## Acceptance summary

A test driver that exercises the new contract should assert:

- **ask_choice unit tests** — alias auto-derivation, explicit alias, `None` opt-out, case-insensitive match, collision rejection, digit-input non-regression.
- **update integration tests** — interactive papers-table search-then-update updates the correct row using only the prompt sequence above (no raw-id prompt emitted for the papers path); `--id 42` bypasses search but still walks the table/field/value/confirmation prompts; bib/authors interactive paths still emit `Please enter the respective id`; zero-results case shows the existing not-found message and exits without writing.
- **delete integration tests** — interactive form emits the search-then-pick dialog and deletes via the picked row; `--id 42` works unchanged; zero-results case shows the existing not-found message and exits without deleting.
- **Constitution-text integration** — Principle III's rendered text in `.specify/memory/constitution.md` matches the v1.4.0 alias bullet (already merged at commit `8ebfa9d`); the contract document at `specs/001-modernize-stack/contracts/cli-commands.md` reflects the deltas above (applied in this same change set).
