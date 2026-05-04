# Quickstart — UX Polish

What's new for the user once 002-ux-polish ships. Pairs with the install/run quickstart at [`specs/001-modernize-stack/quickstart.md`](../001-modernize-stack/quickstart.md), which still describes how to start the CLI from scratch.

## Letter aliases on every menu

Every numbered menu now accepts a single-letter shortcut in addition to the digit. The alias is rendered with parens on the option label so it's discoverable at a glance:

```text
What do you want to do?
1) (S)earch the database
2) (A)dd an entry
3) (U)pdate an entry
4) (Q)uit
Your choice: s
```

Type `s` (or `S`), and the search flow opens. Type `1`, same effect. Both are valid for the lifetime of the menu.

The same shortcut grammar applies on every internal menu — the update table picker, the update field picker, the search-axis picker, the disambiguation list's `abort` row, and the confirmation prompts. There is one menu where the title rows are deliberately digit-only because every title would alias to `t`: the disambiguation list (`Choose paper to extract:`). On that menu, type a digit for a row, or `a` for `abort`.

## Search-then-update for papers

`pdbsearch update` no longer asks for the raw paper id when you're updating a `papers`-table field. Instead, it runs the same search dialog the `search` subcommand does:

```text
$ pdbsearch update
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
2) title: Speech recognition baselines for low-resource languages
3) abort
Choose paper to extract: 1

Enter the new information: Direct speech-to-speech translation, revised

Please verify: You wish to change 'title' of the paper 'Direct speech-to-speech translation with discrete units' (id 42) to 'Direct speech-to-speech translation, revised'.
 Proceed?
1) (Y)es
2) (N)o
Your choice: y
```

Notice the confirmation echoes both the title and the id — the title closes the recognition loop (you confirm what you just picked from the list), and the id stays visible for log/audit traceability.

For the `bib` and `authors` tables, the legacy raw-id prompt is preserved — those paths still ask `Please enter the respective id:`.

### `--id` for scripting

If you already know the row's id (e.g. from a script or a prior search), pass `--id <N>` and the search step is skipped:

```text
$ pdbsearch update --id 42
Which information do you want to update?
1) (P)apers
2) (B)ib
3) (A)uthors
4) (q)uit
Your choice:
```

Note: `--id` only skips the search step on the papers table. Table, field, and new-value collection remain interactive — there are no `--table`, `--field`, or `--value` flags. Apply that pattern only when you actually have a stable id.

## Search-then-delete

`pdbsearch delete` gains the same search-then-pick affordance:

```text
$ pdbsearch delete
Search interface
Please choose a method:
1) Search by (a)uthor
2) Search by (t)itle
Your choice: t
Please enter the paper title: speech

Following papers found:
1) title: Direct speech-to-speech translation with discrete units
2) abort
Choose paper to extract: 1

Please verify: You wish to DELETE paper id 42 ('Direct speech-to-speech translation with discrete units'). This cannot be undone.
1) (Y)es
2) (N)o
Your choice: y
Deleted paper id 42.
```

The existing non-interactive form `pdbsearch delete --id 42` continues to work exactly as before (no behavioural change for scripts).

## Rule of thumb

- Single-letter aliases are always accepted in addition to digits — no menu is digit-only any more (with the disambiguation-list exception above).
- For `update` and `delete`, prefer the interactive form unless you're scripting; you no longer have to know paper ids.
- For scripted paths, `--id <N>` is the stable, non-interactive entry point on both subcommands.
