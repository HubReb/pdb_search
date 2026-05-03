"""Rich-backed user-prompt wrappers (constitution Principle III v1.4.0 boundary).

This is the only module under ``src/paper_sorts/`` permitted to import
:mod:`rich.prompt`. The module has no module-level *constants* — token
sets are inlined into the ``in`` checks where they are used — so adding
a new acceptance token requires touching exactly one site.

Three helpers cover the dialog grammar the legacy
``UserInteraction``/``helpers.get_user_input`` pair imposed:

* :func:`ask_text` — non-empty free text, re-prompts on empty input.
* :func:`ask_choice` — a numbered menu with the abort/quit option
  expected as the last entry; 1-indexed result; re-prompts on
  out-of-range input. Each option also accepts a single-letter alias
  (case-insensitive, unique within a menu) — derived deterministically
  from the label or supplied explicitly per option per constitution
  Principle III v1.4.0.
* :func:`ask_confirm` — three-token grammar: ``1``/``y``/``yes`` returns
  ``True``; ``2``/``n``/``no`` returns ``False``; anything else returns
  ``False`` and emits a logger warning. Matches the legacy behaviour.
  :class:`rich.prompt.Confirm` is *not* used because its built-in
  ``y``/``n`` grammar does not accept the legacy ``1``/``2`` numeric
  pair.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence

from rich.prompt import Prompt


def ask_text(prompt: str) -> str:
    """Read a non-empty string from the user, re-prompting on empty input.

    Args:
        prompt: The question shown to the user.

    Returns:
        The first non-empty response.
    """
    while True:
        value = Prompt.ask(prompt)
        if value:
            return value


def ask_choice(
    prompt: str,
    options: Sequence[str | tuple[str, str | None]],
) -> int:
    """Print a numbered menu of ``options`` and return the 1-indexed selection.

    Each option also accepts a single-letter alias in addition to its
    1-indexed digit. Aliases are derived deterministically (parenthesised
    single-alpha char wins, otherwise first alpha char of the label,
    lower-cased) — see constitution Principle III v1.4.0.

    Each entry of ``options`` is one of:

    * ``str`` — alias auto-derived from the label (two-step rule below).
    * ``(label, alias)`` — explicit single-char alias, overrides
      auto-derivation.
    * ``(label, None)`` — option is digit-only; no alias matches it.

    Two-step alias auto-derivation for plain-``str`` options:

    1. If the label contains exactly one substring matching
       ``(<single-alphabetic-char>)``, the alias is that char
       (lower-cased) — e.g. ``"(Q)uit"`` → ``q``,
       ``"Search by (a)uthor"`` → ``a``.
    2. Otherwise, the alias is the first alphabetic character of the
       label, lower-cased — e.g. ``"papers"`` → ``p``.

    Plain-``str`` options whose label has neither a parenthesised
    single-alpha char nor any alphabetic char at all are rejected with
    :class:`ValueError` at construction time.

    Args:
        prompt: The question presented after the numbered list.
        options: Menu entries; index 0 is shown as ``"1)"``, index 1 as
            ``"2)"``, and so on. See above for the per-entry shape.

    Returns:
        The 1-indexed selection.

    Raises:
        ValueError: If ``options`` is empty, an explicit alias has length
            ≠ 1, two non-``None`` aliases collide (case-insensitive), or
            a plain-``str`` option's label is non-derivable.
    """
    if not options:
        msg = "ask_choice requires at least one option"
        raise ValueError(msg)

    parens_pattern = re.compile(r"\(([A-Za-z])\)")

    labels: list[str] = []
    aliases: list[str | None] = []
    for entry in options:
        if isinstance(entry, tuple):
            label, alias = entry
            if alias is not None and len(alias) != 1:
                msg = f"alias must be a single character, got {alias!r}"
                raise ValueError(msg)
            aliases.append(alias.lower() if alias is not None else None)
            labels.append(label)
            continue
        # plain str — auto-derive
        match = parens_pattern.search(entry)
        if match is not None:
            aliases.append(match.group(1).lower())
        else:
            first_alpha = next((c for c in entry if c.isalpha()), None)
            if first_alpha is None:
                msg = (
                    f"cannot auto-derive alias from label {entry!r}; "
                    "supply an explicit alias or None"
                )
                raise ValueError(msg)
            aliases.append(first_alpha.lower())
        labels.append(entry)

    seen: dict[str, str] = {}
    for label, alias in zip(labels, aliases, strict=True):
        if alias is None:
            continue
        if alias in seen:
            msg = f"alias collision on menu: {alias!r} resolves both {seen[alias]!r} and {label!r}"
            raise ValueError(msg)
        seen[alias] = label

    for idx, (label, alias) in enumerate(zip(labels, aliases, strict=True), start=1):
        rendered = _render_label(label, alias, parens_pattern)
        print(f"{idx}) {rendered}")

    valid: list[str] = [str(i) for i in range(1, len(options) + 1)]
    for alias in aliases:
        if alias is None:
            continue
        valid.extend([alias, alias.upper()])

    response = Prompt.ask(prompt, choices=valid, show_choices=False)
    if response.isdigit():
        return int(response)
    lowered = response.lower()
    for idx, alias in enumerate(aliases, start=1):
        if alias == lowered:
            return idx
    # Should be unreachable — Prompt.ask validates against `choices`.
    msg = f"unexpected response {response!r} not in {valid}"
    raise ValueError(msg)


def _render_label(label: str, alias: str | None, parens_pattern: re.Pattern[str]) -> str:
    """Render an option's display string, inserting parens around the alias.

    Labels already containing a parenthesised single-alpha char render
    verbatim. Plain labels with an auto-derived alias get the alias
    char wrapped in parens at its first occurrence in the label.
    Options with ``alias=None`` render verbatim with no insertion.
    """
    if alias is None:
        return label
    if parens_pattern.search(label) is not None:
        return label
    # Plain label — wrap the alias char on its first occurrence
    # (case-insensitive). The alias was derived as the first alpha
    # char of the label, so it always matches exactly one position.
    for i, char in enumerate(label):
        if char.lower() == alias:
            return f"{label[:i]}({char}){label[i + 1 :]}"
    # Alias does not appear in the label (caller-supplied non-leading
    # alias on a label that doesn't contain the char). Prepend it.
    return f"({alias}) {label}"


def ask_confirm(prompt: str) -> bool:
    """Read a confirmation accepting the legacy numeric/word grammar.

    ``1``/``y``/``yes`` (any case) returns ``True``; ``2``/``n``/``no``
    returns ``False``; anything else returns ``False`` and logs a
    warning, matching the legacy ``UserInteraction`` confirmation
    behaviour exactly.

    Args:
        prompt: The question shown to the user.

    Returns:
        ``True`` for affirmative tokens, ``False`` otherwise.
    """
    response = Prompt.ask(prompt).strip().lower()
    if response in {"1", "y", "yes"}:
        return True
    if response in {"2", "n", "no"}:
        return False
    logging.getLogger(__name__).warning("Unrecognised confirmation %r — treating as 'no'", response)
    return False
