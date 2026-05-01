"""Rich-backed user-prompt wrappers (constitution Principle III v1.3.0 boundary).

This is the only module under ``src/paper_sorts/`` permitted to import
:mod:`rich.prompt`. All imports — and the token sets used by
:func:`ask_confirm` — live inside the functions that need them, so
importing this module has no side effects whatsoever. Useful when the
surrounding test harness wants to swap ``rich`` out before any wrapper
runs.

Three helpers cover the dialog grammar the legacy
``UserInteraction``/``helpers.get_user_input`` pair imposed:

* :func:`ask_text` — non-empty free text, re-prompts on empty input.
* :func:`ask_choice` — a numbered menu with the abort/quit option
  expected as the last entry; 1-indexed result; re-prompts on
  out-of-range input via :class:`IntPrompt`'s ``choices=`` validation.
* :func:`ask_confirm` — three-token grammar: ``1``/``y``/``yes`` returns
  ``True``; ``2``/``n``/``no`` returns ``False``; anything else returns
  ``False`` and emits a logger warning. Matches the legacy behaviour.
  :class:`rich.prompt.Confirm` is *not* used because its built-in
  ``y``/``n`` grammar does not accept the legacy ``1``/``2`` numeric
  pair.
"""


def ask_text(prompt: str) -> str:
    """Read a non-empty string from the user, re-prompting on empty input.

    Args:
        prompt: The question shown to the user.

    Returns:
        The first non-empty response.
    """
    from rich.prompt import Prompt

    while True:
        value = Prompt.ask(prompt)
        if value:
            return value


def ask_choice(
    prompt: str,
    options: list[str],
    *,
    quit_alias: str | None = None,
) -> int:
    """Print a numbered menu of ``options`` and return the 1-indexed selection.

    The caller is responsible for putting the abort/quit option as the last
    entry. Out-of-range and non-integer responses are rejected by
    :class:`IntPrompt`'s ``choices=`` validation (or :class:`Prompt`'s when
    a ``quit_alias`` is set), which re-prompts.

    Args:
        prompt: The question presented after the numbered list.
        options: Menu entries; index 0 is shown as ``"1)"``, index 1 as
            ``"2)"``, and so on.
        quit_alias: Optional keyword-only single character (e.g. ``"q"``)
            accepted case-insensitively as a shortcut for the *last*
            option, satisfying the contract's "``q`` is accepted in
            addition to ``4``" rule for the top-level menu.

    Returns:
        The 1-indexed selection.

    Raises:
        ValueError: If ``options`` is empty.
    """
    from rich.prompt import IntPrompt, Prompt

    if not options:
        msg = "ask_choice requires at least one option"
        raise ValueError(msg)
    for i, opt in enumerate(options, start=1):
        print(f"{i}) {opt}")
    n = len(options)
    if quit_alias is None:
        return IntPrompt.ask(prompt, choices=[str(i) for i in range(1, n + 1)])

    aliases = {quit_alias.lower(), quit_alias.upper()}
    valid = [str(i) for i in range(1, n + 1)] + sorted(aliases)
    response = Prompt.ask(prompt, choices=valid, show_choices=False)
    if response in aliases:
        return n
    return int(response)


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
    import logging

    from rich.prompt import Prompt

    response = Prompt.ask(prompt).strip().lower()
    if response in {"1", "y", "yes"}:
        return True
    if response in {"2", "n", "no"}:
        return False
    logging.getLogger(__name__).warning("Unrecognised confirmation %r — treating as 'no'", response)
    return False
