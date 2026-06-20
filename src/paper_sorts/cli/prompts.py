"""User-facing prompt helpers for paper-sorts.

This is the **only** module under ``src/paper_sorts/`` permitted to import
``rich.prompt`` (constitution Principle III).  All user input must route
through these functions; bare ``input()`` calls anywhere else are a violation.

Functions
---------
:func:`ask_nonempty` — prompt that re-asks until non-empty input.
:func:`ask_choice` — 1-indexed numbered menu with range validation.
:func:`ask_confirm` — y/n confirmation that accepts word and numeric forms.
:func:`ask_optional` — prompt that accepts empty input (returns ``""``)
"""

from __future__ import annotations

import logging

from rich.prompt import Prompt

logger = logging.getLogger(__name__)


def ask_nonempty(prompt_text: str) -> str:
    """Prompt the user for input and re-ask until a non-empty string is given.

    Mirrors the legacy ``helpers.get_user_input`` behaviour: pressing Enter
    without typing anything causes the prompt to repeat.

    :param prompt_text: Instruction shown to the user.
    :returns: Non-empty string entered by the user.
    """
    while True:
        value = Prompt.ask(prompt_text)
        if value.strip():
            return value.strip()
        logger.debug("ask_nonempty: got empty input, re-prompting")


def ask_optional(prompt_text: str) -> str:
    """Prompt the user for input and return the value (may be empty).

    :param prompt_text: Instruction shown to the user.
    :returns: String entered by the user, possibly empty.
    """
    return Prompt.ask(prompt_text, default="")


def ask_choice(options: list[str], prompt_text: str = "Choose") -> int:
    """Present a 1-indexed numbered menu and return the chosen index (1-based).

    Repeats until the user enters a valid integer in the displayed range.

    :param options: List of option labels to display.  Must be non-empty.
    :param prompt_text: Prompt shown after the menu.
    :returns: 1-based integer index of the chosen option.
    :raises ValueError: If *options* is empty.
    """
    if not options:
        raise ValueError("ask_choice: options list must be non-empty")
    while True:
        for i, label in enumerate(options, start=1):
            print(f"  {i}) {label}")
        raw = Prompt.ask(prompt_text)
        try:
            choice = int(raw)
        except ValueError:
            print(f"Please enter a number between 1 and {len(options)}.")
            continue
        if 1 <= choice <= len(options):
            return choice
        print(f"Please enter a number between 1 and {len(options)}.")


def ask_confirm(action_desc: str) -> bool:
    """Ask the user to confirm a destructive action.

    Accepts ``y``, ``yes``, ``1`` to proceed and ``n``, ``no``, ``2`` to
    abort.  Case-insensitive.  Repeats until a valid response is given.

    :param action_desc: Human-readable description of the action to confirm,
        e.g. ``"Delete paper 'My Paper'?"``.
    :returns: ``True`` if the user confirmed, ``False`` if they aborted.
    """
    prompt_text = f"{action_desc} [y/n]"
    while True:
        raw = Prompt.ask(prompt_text).strip().lower()
        if raw in {"y", "yes", "1"}:
            return True
        if raw in {"n", "no", "2"}:
            return False
        print("Please enter y (yes) or n (no).")
