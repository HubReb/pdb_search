"""User-facing prompt helpers — the single entry point for all interactive input.

Per the constitution's UX-consistency principle, this is the **only** module under
``src/paper_sorts/`` permitted to read user input (``rich.prompt`` / ``input``). The helpers
enforce the prompt grammar: non-empty re-prompt, 1-indexed menus with an explicit abort option,
dual-form (numeric + word) confirmation, and numbered disambiguation with out-of-range
re-prompt.
"""

from __future__ import annotations

from collections.abc import Sequence

from rich.console import Console
from rich.prompt import Prompt

_console = Console()


def ask_nonempty(prompt: str) -> str:
    """Prompt until the user provides a non-empty (non-whitespace) answer.

    :param prompt: the instruction shown to the user.
    :return: the user's trimmed, non-empty answer.
    """
    while True:
        answer = Prompt.ask(prompt).strip()
        if answer:
            return answer


def ask_choice(prompt: str, options: Sequence[str]) -> int:
    """Show a 1-indexed menu and return the chosen index.

    The menu is rendered with options numbered from 1. Invalid or out-of-range input
    re-prompts.

    :param prompt: the heading shown above the options.
    :param options: the option labels, displayed 1-indexed.
    :return: the zero-based index of the chosen option.
    """
    _console.print(prompt)
    for i, option in enumerate(options, start=1):
        _console.print(f"{i}) {option}")
    valid = [str(i) for i in range(1, len(options) + 1)]
    while True:
        answer = Prompt.ask("Your choice").strip()
        if answer in valid:
            return int(answer) - 1


def confirm(summary: str) -> bool:
    """Ask the user to confirm an action, accepting numeric and word forms.

    Accepts ``1``/``y``/``yes`` for yes and ``2``/``n``/``no`` for no (case-insensitive).
    Anything else re-prompts.

    :param summary: a plain-language summary of the exact change to confirm.
    :return: ``True`` if confirmed, ``False`` if declined.
    """
    _console.print(summary)
    _console.print("1) (Y)es")
    _console.print("2) (N)o")
    while True:
        answer = Prompt.ask("Your choice").strip().lower()
        if answer in {"1", "y", "yes"}:
            return True
        if answer in {"2", "n", "no"}:
            return False


def pick_from(prompt: str, labels: Sequence[str]) -> int:
    """Disambiguate among labelled items, re-prompting on out-of-range input.

    :param prompt: the heading shown above the numbered items.
    :param labels: the item labels, displayed 1-indexed.
    :return: the zero-based index of the chosen item.
    """
    return ask_choice(prompt, labels)


def show(message: str) -> None:
    """Print a plain message to the user.

    :param message: the text to display.
    """
    _console.print(message)
