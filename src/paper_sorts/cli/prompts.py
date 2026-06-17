"""User-facing prompt helpers — the only module permitted to import ``rich``.

All interactive input routes through here (constitution Principle III). Helpers
enforce the project's prompt grammar: non-empty re-prompting, 1-indexed numbered
choices with an explicit abort, and confirmations that accept both numeric
(``1``/``2``) and word (``y``/``n``/``yes``/``no``) forms.
"""

from __future__ import annotations

from collections.abc import Sequence

from rich.console import Console
from rich.prompt import Prompt

console = Console()


def ask_nonempty(prompt: str) -> str:
    """Prompt until the user provides non-empty (non-whitespace) input.

    :param prompt: the message to display.
    :returns: the trimmed, non-empty user input.
    """
    while True:
        answer = Prompt.ask(prompt).strip()
        if answer:
            return answer


def ask_text(prompt: str) -> str:
    """Prompt once for free-form text, returning it trimmed.

    :param prompt: the message to display.
    :returns: the trimmed user input (may be empty).
    """
    return Prompt.ask(prompt).strip()


def ask_choice(prompt: str, options: Sequence[str]) -> int | None:
    """Present a 1-indexed numbered menu with an abort option.

    :param prompt: the heading to display above the options.
    :param options: the selectable option labels (abort is appended).
    :returns: the 0-based index of the chosen option, or ``None`` if aborted.
    """
    abort_index = len(options) + 1
    while True:
        console.print(prompt)
        for i, label in enumerate(options, start=1):
            console.print(f"{i}) {label}")
        console.print(f"{abort_index}) abort")
        raw = Prompt.ask("Your choice").strip().lower()
        if raw in {"abort", "q", str(abort_index)}:
            return None
        if raw.isdigit():
            value = int(raw)
            if 1 <= value <= len(options):
                return value - 1
        console.print("Please choose a valid option.")


def ask_pick(prompt: str, labels: Sequence[str]) -> int | None:
    """Disambiguation prompt: pick one item from a numbered list, or abort.

    :param prompt: the heading to display.
    :param labels: the item labels to choose among.
    :returns: the 0-based index chosen, or ``None`` if aborted.
    """
    return ask_choice(prompt, labels)


def confirm(prompt: str) -> bool:
    """Confirmation accepting ``1``/``2`` and ``y``/``n``/``yes``/``no``.

    :param prompt: the change summary to confirm.
    :returns: ``True`` to proceed, ``False`` to cancel (default on bad input).
    """
    console.print(prompt)
    raw = Prompt.ask("Proceed? 1) (Y)es  2) (N)o").strip().lower()
    return raw in {"1", "y", "yes"}
