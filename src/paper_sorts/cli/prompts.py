"""User-facing prompt helpers — the only module that imports ``rich.prompt``.

Per the constitution's UX principle, every interactive prompt in the application
routes through this module. The three helpers preserve the legacy grammar:
non-empty re-prompting, 1-indexed menus with a mandatory abort option, and
confirmations that accept both numeric and word forms.
"""

from __future__ import annotations

from rich.console import Console
from rich.prompt import Prompt

_console = Console()

#: Sentinel returned by :func:`ask_choice` when the user chooses to abort.
ABORT = "__abort__"


def ask_text(prompt: str) -> str:
    """Prompt for non-empty text, re-prompting until something is entered.

    :param prompt: the message shown to the user.
    :return: the user's non-empty answer (surrounding whitespace stripped).
    """
    while True:
        answer = Prompt.ask(prompt, console=_console).strip()
        if answer:
            return answer


def ask_choice(prompt: str, options: list[str], *, abort_label: str = "abort") -> int | str:
    """Show a 1-indexed menu with a mandatory abort option and return the choice.

    :param prompt: the heading shown above the options.
    :param options: the selectable option labels, in display order.
    :param abort_label: label for the always-present final abort option.
    :return: the 0-based index of the chosen option, or :data:`ABORT` if the
        user selected the abort option.
    """
    abort_number = len(options) + 1
    while True:
        lines = [prompt]
        for index, label in enumerate(options, start=1):
            lines.append(f"{index}) {label}")
        lines.append(f"{abort_number}) {abort_label}")
        _console.print("\n".join(lines))
        raw = Prompt.ask("Your choice", console=_console).strip()
        try:
            number = int(raw)
        except ValueError:
            continue
        if number == abort_number:
            return ABORT
        if 1 <= number <= len(options):
            return number - 1


def confirm(prompt: str) -> bool:
    """Ask a yes/no confirmation accepting numeric and word forms.

    Accepts ``1``/``y``/``yes`` as yes and ``2``/``n``/``no`` as no. Any other
    input re-prompts.

    :param prompt: the confirmation message.
    :return: ``True`` if confirmed, ``False`` if declined.
    """
    while True:
        raw = Prompt.ask(f"{prompt} [1) (Y)es / 2) (N)o]", console=_console).strip().lower()
        if raw in {"1", "y", "yes"}:
            return True
        if raw in {"2", "n", "no"}:
            return False
