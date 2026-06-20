"""User-facing prompt functions for paper_sorts CLI.

This is the ONLY module in paper_sorts allowed to call ``input()`` or
``rich.prompt.Prompt.ask``.  All CLI subcommands must route prompts through
the functions here.

Constitution Principle III: bare ``input()``, ``typer.prompt``, or
``Prompt.ask`` outside this file are a violation.
"""

import logging

logger = logging.getLogger(__name__)


def ask_input(prompt: str, allow_empty: bool = False) -> str:
    """Prompt the user for a non-empty string, re-prompting if empty.

    :param prompt: The prompt text shown to the user.
    :param allow_empty: When ``True``, an empty answer is accepted.
        Default is ``False`` (re-prompt on empty per legacy ``get_user_input``
        behaviour).
    :returns: The user's answer (stripped of leading/trailing whitespace).
    """
    while True:
        try:
            answer = input(prompt).strip()
        except EOFError:
            # Non-interactive context (e.g. pipe) — treat as empty / quit
            return ""
        if answer or allow_empty:
            return answer
        # Re-prompt on empty


def ask_choice(
    items: list[str],
    prompt: str = "Your choice: ",
    quit_label: str | None = "(Q)uit",
) -> int:
    """Display a 1-indexed numbered list and return the 0-based index chosen.

    Always appends a quit option as the last item.

    :param items: List of display strings for the options.
    :param prompt: Text shown before the input cursor.
    :param quit_label: Label for the implicit quit option.  Pass ``None`` to
        suppress the quit option (rarely correct — prefer the default).
    :returns: 0-based index of the chosen item, or ``-1`` if the user chose
        quit.
    :raises ValueError: Should not be raised; invalid input triggers re-prompt.
    """
    all_items = list(items)
    quit_index = len(all_items) + 1
    while True:
        for i, item in enumerate(all_items, start=1):
            print(f"{i}) {item}")
        if quit_label:
            print(f"{quit_index}) {quit_label}")

        raw = input(prompt).strip().lower()

        # Accept "q" or "quit" as quit
        if quit_label and raw in ("q", "quit", str(quit_index)):
            return -1

        try:
            choice = int(raw)
        except ValueError:
            print("Please enter a valid number.")
            continue

        if 1 <= choice <= len(all_items):
            return choice - 1

        print(f"Please choose a number between 1 and {len(all_items)}.")


def ask_confirmation(message: str) -> bool:
    """Ask the user to confirm a destructive action.

    Accepts both numeric (``1``/``2``) and word (``y``/``n``/``yes``/``no``) forms.

    :param message: Description of the change to confirm.  Printed before the
        confirmation prompt.
    :returns: ``True`` if the user confirms, ``False`` if they decline.
    """
    print(message)
    while True:
        raw = input("Proceed?\n1) (Y)es\n2) (N)o\nYour choice: ").strip().lower()
        match raw:
            case "1" | "y" | "yes":
                return True
            case "2" | "n" | "no":
                return False
            case _:
                print("Please enter 1, 2, y, or n.")
