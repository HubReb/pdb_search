"""Centralised user-facing prompt helpers for paper_sorts CLI.

This is the **only** module under ``src/paper_sorts/`` permitted to import
``rich.prompt``.  All interactive input in the CLI layer must route through
functions defined here.

Functions:
    :func:`ask_nonempty` — prompt until non-empty input received.
    :func:`ask_choice` — numbered menu; validates range; re-prompts on invalid input.
    :func:`ask_confirm` — yes/no confirmation; accepts numeric (1/2) and word forms.
    :func:`ask_search_method` — prompt the user to choose "author" or "title".
"""

from __future__ import annotations

from rich.prompt import Prompt

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def ask_nonempty(prompt: str) -> str:
    """Prompt the user until they enter a non-empty string.

    :param prompt: Instruction text shown to the user.
    :return: The non-empty string entered by the user.
    """
    while True:
        value = Prompt.ask(prompt)
        if value.strip():
            return value.strip()
        # Rich already printed the prompt; print inline rejection message.
        print("Input cannot be empty. Please try again.")


def ask_choice(options: list[str], prompt: str = "Your choice") -> int:
    """Display a 1-indexed numbered menu and return the validated choice index.

    :param options: List of option strings to display.  Must be non-empty.
    :param prompt: Prompt text shown after the menu.
    :return: 1-based index of the chosen option (i.e. 1 ≤ return value ≤ len(options)).
    :raises ValueError: If *options* is empty.
    """
    if not options:
        raise ValueError("ask_choice requires at least one option.")
    menu = "\n".join(f"{i + 1}) {opt}" for i, opt in enumerate(options))
    while True:
        print(menu)
        raw = Prompt.ask(prompt)
        try:
            choice = int(raw.strip())
        except ValueError:
            print(f"Please enter a number between 1 and {len(options)}.")
            continue
        if 1 <= choice <= len(options):
            return choice
        print(f"Please enter a number between 1 and {len(options)}.")


def ask_confirm(summary: str) -> bool:
    """Ask the user to confirm or abort an action.

    Accepts both numeric (``1``/``2``) and word (``y``/``yes``/``n``/``no``) input.

    :param summary: Description of the action to be confirmed, displayed before the
        confirmation prompt.
    :return: ``True`` if the user confirmed; ``False`` if they declined.
    """
    print(summary)
    while True:
        raw = Prompt.ask("Proceed? (1/y/yes to confirm, 2/n/no to abort)").strip().lower()
        if raw in ("1", "y", "yes"):
            return True
        if raw in ("2", "n", "no"):
            return False
        print("Please enter 1/y/yes to confirm or 2/n/no to abort.")


def ask_search_method() -> str:
    """Ask the user whether to search by author or by title.

    :return: Either ``"author"`` or ``"title"``.
    """
    choice = ask_choice(["Search by author", "Search by paper title"], "Search interface")
    return "author" if choice == 1 else "title"
