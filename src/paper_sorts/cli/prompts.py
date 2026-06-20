"""All user-facing prompt helpers for paper_sorts.

Per constitution Principle III, this is the **only** module under
``src/paper_sorts/`` that may call ``input()`` or ``rich.prompt``.  All other
CLI modules route user input through the functions defined here.

Functions
---------
- :func:`ask_str` — prompt for a non-empty string (re-prompts on empty input).
- :func:`ask_choice` — show a numbered menu, return the user's choice index
  (0-based).
- :func:`ask_confirm` — ask yes/no, accept ``1``/``2`` and ``y``/``n``/
  ``yes``/``no``.
"""

from __future__ import annotations


def ask_str(prompt: str) -> str:
    """Prompt the user for a non-empty string, re-prompting on empty input.

    Preserves the legacy ``get_user_input`` behaviour: pressing Enter on an
    empty line causes the prompt to repeat until the user provides input.

    :param prompt: The prompt text displayed to the user.
    :returns: The user's non-empty response string (stripped of leading/
        trailing whitespace).
    """
    while True:
        response = input(prompt).strip()
        if response:
            return response


def ask_choice(options: list[str], prompt: str = "Your choice: ") -> int:
    """Display a 1-indexed numbered menu and return the user's choice (0-based).

    The menu is printed once; on invalid input the prompt is repeated.
    The caller is responsible for including an abort/quit option in *options*
    to satisfy constitution Principle III.

    :param options: List of option labels, displayed as ``1) label``, ``2) label``, …
    :param prompt: The prompt shown after the menu.
    :returns: Zero-based index of the chosen option.
    :raises SystemExit: Never — loops until valid input is given.
    """
    for i, option in enumerate(options, start=1):
        print(f"{i}) {option}")
    while True:
        raw = input(prompt).strip()
        try:
            choice = int(raw)
        except ValueError:
            print(f"Please enter a number between 1 and {len(options)}.")
            continue
        if 1 <= choice <= len(options):
            return choice - 1
        print(f"Please enter a number between 1 and {len(options)}.")


def ask_confirm(message: str) -> bool:
    """Prompt the user to confirm an action.

    Accepts ``1``/``y``/``yes`` (case-insensitive) as affirmative and
    ``2``/``n``/``no`` as negative.  Any other input re-prompts.

    :param message: Description of the action to confirm (shown before the
        y/n prompt).
    :returns: ``True`` if the user confirmed; ``False`` otherwise.
    """
    print(message)
    while True:
        raw = input("1) (Y)es  2) (N)o\nYour choice: ").strip().lower()
        if raw in ("1", "y", "yes"):
            return True
        if raw in ("2", "n", "no"):
            return False
        print("Please enter 1, y, yes, 2, n, or no.")
