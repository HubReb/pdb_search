"""User-facing prompt helpers for paper_sorts CLI.

This is the ONLY module under src/paper_sorts/ permitted to import
rich.prompt (constitution Principle III). All user-facing prompts
MUST route through this module — bare input(), bare typer.prompt,
and bare rich.prompt.Prompt.ask calls anywhere else are violations.

Invariants enforced here:
  - Non-empty re-prompt: ask_nonempty loops until the user types something.
  - 1-indexed menus: ask_menu displays 1..N options and returns a 0-based index.
  - Abort always available: callers are responsible for including a quit/abort
    option in their menu_items list.
  - Confirmation dual form: ask_confirm accepts 1/y/yes (True) and 2/n/no (False).
"""

from __future__ import annotations

from rich.prompt import Prompt


def ask_nonempty(prompt_text: str) -> str:
    """Prompt the user until they enter a non-empty string.

    :param prompt_text: The prompt string shown to the user.
    :return: Non-empty user input (stripped of leading/trailing whitespace).
    """
    while True:
        response = Prompt.ask(prompt_text)
        stripped = response.strip()
        if stripped:
            return stripped
        Prompt.ask("[yellow]Input cannot be empty. Please try again[/yellow]")


def ask_choice(prompt_text: str, options: list[str]) -> int:
    """Display a numbered menu and return the 0-based index of the chosen item.

    Displays options as a 1-indexed list, re-prompts on invalid input.

    :param prompt_text: The prompt string shown above the menu.
    :param options: List of option strings to display.
    :return: 0-based index of the selected option.
    :raises ValueError: If options list is empty.
    """
    if not options:
        raise ValueError("Options list must not be empty.")
    menu_lines = "\n".join(f"  {i + 1}) {opt}" for i, opt in enumerate(options))
    while True:
        response = Prompt.ask(f"{prompt_text}\n{menu_lines}\nYour choice")
        try:
            idx = int(response.strip()) - 1
            if 0 <= idx < len(options):
                return idx
        except ValueError:
            pass
        Prompt.ask(
            f"[yellow]Please enter a number between 1 and {len(options)}. "
            "Press Enter to retry[/yellow]"
        )


def ask_menu(prompt_text: str, options: list[str]) -> int:
    """Display a numbered menu and return the 0-based index of the chosen item.

    Identical to ask_choice but intended for top-level menus where the
    caller has already included an explicit quit/abort option.

    :param prompt_text: The prompt string shown above the menu.
    :param options: List of option strings (must include a quit/abort option).
    :return: 0-based index of the selected option.
    """
    return ask_choice(prompt_text, options)


def ask_confirm(prompt_text: str) -> bool:
    """Ask for a yes/no confirmation.

    Accepts both numeric (1/2) and word (y/yes/n/no) forms, case-insensitive.
    Re-prompts on unrecognised input.

    :param prompt_text: The prompt string summarising the action to confirm.
    :return: True if the user confirmed (yes/1), False if declined (no/2).
    """
    while True:
        response = Prompt.ask(
            f"{prompt_text}\n  1) (Y)es\n  2) (N)o\nYour choice"
        ).strip().lower()
        if response in ("1", "y", "yes"):
            return True
        if response in ("2", "n", "no"):
            return False
        Prompt.ask(
            "[yellow]Please enter 1/yes/y to confirm or 2/no/n to cancel. "
            "Press Enter to retry[/yellow]"
        )
