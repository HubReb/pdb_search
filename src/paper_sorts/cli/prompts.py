"""User-facing prompt helpers for paper_sorts CLI.

This is the ONLY module in src/paper_sorts/ permitted to use rich.prompt
or call input() directly. All other modules must call these helpers.

UX grammar (constitution Principle III):
- Menus are 1-indexed and always include an abort/quit option.
- Empty input on required prompts is re-prompted until non-empty.
- Confirmations accept numeric (1/2) and word (y/n/yes/no), case-insensitive.
- Failure messages are plain-language; technical detail goes to the logger.
"""

from __future__ import annotations

from rich.prompt import Prompt


def ask_text(prompt: str, allow_empty: bool = False) -> str:
    """Prompt the user for a text string.

    Re-prompts until a non-empty value is provided unless allow_empty is True.

    :param prompt: the prompt string shown to the user
    :param allow_empty: if True, return immediately even for empty input
    :return: the user's (non-empty if allow_empty=False) input string
    """
    while True:
        value = Prompt.ask(prompt)
        if value or allow_empty:
            return value
        Prompt.ask("[yellow]Input cannot be empty. Please try again[/yellow]\n" + prompt)


def ask_choice(prompt: str, options: list[str], allow_quit: bool = True) -> str | None:
    """Display a numbered menu and return the selected option string.

    Menus are 1-indexed (constitution Principle III). If allow_quit is True,
    an additional "Quit / abort" option is appended. Selecting it returns None.
    Re-prompts on invalid input.

    :param prompt: header text displayed above the menu
    :param options: list of option strings (displayed as 1-indexed)
    :param allow_quit: if True, append a "Quit / abort" option that returns None
    :return: the selected option string, or None if the user chose quit/abort
    """
    display_options = list(options)
    quit_index: int | None = None
    if allow_quit:
        quit_index = len(display_options) + 1
        display_options.append("Quit / abort")

    menu_lines = [prompt]
    for i, opt in enumerate(display_options, start=1):
        menu_lines.append(f"  {i}) {opt}")
    menu_text = "\n".join(menu_lines)

    while True:
        raw = Prompt.ask(menu_text)
        try:
            index = int(raw)
        except ValueError:
            Prompt.ask("[yellow]Please enter a number.[/yellow]\n" + menu_text)
            continue
        if index < 1 or index > len(display_options):
            Prompt.ask(
                f"[yellow]Please enter a number between 1 and {len(display_options)}.[/yellow]\n"
                + menu_text
            )
            continue
        if allow_quit and index == quit_index:
            return None
        return options[index - 1]


def ask_confirm(prompt: str) -> bool:
    """Ask a yes/no confirmation question.

    Accepts numeric (1/2) and word (y/n/yes/no) forms, case-insensitive.
    Re-prompts on unrecognised input.

    :param prompt: the question to display; the accepted inputs are appended
    :return: True for yes/1/y, False for no/2/n
    """
    full_prompt = f"{prompt}\n  1) (Y)es\n  2) (N)o"
    while True:
        raw = Prompt.ask(full_prompt).strip().lower()
        if raw in ("1", "y", "yes"):
            return True
        if raw in ("2", "n", "no"):
            return False
        Prompt.ask(
            "[yellow]Please enter 1/y/yes or 2/n/no.[/yellow]\n" + full_prompt
        )
