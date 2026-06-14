"""User-facing prompt helpers for paper_sorts CLI.

This is the ONLY module under ``src/paper_sorts/`` permitted to call
``input()``, ``rich.prompt.Prompt.ask``, or ``typer.prompt``.  All other
modules must route prompts through this module.

Constitution Principle III:
    - All menus are 1-indexed.
    - Every menu includes an explicit abort/quit option.
    - Destructive operations present a confirmation summary.
    - Confirmation accepts both numeric (``1``/``2``) and word (``y``/``n``) forms.
    - Empty input re-prompts indefinitely.
"""

from __future__ import annotations

from rich.console import Console
from rich.prompt import Prompt

_console = Console()


def ask_str(prompt: str) -> str:
    """Prompt the user for a non-empty string, re-prompting on empty input.

    :param prompt: instruction text shown to the user.
    :returns: the user's non-empty input string.
    """
    while True:
        value = Prompt.ask(prompt)
        if value.strip():
            return value.strip()
        _console.print("[yellow]Input cannot be empty. Please try again.[/yellow]")


def ask_choice(title: str, options: list[str], include_quit: bool = True) -> int | None:
    """Display a numbered menu and return the 1-based index of the chosen option.

    The menu always includes a quit/abort option as the last entry (unless
    ``include_quit=False``).

    :param title: heading text for the menu.
    :param options: list of option labels (without numbers).
    :param include_quit: if ``True`` (default), append a ``(Q)uit / abort`` entry.
    :returns: 1-based integer index of the selected option, or ``None`` if the
        user chose the quit/abort option.
    :raises ValueError: never raised; invalid input simply re-prompts.
    """
    all_options = list(options)
    if include_quit:
        all_options.append("(Q)uit / abort")

    while True:
        _console.print(f"\n[bold]{title}[/bold]")
        for i, opt in enumerate(all_options, start=1):
            _console.print(f"  {i}) {opt}")

        raw = Prompt.ask("Your choice").strip().lower()

        # Accept "q" or "quit" as the quit option
        if include_quit and raw in ("q", "quit", "abort"):
            return None
        # Try numeric
        try:
            index = int(raw)
        except ValueError:
            _console.print("[yellow]Please enter a valid number.[/yellow]")
            continue

        if include_quit and index == len(all_options):
            return None
        if 1 <= index <= len(all_options) - (1 if include_quit else 0):
            return index
        _console.print(
            f"[yellow]Please choose a number between 1 and {len(all_options)}.[/yellow]"
        )


def ask_confirm(prompt: str) -> bool:
    """Prompt the user for a yes/no confirmation.

    Accepts both numeric (``1``/``2``) and word (``y``/``n``/``yes``/``no``) forms.
    Re-prompts on unrecognised input.

    :param prompt: instruction text summarising the change to confirm.
    :returns: ``True`` if the user confirmed, ``False`` otherwise.
    """
    _console.print(f"\n{prompt}")
    while True:
        raw = Prompt.ask("  1) (Y)es  2) (N)o  —  Your choice").strip().lower()
        match raw:
            case "1" | "y" | "yes":
                return True
            case "2" | "n" | "no":
                return False
            case _:
                _console.print(
                    "[yellow]Please enter 1/y/yes or 2/n/no.[/yellow]"
                )


def print_paper(title: str, authors: list[str], contents: str, bibtex: str) -> None:
    """Pretty-print a paper record to the console.

    :param title: publication title.
    :param authors: list of author name strings.
    :param contents: one-sentence summary.
    :param bibtex: full BibTeX source string.
    """
    author_str = " and ".join(authors) if authors else "(no authors)"
    _console.print(f"\n[bold]title:[/bold] {title}")
    _console.print(f"[bold]authors:[/bold] {author_str}")
    _console.print(f"[bold]summary:[/bold] {contents}")
    _console.print(f"[bold]bib entry:[/bold]\n{bibtex}\n")
