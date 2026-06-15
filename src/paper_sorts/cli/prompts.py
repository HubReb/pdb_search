"""All user-facing prompts and output.

This is the **only** module under ``paper_sorts`` permitted to call
``input``/``rich.prompt`` (Constitution Principle III). The grammar is uniform:
non-empty re-prompting, 1-indexed menus that always include an abort/quit
option, dual-form confirmations, and plain-language error output.
"""

from __future__ import annotations

from rich.console import Console
from rich.prompt import Prompt

from paper_sorts.db.repositories import PaperSummary

_console = Console()


def ask_text(prompt: str) -> str:
    """Prompt for free text, re-prompting until the input is non-empty.

    :param prompt: the message shown to the user.
    :return: the user's non-empty answer (whitespace-stripped).
    """
    while True:
        answer = Prompt.ask(prompt).strip()
        if answer:
            return answer


def ask_choice(prompt: str, options: list[str], abort_label: str = "abort") -> int | None:
    """Show a 1-indexed menu with an explicit abort option and return the choice.

    :param prompt: the menu header.
    :param options: the selectable option labels (1..N).
    :param abort_label: the label for the final abort/quit entry.
    :return: the 0-based index of the chosen option, or ``None`` if the user aborts.
    """
    abort_index = len(options) + 1
    while True:
        _console.print(prompt)
        for index, label in enumerate(options, start=1):
            _console.print(f"{index}) {label}")
        _console.print(f"{abort_index}) {abort_label}")
        raw = Prompt.ask("Your choice").strip().lower()
        if raw in {str(abort_index), "q", "abort"}:
            return None
        if raw.isdigit():
            value = int(raw)
            if 1 <= value <= len(options):
                return value - 1
        _console.print("Your input was invalid")


def confirm(prompt: str) -> bool:
    """Ask a yes/no confirmation accepting both numeric and word forms.

    :param prompt: the confirmation message (the caller summarises the change).
    :return: ``True`` for yes (``1``/``y``/``yes``), ``False`` otherwise.
    """
    _console.print(prompt)
    raw = Prompt.ask("1) (Y)es  2) (N)o").strip().lower()
    return raw in {"1", "y", "yes"}


def print_paper(paper: PaperSummary) -> None:
    """Pretty-print a paper hit (title, authors, summary, BibTeX entry).

    :param paper: the paper summary to display.
    """
    authors = " and ".join(paper.authors)
    _console.print(f"title: {paper.title}")
    _console.print(f"authors: {authors}")
    _console.print(f"summary: {paper.summary}")
    _console.print(f"bib entry: {paper.bibtex}")


def info(message: str) -> None:
    """Print a plain informational message.

    :param message: the text to show.
    """
    _console.print(message)


def error(message: str) -> None:
    """Print a short, plain-language error message (no traceback).

    :param message: the user-facing error text.
    """
    _console.print(message)
