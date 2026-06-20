"""User-facing prompt helpers — the sole importer of ``rich.prompt``.

All interactive input in the application routes through these helpers so the
prompt grammar (non-empty re-prompt, 1-indexed menus with a mandatory abort,
dual-form confirmation) is uniform (constitution Principle III).
"""

from __future__ import annotations

from rich.console import Console
from rich.prompt import Prompt

from paper_sorts.db.repositories import PaperSummary

_console = Console()


def ask_text(prompt: str) -> str:
    """Prompt for free text, re-prompting until the user enters something.

    :param prompt: the message shown to the user.
    :returns: the non-empty, stripped answer.
    """
    while True:
        answer = Prompt.ask(prompt).strip()
        if answer:
            return answer


def ask_choice(prompt: str, options: list[str]) -> int | None:
    """Show a 1-indexed menu with a trailing abort option; return the index.

    The abort option is always appended as the final entry. Out-of-range or
    non-numeric input re-prompts.

    :param prompt: the header shown above the options.
    :param options: the selectable options (the abort entry is added).
    :returns: the 0-based index of the chosen option, or ``None`` if aborted.
    """
    abort_number = len(options) + 1
    while True:
        _console.print(prompt)
        for i, option in enumerate(options, start=1):
            _console.print(f"{i}) {option}")
        _console.print(f"{abort_number}) abort")
        raw = Prompt.ask("Your choice").strip().lower()
        if raw in {"abort", "q", "quit", str(abort_number)}:
            return None
        try:
            chosen = int(raw)
        except ValueError:
            continue
        if 1 <= chosen <= len(options):
            return chosen - 1


def confirm(summary: str) -> bool:
    """Confirm a destructive change, accepting numeric and word forms.

    :param summary: a one-line description of the exact change.
    :returns: ``True`` to proceed, ``False`` to cancel.
    """
    _console.print(summary)
    while True:
        raw = Prompt.ask("Proceed? 1) (Y)es  2) (N)o").strip().lower()
        if raw in {"1", "y", "yes"}:
            return True
        if raw in {"2", "n", "no"}:
            return False


def print_paper(paper: PaperSummary) -> None:
    """Pretty-print a resolved paper in the legacy format.

    :param paper: the summary to display.
    """
    _console.print(f"title: {paper.title}")
    _console.print(f"authors: {' and '.join(paper.authors)}")
    _console.print(f"summary: {paper.summary}")
    _console.print(f"bib entry: {paper.bibtex}")


def info(message: str) -> None:
    """Print a short plain-language message to the user."""
    _console.print(message)
