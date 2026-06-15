"""User-facing prompt helpers — the single prompt-routing seam.

This is the **only** module under ``src/paper_sorts/`` permitted to import
``rich.prompt`` (constitution Principle III). Every menu, confirmation, and
free-text prompt in the CLI routes through here so the prompt grammar stays
uniform: 1-indexed menus, a mandatory abort/quit option, empty-input
re-prompting, out-of-range re-prompting, and dual-form (numeric/word)
confirmations.
"""

from __future__ import annotations

from rich.console import Console
from rich.prompt import Prompt

from paper_sorts.db.repositories import PaperSummary

_console = Console()


def ask_nonempty(prompt: str) -> str:
    """Prompt until the user enters a non-empty value.

    Mirrors the legacy ``get_user_input``: pressing Enter on a required prompt
    re-prompts rather than accepting an empty string.

    :param prompt: the instruction shown to the user.
    :returns: the non-empty user input.
    """
    while True:
        answer = Prompt.ask(prompt, console=_console).strip()
        if answer:
            return answer


def ask_choice(prompt: str, options: list[str], *, abort_label: str = "abort") -> int | None:
    """Show a 1-indexed menu with a mandatory abort option.

    :param prompt: header text shown above the options.
    :param options: the selectable option labels (1-indexed in display).
    :param abort_label: label for the always-present final abort option.
    :returns: the 0-based index of the chosen option, or ``None`` if the user
        aborted.
    """
    abort_number = len(options) + 1
    while True:
        _console.print(prompt)
        for index, label in enumerate(options, start=1):
            _console.print(f"{index}) {label}")
        _console.print(f"{abort_number}) {abort_label}")
        raw = Prompt.ask("Your choice", console=_console).strip().lower()
        if raw in {abort_label, "q", "quit", str(abort_number)}:
            return None
        if raw.isdigit():
            picked = int(raw)
            if 1 <= picked <= len(options):
                return picked - 1
        # Out-of-range or unparseable: re-prompt.


def ask_confirm(summary: str) -> bool:
    """Confirm a destructive change, accepting both numeric and word forms.

    :param summary: a one-line description of the exact change to apply.
    :returns: ``True`` to proceed, ``False`` to cancel.
    """
    while True:
        _console.print(summary)
        raw = Prompt.ask("Proceed? 1) (Y)es  2) (N)o", console=_console).strip().lower()
        if raw in {"1", "y", "yes"}:
            return True
        if raw in {"2", "n", "no"}:
            return False
        # Unrecognised: re-prompt.


def pick_from(prompt: str, papers: list[PaperSummary]) -> PaperSummary | None:
    """Disambiguate among multiple papers via a 1-indexed numbered list.

    Out-of-range selections re-prompt; an abort option is always present.

    :param prompt: header text shown above the list.
    :param papers: the candidate papers.
    :returns: the chosen paper, or ``None`` if the user aborted.
    """
    labels = [f"title: {paper.title}" for paper in papers]
    index = ask_choice(prompt, labels)
    if index is None:
        return None
    return papers[index]


def display_paper(paper: PaperSummary) -> None:
    """Print a paper in the legacy pretty-print format.

    :param paper: the paper to display.
    """
    _console.print(f"title: {paper.title}")
    _console.print(f"authors: {paper.authors}")
    _console.print(f"summary: {paper.summary}")
    _console.print(f"bib entry: {paper.bibtex}")


def info(message: str) -> None:
    """Print a short plain-language message to the user.

    :param message: the message text.
    """
    _console.print(message)
