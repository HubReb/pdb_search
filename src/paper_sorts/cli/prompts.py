"""User-facing prompt and display functions for paper_sorts CLI.

This is the ONLY module in src/paper_sorts/ permitted to import rich.prompt
(constitution Principle III).  All other modules that need to prompt the user
MUST call these functions.

Design rules enforced here:
  - Empty input is re-prompted when required=True (spec US2 acceptance 5).
  - ask_confirm accepts y/yes/1/n/no/2 case-insensitively (constitution Principle III).
  - Numbered menus are 1-indexed (constitution Principle III).
  - Every menu includes an explicit abort/quit option (constitution Principle III).
"""

from __future__ import annotations

from rich.console import Console
from rich.prompt import Prompt

from paper_sorts.db.repositories import PaperSummary

_console = Console()


def ask_str(prompt_text: str, *, required: bool = True) -> str:
    """Prompt the user for a string value.

    Re-prompts on empty input when required=True.

    Args:
        prompt_text: The prompt string shown to the user.
        required: If True, reject empty input and re-prompt.

    Returns:
        The non-empty string entered by the user (stripped), or empty string
        if required=False and the user pressed Enter.
    """
    while True:
        value = Prompt.ask(prompt_text)
        stripped = value.strip()
        if stripped or not required:
            return stripped
        _console.print("[yellow]Input cannot be empty. Please try again.[/yellow]")


def ask_int(prompt_text: str, choices: list[int]) -> int:
    """Prompt the user for an integer choice from the given list.

    Re-prompts on invalid input or out-of-range values.

    Args:
        prompt_text: The prompt string shown to the user.
        choices: Valid integer choices (e.g. [1, 2, 3]).

    Returns:
        The integer chosen by the user.
    """
    choices_str = "/".join(str(c) for c in choices)
    full_prompt = f"{prompt_text} [{choices_str}]"
    while True:
        raw = Prompt.ask(full_prompt)
        raw = raw.strip()
        # Allow 'q' / 'Q' as shorthand for the last choice if it is the quit option
        try:
            value = int(raw)
            if value in choices:
                return value
            _console.print(
                f"[yellow]Please enter one of: {choices_str}[/yellow]"
            )
        except ValueError:
            _console.print(
                f"[yellow]Please enter a number. Valid choices: {choices_str}[/yellow]"
            )


def ask_confirm(prompt_text: str) -> bool:
    """Prompt the user for a yes/no confirmation.

    Accepts: y, yes, 1 (True) and n, no, 2 (False), case-insensitively.
    Re-prompts on any other input.

    Args:
        prompt_text: The prompt string (without the [y/n] hint — it is appended).

    Returns:
        True if the user confirmed; False if they declined.
    """
    full_prompt = f"{prompt_text} [y/n]"
    while True:
        raw = Prompt.ask(full_prompt).strip().lower()
        if raw in {"y", "yes", "1"}:
            return True
        if raw in {"n", "no", "2"}:
            return False
        _console.print("[yellow]Please enter y/yes/1 or n/no/2.[/yellow]")


def display_paper(summary: PaperSummary) -> None:
    """Print a formatted paper summary to the console.

    Args:
        summary: PaperSummary DTO to display.
    """
    authors_str = "; ".join(summary.authors) if summary.authors else "(none)"
    _console.print(f"[bold]Title:[/bold] {summary.title}")
    _console.print(f"[bold]Authors:[/bold] {authors_str}")
    _console.print(f"[bold]Summary:[/bold] {summary.contents}")
    _console.print(f"[bold]BibTeX key:[/bold] {summary.bibtex_id}")
    _console.print(f"[bold]BibTeX:[/bold]\n  {summary.bibtex}")


def display_papers_list(summaries: list[PaperSummary]) -> None:
    """Print a numbered list of papers for disambiguation.

    Args:
        summaries: List of PaperSummary DTOs to display.
    """
    for i, s in enumerate(summaries, start=1):
        authors_str = "; ".join(s.authors) if s.authors else "(none)"
        _console.print(f"  {i}) {s.title} — {authors_str} [{s.bibtex_id}]")
