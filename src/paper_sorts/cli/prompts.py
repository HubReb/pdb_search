"""User-facing prompts for paper_sorts CLI.

This is the ONLY module in src/paper_sorts/ permitted to import rich.prompt.
All prompts (input, choices, confirmations, display) route through here.
"""

from rich.console import Console
from rich.prompt import Prompt

from paper_sorts.db.repositories import PaperSummary

_console = Console()
_err_console = Console(stderr=True)


def ask_text(prompt: str) -> str:
    """Prompt for a non-empty text value, re-prompting on empty input.

    :param prompt: Instruction displayed to the user.
    :returns: Non-empty string entered by the user.
    """
    while True:
        value = Prompt.ask(prompt)
        if value.strip():
            return value.strip()
        _err_console.print("[yellow]Please enter a non-empty value.[/yellow]")


def ask_choice(options: list[str], prompt: str = "Choose") -> int:
    """Display a 1-indexed numbered menu and return the chosen index (0-based).

    The last option is always an abort/quit choice. If the user selects it,
    returns ``len(options) - 1`` (the last index). Out-of-range input re-prompts.

    :param options: List of option labels. The last entry should be the abort option.
    :param prompt: Prompt text displayed after the menu.
    :returns: 0-based index of the chosen option.
    """
    while True:
        for i, option in enumerate(options, start=1):
            _console.print(f"  {i}) {option}")
        raw = Prompt.ask(prompt)
        try:
            choice = int(raw)
        except ValueError:
            _err_console.print("[yellow]Please enter a number.[/yellow]")
            continue
        if 1 <= choice <= len(options):
            return choice - 1
        _err_console.print(
            f"[yellow]Please enter a number between 1 and {len(options)}.[/yellow]"
        )


def ask_confirmation(summary: str) -> bool:
    """Present a summary of a destructive action and ask for confirmation.

    Accepts: y, yes, 1 (confirm) or n, no, 2 (abort). Case-insensitive.
    Re-prompts on unrecognised input.

    :param summary: Human-readable description of the change to be applied.
    :returns: ``True`` if the user confirms, ``False`` if they abort.
    """
    _console.print(f"\n[bold]About to:[/bold] {summary}")
    while True:
        raw = Prompt.ask("Confirm? [y/n/yes/no/1/2]").strip().lower()
        if raw in {"y", "yes", "1"}:
            return True
        if raw in {"n", "no", "2"}:
            return False
        _err_console.print("[yellow]Please enter y/yes/1 or n/no/2.[/yellow]")


def ask_bibtex_file() -> str | None:
    """Prompt for an optional .bib file path.

    :returns: Path string if the user entered one, or ``None`` for manual entry.
    """
    value = Prompt.ask("Enter path to .bib file (or leave blank to enter manually)", default="")
    return value.strip() if value.strip() else None


def pretty_print_paper(paper: PaperSummary) -> None:
    """Display a paper's metadata in a readable format.

    :param paper: :class:`~paper_sorts.db.repositories.PaperSummary` DTO to display.
    """
    authors = ", ".join(paper.authors) if paper.authors else "(no authors)"
    _console.print(f"\n[bold]Title:[/bold]   {paper.title}")
    _console.print(f"[bold]Authors:[/bold] {authors}")
    _console.print(f"[bold]Summary:[/bold] {paper.contents}")
    _console.print(f"[bold]BibTeX:[/bold]\n{paper.bibtex}\n")
