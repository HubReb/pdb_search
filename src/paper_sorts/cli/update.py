"""Update subcommand for paper_sorts CLI.

Allows the user to update the title, contents, BibTeX entry, or authors of
an existing paper.  Requires confirmation before writing (constitution
Principle III — destructive operations need confirmation).
"""

from __future__ import annotations

import logging

import typer
from rich.console import Console

from paper_sorts.cli.prompts import ask_confirm, ask_int, ask_str, display_paper
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service
from paper_sorts.services.paper_service import UpdateableField

logger = logging.getLogger(__name__)
app = typer.Typer(help="Update a field of an existing paper.")
_console = Console()


@app.callback(invoke_without_command=True)
def update_callback(
    ctx: typer.Context,
    paper_id: int | None = typer.Option(None, "--id", help="Paper ID to update"),
) -> None:
    """Run update flow when invoked as subcommand.

    Args:
        ctx: Typer context.
        paper_id: Optional paper ID; if absent, search first.
    """
    if ctx.invoked_subcommand is None:
        from paper_sorts.cli.app import get_database_url

        run_update(get_database_url(), paper_id=paper_id)


def run_update(database_url: str, paper_id: int | None = None) -> None:
    """Interactive update paper flow.

    Args:
        database_url: SQLAlchemy connection string.
        paper_id: If provided, update this paper directly; otherwise search first.
    """
    if paper_id is None:
        paper_id = _find_paper(database_url)
        if paper_id is None:
            return

    # Show current values
    try:
        from paper_sorts.db.repositories import PaperRepository

        with with_session(database_url) as session:
            repo = PaperRepository(session)
            current = repo.get_by_id(paper_id)
    except Exception:
        logger.exception("Failed to fetch paper id=%d", paper_id)
        _console.print("[red]Could not fetch paper. Check logs.[/red]")
        return

    if current is None:
        _console.print(f"[red]No paper with id={paper_id}.[/red]")
        return

    _console.print("\n[bold]Current values:[/bold]")
    display_paper(current)

    # Field selection submenu
    _console.print("\n[bold]What would you like to update?[/bold]")
    _console.print("1) Title")
    _console.print("2) Contents (summary)")
    _console.print("3) BibTeX entry")
    _console.print("4) Authors")
    _console.print("5) Abort")
    choice = ask_int("Choice", [1, 2, 3, 4, 5])
    if choice == 5:
        _console.print("Update aborted. No changes made.")
        return

    field_map: dict[int, UpdateableField] = {1: "title", 2: "contents", 3: "bibtex", 4: "authors"}
    field: UpdateableField = field_map[choice]

    author_list: list[str] = []
    new_value_str: str = ""
    display_value: str = ""
    if field == "authors":
        raw = ask_str("Enter new authors (comma-separated)")
        author_list = [a.strip() for a in raw.split(",") if a.strip()]
        display_value = ", ".join(author_list)
    else:
        new_value_str = ask_str(f"Enter new {field}")
        display_value = new_value_str

    confirmed = ask_confirm(f'Update {field} to "{display_value}"?')
    if not confirmed:
        _console.print("Update aborted. No changes made.")
        return

    update_value: str | list[str] = author_list if field == "authors" else new_value_str
    try:
        with with_session(database_url) as session:
            updated = paper_service.update_field(session, paper_id, field, update_value)
        _console.print("[green]Update successful.[/green]")
        display_paper(updated)
    except Exception as exc:
        logger.exception("update_field failed for paper id=%d field=%r", paper_id, field)
        _console.print(f"[red]Update failed: {exc}[/red]")


def _find_paper(database_url: str) -> int | None:
    """Search for a paper by title and return its id.

    Args:
        database_url: SQLAlchemy connection string.

    Returns:
        Paper id, or None if user aborted or no results found.
    """
    title = ask_str("Enter title to search for")
    try:
        with with_session(database_url) as session:
            results = paper_service.search_by_title(session, title)
    except Exception:
        logger.exception("search failed during update")
        _console.print("[red]Search failed. Check logs.[/red]")
        return None

    if not results:
        _console.print("No papers found.")
        return None

    if len(results) == 1:
        return results[0].id

    from paper_sorts.cli.prompts import display_papers_list

    display_papers_list(results)
    _console.print(f"{len(results) + 1}) Abort")
    choices = list(range(1, len(results) + 2))
    choice = ask_int("Select paper to update", choices)
    if choice == len(results) + 1:
        return None
    return results[choice - 1].id
