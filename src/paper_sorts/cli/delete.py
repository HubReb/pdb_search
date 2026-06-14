"""Delete subcommand for paper_sorts CLI.

Implements ``pdbsearch delete``: searches for a paper (by title), shows the
match, asks for confirmation, and deletes it.
"""

from __future__ import annotations

import logging

import typer
from rich.console import Console
from sqlalchemy.engine import Engine

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_nonempty
from paper_sorts.db.repositories import PaperSummary
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)
console = Console()

app = typer.Typer(help="Delete a paper from the database.")


def _display_summary(paper: PaperSummary) -> None:
    """Print a compact paper summary before the confirmation step.

    :param paper: :class:`~paper_sorts.db.repositories.PaperSummary` to display.
    """
    authors_str = " and ".join(paper.authors)
    console.print(f"  id={paper.id}  title={paper.title!r}  authors={authors_str!r}")


def run_delete(engine: Engine) -> None:
    """Interactively delete a paper from the database.

    Searches by title, shows the matching paper(s), asks the user to select
    one, then confirms before deleting.

    :param engine: Active SQLAlchemy engine.
    """
    title = ask_nonempty("Paper title to delete")
    results = paper_service.search_by_title(engine, title)

    if not results:
        console.print(f"[yellow]Paper {title!r} not found.[/yellow]")
        logger.info("Delete: paper %r not found", title)
        return

    if len(results) == 1:
        paper = results[0]
    else:
        options = [f"{p.title} (id={p.id})" for p in results]
        options.append("(A)bort")
        idx = ask_choice(options, "Multiple matches — choose paper to delete")
        if idx == len(results) + 1:
            console.print("Delete aborted.")
            return
        paper = results[idx - 1]

    console.print("Paper to delete:")
    _display_summary(paper)

    summary = (
        f"Delete [bold]{paper.title!r}[/bold] (id={paper.id})? "
        "This action cannot be undone."
    )
    if not ask_confirm(summary):
        console.print("Delete aborted.")
        return

    try:
        paper_service.delete_paper(engine, paper.id)
        console.print(f"[green]Deleted paper id={paper.id}.[/green]")
        logger.info("Deleted paper id=%d", paper.id)
    except ValueError as exc:
        console.print(f"[red]Could not delete: {exc}[/red]")
        logger.error("Delete failed: %s", exc)
    except Exception as exc:
        console.print(
            "[red]An unexpected error occurred. Check logs for details.[/red]"
        )
        logger.exception("Unexpected error deleting: %s", exc)


@app.callback(invoke_without_command=True)
def delete_cmd(ctx: typer.Context) -> None:
    """Interactively delete a paper from the database.

    Searches by title, shows the matching paper(s), asks the user to select
    one, then confirms before deleting.
    """
    if ctx.invoked_subcommand is not None:
        return
    engine: Engine = ctx.obj["engine"]
    run_delete(engine)
