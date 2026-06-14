"""Delete subcommand for pdbsearch CLI.

Searches for a paper by title and deletes it after user confirmation.
"""

from __future__ import annotations

import logging

from rich.console import Console

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_text
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)
console = Console()


def delete_callback(db_url: str) -> None:
    """Interactive delete subcommand entrypoint.

    Prompts for a paper title, searches for matches, disambiguates if needed,
    and deletes the chosen paper after confirmation.

    Args:
        db_url: SQLAlchemy-compatible database URL from the app callback.
    """
    title = ask_text("Please enter the paper title to delete: ")
    papers = paper_service.search_by_title(db_url, title.strip())

    if not papers:
        console.print("Paper was not found in database.")
        logger.info("Delete: title %r not found.", title)
        return

    if len(papers) > 1:
        options = [f"{p.title} (id={p.paper_id})" for p in papers]
        idx = ask_choice("Multiple papers found — please choose one to delete:", options)
        paper = papers[idx]
    else:
        paper = papers[0]

    console.print(
        f"\n[bold]Paper to delete:[/bold]\n"
        f"  title:   {paper.title}\n"
        f"  authors: {paper.authors}\n"
        f"  bibtex:  {paper.bibtex_id}\n"
    )

    confirmed = ask_confirm(f"Are you sure you want to delete {paper.title!r}?")
    if not confirmed:
        console.print("Delete cancelled.")
        return

    try:
        paper_service.delete_paper(db_url, paper.paper_id)
        console.print(f"[green]Paper {paper.title!r} deleted.[/green]")
        logger.info("Deleted paper %r (id=%d)", paper.title, paper.paper_id)
    except ValueError as exc:
        console.print(f"[red]Could not delete paper — {exc}[/red]")
        logger.error("Delete failed for paper id=%d: %s", paper.paper_id, exc)
