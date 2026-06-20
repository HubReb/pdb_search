"""Delete subcommand for pdbsearch.

Provides :func:`delete_cmd` — a Typer command that lets the user search for a
paper, display its details, and confirm deletion.
"""

from __future__ import annotations

import logging

import typer

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_nonempty
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Delete a paper from the database.")


def run_delete(database_url: str, paper_id: int | None = None) -> None:
    """Interactive delete flow (called from interactive menu or subcommand).

    :param database_url: PostgreSQL DSN.
    :param paper_id: If known, skip the search step.
    """
    if paper_id is None:
        search_options = ["Search by title", "Search by author", "Abort"]
        method = ask_choice(search_options, "Find paper to delete")
        if method == 3:
            print("Delete aborted.")
            return

        if method == 1:
            query = ask_nonempty("Enter title")
            results = paper_service.search_by_title(
                query, database_url=database_url, with_session_fn=with_session
            )
        else:
            query = ask_nonempty("Enter author name (Last, First)")
            results = paper_service.search_by_author(
                query, database_url=database_url, with_session_fn=with_session
            )

        if not results:
            print("No papers found.")
            return

        if len(results) == 1:
            chosen = results[0]
        else:
            options = [f"{r.title} (id={r.paper_id})" for r in results]
            options.append("Abort")
            pick = ask_choice(options, "Select paper to delete")
            if pick == len(options):
                print("Delete aborted.")
                return
            chosen = results[pick - 1]

        paper_id = chosen.paper_id

    # Fetch and display paper details
    with with_session(database_url) as session:
        from paper_sorts.db.repositories import PaperRepository

        summary = PaperRepository.get_by_id(session, paper_id)

    if summary is None:
        print(f"Paper id={paper_id} not found.")
        return

    authors = ", ".join(summary.authors) if summary.authors else "(no authors)"
    print(f"\ntitle: {summary.title}")
    print(f"authors: {authors}")
    print(f"summary: {summary.summary}")

    if not ask_confirm(f"Delete paper {summary.title!r} (id={paper_id})?"):
        print("Delete cancelled.")
        return

    try:
        paper_service.delete_paper(
            paper_id, database_url=database_url, with_session_fn=with_session
        )
        print("Paper deleted.")
    except Exception as exc:
        logger.error("Failed to delete paper %d: %s", paper_id, exc)
        print("Error: could not delete paper. Check logs for details.")


@app.callback(invoke_without_command=True)
def delete_cmd(
    ctx: typer.Context,
    database_url: str = typer.Option(
        None, "--database-url", envvar="PDBSEARCH_DATABASE_URL", help="PostgreSQL DSN"
    ),
    paper_id: int = typer.Option(None, "--id", help="Paper ID to delete directly"),
) -> None:
    """Delete a paper from the database."""
    if ctx.invoked_subcommand is not None:
        return
    if not database_url:
        typer.echo("Error: database URL not configured.", err=True)
        raise typer.Exit(1)
    run_delete(database_url, paper_id=paper_id)
