"""Update subcommand for pdbsearch.

Provides :func:`update_cmd` — a Typer command that lets the user search for a
paper and update one of its fields (title, summary, bibtex, author).  Presents
a confirmation step before writing.
"""

from __future__ import annotations

import logging

import typer

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_nonempty
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Update a field on an existing paper.")

FIELD_LABELS = ["title", "contents", "bibtex", "author"]
FIELD_DISPLAY = [
    "Title",
    "Summary (contents)",
    "BibTeX entry",
    "Author (replace all with one)",
]


def run_update(database_url: str, paper_id: int | None = None) -> None:
    """Interactive update flow (called from interactive menu or subcommand).

    :param database_url: PostgreSQL DSN.
    :param paper_id: If known, skip the search step.
    """
    if paper_id is None:
        # Search for a paper first
        search_options = ["Search by title", "Search by author", "Abort"]
        method = ask_choice(search_options, "Find paper to update")
        if method == 3:
            print("Update aborted.")
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
            pick = ask_choice(options, "Select paper to update")
            if pick == len(options):
                print("Update aborted.")
                return
            chosen = results[pick - 1]

        paper_id = chosen.paper_id
        print(f"\nSelected: {chosen.title!r} (id={paper_id})")

    # Choose field
    field_options = FIELD_DISPLAY + ["Abort"]
    field_pick = ask_choice(field_options, "Field to update")
    if field_pick == len(field_options):
        print("Update aborted.")
        return

    field = FIELD_LABELS[field_pick - 1]
    new_value = ask_nonempty(f"New value for {FIELD_DISPLAY[field_pick - 1]}")

    if not ask_confirm(f"Update {FIELD_DISPLAY[field_pick - 1]!r} to {new_value!r}?"):
        print("Update cancelled.")
        return

    try:
        paper_service.update_field(
            paper_id,
            field,  # type: ignore[arg-type]
            new_value,
            database_url=database_url,
            with_session_fn=with_session,
        )
        print("Update saved.")
    except Exception as exc:
        logger.error("Failed to update paper %d: %s", paper_id, exc)
        print("Error: could not update paper. Check logs for details.")


@app.callback(invoke_without_command=True)
def update_cmd(
    ctx: typer.Context,
    database_url: str = typer.Option(
        None, "--database-url", envvar="PDBSEARCH_DATABASE_URL", help="PostgreSQL DSN"
    ),
    paper_id: int = typer.Option(None, "--id", help="Paper ID to update directly"),
) -> None:
    """Update a field on an existing paper."""
    if ctx.invoked_subcommand is not None:
        return
    if not database_url:
        typer.echo("Error: database URL not configured.", err=True)
        raise typer.Exit(1)
    run_update(database_url, paper_id=paper_id)
