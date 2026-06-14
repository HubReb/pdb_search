"""Update subcommand for paper_sorts CLI."""

from __future__ import annotations

import logging
from typing import Literal

import typer

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_str
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service
from paper_sorts.services.paper_service import UpdateTable

log = logging.getLogger(__name__)

app = typer.Typer(help="Update an existing database entry.")


def _run_update(database_url: str) -> None:
    """Execute the interactive update flow.

    :param database_url: SQLAlchemy-compatible database URL.
    """
    table_choice = ask_choice(
        "Which information do you want to update?",
        ["papers", "bib", "authors"],
    )
    if table_choice is None:
        typer.echo("Update aborted.")
        return

    table: UpdateTable
    column: str

    match table_choice:
        case 1:
            table = "papers"
            col_choice = ask_choice(
                "Which column in papers?",
                ["title", "contents"],
            )
            if col_choice is None:
                typer.echo("Update aborted.")
                return
            column = "title" if col_choice == 1 else "contents"
        case 2:
            table = "bib"
            column = "bibtex"
            typer.echo("Only the 'bibtex' column can be updated — the bibtex_id cannot be changed.")
        case 3:
            table = "authors_id"
            column = "author"
            typer.echo("Only the author name can be updated.")
        case _:
            typer.echo("Invalid choice.")
            return

    identifier = ask_str("Identifier of the entry to update (paper ID, bibtex_id, or author name)")
    new_value = ask_str("New value")

    confirmed = ask_confirm(
        f"Change '{column}' of entry '{identifier}' to '{new_value}'?"
    )
    if not confirmed:
        typer.echo("Update aborted.")
        return

    try:
        with with_session(database_url) as session:
            paper_service.update_field(session, table, identifier, column, new_value)
        typer.echo("Update successful.")
        log.info("Updated %s.%s for identifier '%s'.", table, column, identifier)
    except ValueError as exc:
        typer.echo(f"Could not update entry: {exc}")
        log.error("Update failed for %s.%s '%s': %s", table, column, identifier, exc)
