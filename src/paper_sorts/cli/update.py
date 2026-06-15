"""CLI 'update' subcommand for paper_sorts.

Prompts for table → column → identifier → new value → confirmation.
All prompts route through cli/prompts.py (constitution Principle III).
Destructive operation: requires explicit confirmation before applying change.
"""

from __future__ import annotations

import logging

import typer

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_nonempty
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer()

# Map display label → (table, column) for the update matrix
_TABLE_OPTIONS = ["papers", "bib", "authors_id", "abort"]
_PAPERS_COLUMN_OPTIONS = ["title", "contents", "abort"]


def run_update(db_url: str) -> None:
    """Execute the interactive update flow.

    :param db_url: SQLAlchemy-compatible database URL.
    """
    table_idx = ask_choice(
        "Which information do you want to update?",
        ["papers (title / contents)", "bib (bibtex)", "authors (author name)", "abort"],
    )
    if table_idx == 3:
        typer.echo("Update aborted.")
        return

    table = _TABLE_OPTIONS[table_idx]

    if table == "papers":
        col_idx = ask_choice(
            "Which column do you want to update?",
            ["title", "contents", "abort"],
        )
        if col_idx == 2:
            typer.echo("Update aborted.")
            return
        column = _PAPERS_COLUMN_OPTIONS[col_idx]
        typer.echo(f"Searching by current {column}.")
        identifier = ask_nonempty(f"Enter the current {column} of the paper to update")
    elif table == "bib":
        typer.echo("Only the bibtex text can be updated — the bibtex key cannot be changed.")
        column = "bibtex"
        identifier = ask_nonempty("Enter the bibtex_id (key) of the entry to update")
    else:  # authors_id
        typer.echo("Only the author name can be updated.")
        column = "author"
        identifier = ask_nonempty("Enter the current author name")

    new_value = ask_nonempty("Enter the new value")

    confirmed = ask_confirm(
        f"Please verify: you wish to change '{column}' of '{identifier}' to '{new_value}'."
        "\nProceed?"
    )
    if not confirmed:
        typer.echo("Update cancelled.")
        return

    try:
        with with_session(db_url) as session:
            paper_service.update_field(session, table, column, identifier, new_value)  # type: ignore[arg-type]
        typer.echo("Update applied successfully.")
    except ValueError as exc:
        logger.error("Update failed: %s", exc)
        typer.echo(f"Could not update entry: {exc}", err=True)


@app.command()
def update(ctx: typer.Context) -> None:
    """Update an existing paper, BibTeX entry, or author name."""
    db_url: str = ctx.obj["db_url"]
    run_update(db_url)
