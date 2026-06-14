"""Update subcommand for paper_sorts CLI.

Implements ``pdbsearch update``: prompts for table, field, identifier, and
new value, then shows a confirmation before updating.
"""

from __future__ import annotations

import logging
from typing import Literal

import typer
from rich.console import Console
from sqlalchemy.engine import Engine

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_nonempty
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)
console = Console()

app = typer.Typer(help="Update an existing paper field.")


def run_update(engine: Engine) -> None:
    """Interactively update a paper field in the database.

    Prompts the user to select the table, field, identifier, and new value.
    Shows a confirmation summary before applying the change.

    :param engine: Active SQLAlchemy engine.
    """
    table_choice = ask_choice(
        [
            "papers (title / contents)",
            "bib (bibtex entry)",
            "authors_id (author name)",
            "(A)bort",
        ],
        "Which table to update",
    )
    if table_choice == 4:
        console.print("Update aborted.")
        return

    table_map: dict[int, Literal["papers", "bib", "authors_id"]] = {
        1: "papers",
        2: "bib",
        3: "authors_id",
    }
    table: Literal["papers", "bib", "authors_id"] = table_map[table_choice]

    if table == "papers":
        field_choice = ask_choice(
            ["title", "contents", "(A)bort"],
            "Which field to update",
        )
        if field_choice == 3:
            console.print("Update aborted.")
            return
        field = "title" if field_choice == 1 else "contents"
        identifier_prompt = "Paper ID (integer)"
    elif table == "bib":
        console.print("Only the bibtex content can be updated (not the key).")
        field = "bibtex"
        identifier_prompt = "Paper ID (integer)"
    else:
        console.print("Only the author name can be updated.")
        field = "author"
        identifier_prompt = "Author ID (integer)"

    identifier_raw = ask_nonempty(identifier_prompt)
    try:
        identifier = int(identifier_raw)
    except ValueError:
        console.print("[red]Identifier must be an integer.[/red]")
        return

    new_value = ask_nonempty("New value")

    summary = (
        f"You are about to update table=[bold]{table}[/bold], "
        f"field=[bold]{field}[/bold], id=[bold]{identifier}[/bold] "
        f"→ [bold]{new_value!r}[/bold]"
    )
    if not ask_confirm(summary):
        console.print("Update aborted.")
        return

    try:
        paper_service.update_field(engine, identifier, table, field, new_value)
        console.print("[green]Update successful.[/green]")
        logger.info("Updated %s.%s for id=%d", table, field, identifier)
    except ValueError as exc:
        console.print(f"[red]Could not update: {exc}[/red]")
        logger.error("Update failed: %s", exc)
    except Exception as exc:
        console.print(
            "[red]An unexpected error occurred. Check logs for details.[/red]"
        )
        logger.exception("Unexpected error updating: %s", exc)


@app.callback(invoke_without_command=True)
def update_cmd(ctx: typer.Context) -> None:
    """Interactively update a paper field in the database.

    Prompts the user to select the table, field, identifier, and new value.
    Shows a confirmation summary before applying the change.
    """
    if ctx.invoked_subcommand is not None:
        return
    engine: Engine = ctx.obj["engine"]
    run_update(engine)
