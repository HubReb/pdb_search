"""Update subcommand for pdbsearch CLI."""

import logging
import sys
from typing import Literal

import typer
from sqlalchemy.engine import Engine

from paper_sorts.cli.prompts import ask_choice, ask_confirmation, ask_input
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Update an existing paper field.")


def run_update(engine: Engine) -> None:
    """Interactive update flow — called from the top-level menu.

    :param engine: Active SQLAlchemy engine.
    """
    # Step 1: table
    table_idx = ask_choice(
        ["papers", "bib", "authors"],
        prompt="Which table? ",
        quit_label="(A)bort",
    )
    if table_idx == -1:
        print("Update aborted.")
        return

    table_map: list[Literal["papers", "bib", "authors_id"]] = ["papers", "bib", "authors_id"]
    table = table_map[table_idx]

    # Step 2: column
    if table == "papers":
        col_idx = ask_choice(
            ["title", "contents"],
            prompt="Which column? ",
            quit_label="(A)bort",
        )
        if col_idx == -1:
            print("Update aborted.")
            return
        column = ["title", "contents"][col_idx]
    elif table == "bib":
        print("Only the bibtex column can be updated.")
        column = "bibtex"
    else:  # authors_id
        print("Only the author name can be updated.")
        column = "author"

    # Step 3: identifier + new value
    identifier = ask_input(
        "Enter the identifier of the row to update (paper id / bibtex_id / author id): "
    )
    value = ask_input("Enter the new value: ")

    if not ask_confirmation(
        f"About to update {table}.{column} "
        f"(identifier={identifier!r}) → {value!r}"
    ):
        print("Update aborted.")
        return

    try:
        paper_service.update_field(engine, table, column, identifier, value)
        print("Update successful.")
    except ValueError as exc:
        logger.error("Update failed: %s", exc)
        print(f"Could not update: {exc}")


@app.command()
def update_cmd(
    ctx: typer.Context,
    table: str | None = typer.Option(None, "--table"),
    column: str | None = typer.Option(None, "--column"),
    identifier: str | None = typer.Option(None, "--id"),
    value: str | None = typer.Option(None, "--value"),
) -> None:
    """Update a single field in the database.

    :param ctx: Typer context carrying the engine.
    :param table: Table to update (papers, bib, authors).
    :param column: Column to update.
    :param identifier: Row identifier (paper id, bibtex_id, or author id).
    :param value: New value to set.
    """
    raw_engine = ctx.obj.get("engine") if ctx.obj else None
    if raw_engine is None or not isinstance(raw_engine, Engine):
        logger.error("No database connection available")
        print("Error: no database URL configured.")
        sys.exit(1)

    run_update(raw_engine)
