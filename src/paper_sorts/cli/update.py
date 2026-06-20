"""Update subcommand for pdbsearch.

Registered as ``pdbsearch update`` in :mod:`paper_sorts.cli.app`.
"""

from __future__ import annotations

import logging
from typing import Literal, cast

import typer
from sqlalchemy import Engine

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_str
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Update an existing entry in the database.")


@app.callback(invoke_without_command=True)
def update_cmd(ctx: typer.Context) -> None:
    """Interactively update a field in the database.

    :param ctx: Typer context carrying the SQLAlchemy engine.
    """
    engine = cast(Engine, ctx.obj["engine"])

    table_choice = ask_choice(
        ["papers (title / contents)", "bib (BibTeX entry)", "authors_id (author name)", "abort"],
        prompt="Which table do you want to update? ",
    )
    if table_choice == 3:
        print("Aborted.")
        return

    tables: list[Literal["papers", "bib", "authors_id"]] = ["papers", "bib", "authors_id"]
    table: Literal["papers", "bib", "authors_id"] = tables[table_choice]

    if table == "papers":
        field_choice = ask_choice(
            ["title", "contents", "abort"],
            prompt="Which field do you want to update? ",
        )
        if field_choice == 2:
            print("Aborted.")
            return
        field = ["title", "contents"][field_choice]
        identifier = ask_str("Enter the paper id (numeric): ")

    elif table == "bib":
        print("Only the 'bibtex' field can be updated — the key cannot be changed.")
        field = "bibtex"
        identifier = ask_str("Enter the bibtex_id of the entry to update: ")

    else:  # authors_id
        print("Only the author name can be updated.")
        field = "author"
        identifier = ask_str("Enter the current author name: ")

    value = ask_str("Enter the new value: ")

    if not ask_confirm(
        f"You wish to change '{field}' of '{identifier}' to '{value}'."
    ):
        print("Aborted — no changes made.")
        return

    try:
        paper_service.update_field(
            engine,
            table=table,
            identifier=identifier,
            field=field,
            value=value,
        )
        print(f"Updated {table}.{field} for '{identifier}'.")
    except ValueError as exc:
        logger.error("Failed to update: %s", exc)
        print(f"Could not update entry — {exc}")
