"""Typer `update` subcommand for paper_sorts.

Search-to-locate a paper, then update one of its fields with confirmation.
All prompts route through cli/prompts.py (constitution III).
"""

from __future__ import annotations

import logging
import sys

import typer
from sqlalchemy import Engine

from paper_sorts.cli import prompts
from paper_sorts.cli.search import run_search
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger("paper_sorts.cli.update")

app = typer.Typer()


def run_update(engine: Engine) -> bool:
    """Run the interactive update dialog against the given engine.

    :param engine: SQLAlchemy engine connected to the database
    :type engine: Engine
    :return: True if update was performed, False if cancelled or failed
    :rtype: bool
    """
    # Step 1: search to locate the paper
    method = prompts.ask_search_method()
    if method is prompts.ABORT:
        return False

    if method == 0:
        author = prompts.ask_text("Please enter the author's name")
        with with_session(engine) as session:
            results = paper_service.search_by_author(session, author.strip())
    else:
        title = prompts.ask_text("Please enter the paper title")
        with with_session(engine) as session:
            results = paper_service.search_by_title(session, title.strip())

    if not results:
        print("Paper not found.")
        return False

    if len(results) == 1:
        paper = results[0]
    else:
        paper = prompts.ask_paper_from_list(results)
        if paper is None:
            return False

    prompts.pretty_print_paper(paper)

    # Step 2: choose table/column to update
    table = prompts.ask_update_table()
    if table is None:
        print("Update cancelled.")
        return False

    if table == "papers":
        column = prompts.ask_papers_column()
        if column is None:
            print("Update cancelled.")
            return False
    elif table == "bib":
        column = "bibtex"
    else:  # authors
        column = "author"

    # Step 3: get new value
    new_value = prompts.ask_text(f"Enter the new value for '{column}'")

    # For author updates, we need to prompt for author_id too
    if table == "authors" and column == "author":
        author_id_str = prompts.ask_text(
            "Enter the author_id to update (see authors_id table)"
        )
        new_value = f"{author_id_str}:{new_value}"

    # Step 4: confirm the change
    change_description = (
        f"You wish to change '{column}' of paper '{paper.title}' "
        f"(id: {paper.paper_id}) to '{new_value}'."
    )
    confirmed = prompts.ask_confirmation(change_description)
    if not confirmed:
        print("Update cancelled.")
        return False

    # Step 5: apply update
    try:
        with with_session(engine) as session:
            paper_service.update_field(session, paper.paper_id, table, column, new_value)
        print(f"Updated {column} of '{paper.title}'.")
        return True
    except ValueError as exc:
        print(f"Could not update: {exc}")
        logger.error("update_field failed: %s", exc)
        return False
    except Exception as exc:  # noqa: BLE001
        print("Could not update. Check logs for details.")
        logger.error("update_field unexpected error: %s", exc)
        return False


@app.command("update")
def update_command(
    ctx: typer.Context,
) -> None:
    """Update a field of an existing paper entry."""
    engine: Engine = ctx.obj
    success = run_update(engine)
    if not success:
        sys.exit(1)
