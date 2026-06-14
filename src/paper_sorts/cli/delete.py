"""Typer `delete` subcommand for paper_sorts.

Search-to-locate a paper, display it, then confirm before deleting.
All prompts route through cli/prompts.py (constitution III).
"""

from __future__ import annotations

import logging
import sys

import typer
from sqlalchemy import Engine

from paper_sorts.cli import prompts
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger("paper_sorts.cli.delete")

app = typer.Typer()


def run_delete(engine: Engine) -> bool:
    """Run the interactive delete dialog against the given engine.

    :param engine: SQLAlchemy engine connected to the database
    :type engine: Engine
    :return: True if paper was deleted, False if cancelled or not found
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

    chosen = results[0] if len(results) == 1 else prompts.ask_paper_from_list(results)
    if chosen is None:
        return False

    paper = chosen
    # Step 2: display the paper
    prompts.pretty_print_paper(paper)

    # Step 3: confirm deletion
    confirmed = prompts.ask_confirmation(
        f"Delete '{paper.title}' (bibtex_id: {paper.bibtex_id})?"
    )
    if not confirmed:
        print("Delete cancelled.")
        return False

    # Step 4: delete
    try:
        with with_session(engine) as session:
            paper_service.delete_paper(session, paper.paper_id)
        print(f"Deleted '{paper.title}'.")
        logger.info("Deleted paper %d ('%s')", paper.paper_id, paper.title)
        return True
    except ValueError as exc:
        print(f"Could not delete: {exc}")
        logger.error("delete_paper failed: %s", exc)
        return False
    except Exception as exc:
        print("Could not delete. Check logs for details.")
        logger.error("delete_paper unexpected error: %s", exc)
        return False


@app.command("delete")
def delete_command(
    ctx: typer.Context,
) -> None:
    """Delete a paper entry from the database."""
    engine: Engine = ctx.obj
    success = run_delete(engine)
    if not success:
        sys.exit(1)
