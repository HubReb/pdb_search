"""Typer `search` subcommand for paper_sorts.

Provides interactive search by author or title via the shared session.
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

logger = logging.getLogger("paper_sorts.cli.search")

app = typer.Typer()


def run_search(engine: Engine) -> None:
    """Run the interactive search dialog against the given engine.

    :param engine: SQLAlchemy engine connected to the database
    :type engine: Engine
    """
    method = prompts.ask_search_method()
    if method is prompts.ABORT:
        return

    if method == 0:
        # Search by author
        author = prompts.ask_text("Please enter the author's name")
        with with_session(engine) as session:
            results = paper_service.search_by_author(session, author.strip())
        if not results:
            print("Author was not found in database.")
            logger.info("search_by_author(%r): no results", author)
            return
    else:
        # Search by title
        title = prompts.ask_text("Please enter the paper title")
        with with_session(engine) as session:
            results = paper_service.search_by_title(session, title.strip())
        if not results:
            print("Paper was not found in database.")
            logger.info("search_by_title(%r): no results", title)
            return

    if len(results) == 1:
        paper = results[0]
    else:
        paper = prompts.ask_paper_from_list(results)
        if paper is None:
            return

    prompts.pretty_print_paper(paper)


@app.command("search")
def search_command(
    ctx: typer.Context,
) -> None:
    """Search the database for papers by author or title."""
    engine: Engine = ctx.obj
    try:
        run_search(engine)
    except Exception as exc:  # noqa: BLE001
        logger.error("Search failed: %s", exc)
        print("Search failed. Check logs for details.")
        sys.exit(1)
