"""Delete subcommand for pdbsearch.

Registered as ``pdbsearch delete`` in :mod:`paper_sorts.cli.app`.
"""

from __future__ import annotations

import logging
from typing import cast

import typer
from sqlalchemy import Engine

from paper_sorts.cli.prompts import ask_confirm, ask_str
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Delete a paper from the database.")


@app.callback(invoke_without_command=True)
def delete_cmd(ctx: typer.Context) -> None:
    """Interactively delete a paper from the database.

    Prompts for a BibTeX key, shows the paper details, and asks for
    confirmation before deleting.

    :param ctx: Typer context carrying the SQLAlchemy engine.
    """
    engine = cast(Engine, ctx.obj["engine"])

    bibtex_id = ask_str("Enter the BibTeX key of the paper to delete: ")

    # Look up the BibTeX entry to show details before deletion
    from paper_sorts.db.repositories import BibRepository
    from paper_sorts.db.session import with_session

    with with_session(engine) as session:
        bibtex = BibRepository.get_bibtex(session, bibtex_id)

    if bibtex is None:
        print(f"No paper found with BibTeX key {bibtex_id!r}.")
        return

    print(f"\nAbout to delete paper with key: {bibtex_id!r}")
    print(f"BibTeX entry preview:\n{bibtex[:200]}...")

    if not ask_confirm("Are you sure you want to delete this entry?"):
        print("Aborted — no changes made.")
        return

    try:
        paper_service.delete_paper(engine, bibtex_id)
        print(f"Deleted paper {bibtex_id!r} from the database.")
    except ValueError as exc:
        logger.error("Failed to delete paper %r: %s", bibtex_id, exc)
        print(f"Could not delete entry — {exc}")
