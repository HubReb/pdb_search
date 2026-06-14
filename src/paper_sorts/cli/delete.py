"""Delete subcommand for paper_sorts CLI."""

from __future__ import annotations

import logging

import typer

from paper_sorts.cli.prompts import ask_confirm, ask_str
from paper_sorts.db.repositories import PaperRepository
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

log = logging.getLogger(__name__)

app = typer.Typer(help="Delete a paper from the database.")


def _run_delete(database_url: str) -> None:
    """Execute the interactive delete flow.

    :param database_url: SQLAlchemy-compatible database URL.
    """
    bibtex_id = ask_str("BibTeX key of the paper to delete")

    with with_session(database_url) as session:
        summary = PaperRepository.get_by_bibtex_id(session, bibtex_id)
        if summary is None:
            typer.echo(f"No paper found with BibTeX key '{bibtex_id}'.")
            return

        typer.echo(f"\nTitle:   {summary.title}")
        typer.echo(f"Authors: {' and '.join(summary.authors)}")
        typer.echo(f"Summary: {summary.contents}\n")

        confirmed = ask_confirm(f"Delete paper '{summary.title}'?")
        if not confirmed:
            typer.echo("Delete aborted.")
            return

        try:
            paper_service.delete_paper(session, bibtex_id)
            typer.echo(f"Paper '{summary.title}' deleted successfully.")
            log.info("Deleted paper bibtex_id='%s'.", bibtex_id)
        except ValueError as exc:
            typer.echo(f"Could not delete paper: {exc}")
            log.error("Delete failed for bibtex_id='%s': %s", bibtex_id, exc)
