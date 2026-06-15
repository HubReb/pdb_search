"""CLI 'delete' subcommand for paper_sorts.

Prompts for a BibTeX key, shows the paper summary, then asks for confirmation
before deleting. Destructive operation requires explicit confirmation.
"""

from __future__ import annotations

import logging

import typer

from paper_sorts.cli.prompts import ask_confirm, ask_nonempty
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer()


def run_delete(db_url: str) -> None:
    """Execute the interactive delete flow.

    :param db_url: SQLAlchemy-compatible database URL.
    """
    bibtex_id = ask_nonempty("Enter the BibTeX key of the paper to delete")

    # Show the paper details before asking for confirmation.
    # Capture title/contents inside the session to avoid DetachedInstanceError.
    paper_title: str = ""
    paper_contents: str = ""
    with with_session(db_url) as session:
        from paper_sorts.db.repositories import PaperRepository

        repo = PaperRepository(session)
        paper = repo.find_by_bibtex_id(bibtex_id)
        if paper is None:
            typer.echo(f"No paper found with BibTeX key '{bibtex_id}'.")
            return
        paper_title = paper.title
        paper_contents = paper.contents

    typer.echo("\nPaper to delete:")
    typer.echo(f"  title:   {paper_title}")
    typer.echo(f"  key:     {bibtex_id}")
    typer.echo(f"  summary: {paper_contents}")

    confirmed = ask_confirm(
        f"Are you sure you want to permanently delete '{bibtex_id}' ('{paper_title}')?"
    )
    if not confirmed:
        typer.echo("Delete cancelled.")
        return

    with with_session(db_url) as session:
        deleted = paper_service.delete_paper(session, bibtex_id)

    if deleted:
        typer.echo(f"Paper '{bibtex_id}' deleted successfully.")
    else:
        typer.echo(f"Paper '{bibtex_id}' was not found.", err=True)


@app.command()
def delete(ctx: typer.Context) -> None:
    """Delete a paper and its related BibTeX and author links."""
    db_url: str = ctx.obj["db_url"]
    run_delete(db_url)
