"""Delete subcommand for pdbsearch CLI."""

import logging
import sys

import typer
from sqlalchemy.engine import Engine

from paper_sorts.cli.prompts import ask_choice, ask_confirmation, ask_input
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Delete a paper from the database.")


def run_delete(engine: Engine) -> None:
    """Interactive delete flow — called from the top-level menu.

    :param engine: Active SQLAlchemy engine.
    """
    title = ask_input("Enter the title of the paper to delete: ")
    results = paper_service.search_by_title(engine, title)

    if not results:
        print("Paper not found in the database.")
        return

    if len(results) > 1:
        labels = [f"{p.title} (id={p.id}, bibtex_id={p.bibtex_id})" for p in results]
        idx = ask_choice(labels, prompt="Choose paper to delete: ", quit_label="(A)bort")
        if idx == -1:
            print("Delete aborted.")
            return
        paper = results[idx]
    else:
        paper = results[0]

    authors_str = " and ".join(paper.authors) if paper.authors else "(no authors)"
    if not ask_confirmation(
        f"About to delete:\n"
        f"  Title: {paper.title}\n"
        f"  Authors: {authors_str}\n"
        f"  BibTeX key: {paper.bibtex_id}\n"
        f"  Summary: {paper.contents[:80]}..."
    ):
        print("Delete aborted.")
        return

    try:
        paper_service.delete_paper(engine, paper.id)
        print(f"Successfully deleted '{paper.title}'.")
    except ValueError as exc:
        logger.error("Delete failed: %s", exc)
        print(f"Could not delete: {exc}")


@app.command()
def delete_cmd(
    ctx: typer.Context,
    paper_id: int | None = typer.Option(None, "--id", help="Paper ID to delete"),
) -> None:
    """Delete a paper by searching for it (or specifying its ID).

    :param ctx: Typer context carrying the engine.
    :param paper_id: Optional paper ID to skip the search step.
    """
    raw_engine = ctx.obj.get("engine") if ctx.obj else None
    if raw_engine is None or not isinstance(raw_engine, Engine):
        logger.error("No database connection available")
        print("Error: no database URL configured.")
        sys.exit(1)

    run_delete(raw_engine)
