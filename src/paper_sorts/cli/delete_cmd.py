"""delete subcommand for pdbsearch CLI.

Provides `pdbsearch delete` (direct invocation) and `run_delete`
(called as a standalone action). All prompts route through cli/prompts.py.
"""

from __future__ import annotations

import logging
from typing import Annotated

import typer

logger = logging.getLogger(__name__)

app = typer.Typer()


def run_delete(database_url: str, bibtex_id: str | None = None) -> None:
    """Interactive delete flow.

    :param database_url: PostgreSQL DSN
    :param bibtex_id: optional BibTeX key; if omitted, triggers a search to find the paper
    """
    from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_text
    from paper_sorts.services.paper_service import delete_paper, search_by_title

    if bibtex_id is None:
        query = ask_text("Enter a title fragment to find the paper to delete")
        try:
            results = search_by_title(database_url, query)
        except Exception as exc:
            logger.error("Search failed: %s", exc)
            print("Search failed — please check the logs.")
            return
        if not results:
            print(f"No papers found for '{query}'.")
            return
        if len(results) == 1:
            chosen = results[0]
        else:
            labels = [f"{p.title} ({p.bibtex_id})" for p in results]
            chosen_label = ask_choice(
                f"Found {len(results)} matches. Select one:", labels, allow_quit=True
            )
            if chosen_label is None:
                return
            idx = labels.index(chosen_label)
            chosen = results[idx]
        bibtex_id = chosen.bibtex_id
        title = chosen.title
    else:
        title = bibtex_id  # will be overwritten by delete result

    if not ask_confirm(
        f"You will permanently delete '{title}' ({bibtex_id}).\n"
        f"This also removes BibTeX entry and orphan authors. Proceed?"
    ):
        print("Delete cancelled.")
        return

    try:
        deleted_title = delete_paper(database_url, bibtex_id)
        print(f"Deleted '{deleted_title}'.")
    except KeyError as exc:
        print(f"Could not delete: {exc}")
        logger.error("delete_paper failed: %s", exc)
    except Exception as exc:
        logger.error("Unexpected error deleting: %s", exc)
        print("Delete failed — please check the logs.")


@app.command("delete")
def delete_cmd(
    ctx: typer.Context,
    paper_id: Annotated[
        str | None,
        typer.Option("--id", help="BibTeX key of the paper to delete"),
    ] = None,
) -> None:
    """Delete a paper and its associated data from the database.

    :param ctx: Typer context carrying settings from the app callback
    :param paper_id: optional BibTeX key; if omitted, a search flow selects the paper
    """
    settings = ctx.obj["settings"] if ctx.obj else None
    database_url: str
    if settings is not None:
        database_url = settings.get_database_url()
    else:
        raise typer.BadParameter("No database URL configured.")

    run_delete(database_url, paper_id)
