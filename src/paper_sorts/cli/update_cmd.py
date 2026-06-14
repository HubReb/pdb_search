"""update subcommand for pdbsearch CLI.

Provides `pdbsearch update` (direct invocation) and `run_update`
(called from interactive menu). All prompts route through cli/prompts.py.
"""

from __future__ import annotations

import logging
from typing import Annotated

import typer

from paper_sorts.services.paper_service import UpdateTarget

logger = logging.getLogger(__name__)

app = typer.Typer()

_FIELD_LABELS: list[tuple[str, UpdateTarget]] = [
    ("Title", "title"),
    ("Summary / contents", "contents"),
    ("BibTeX entry", "bibtex"),
    ("Author name", "author"),
]


def run_update(database_url: str, bibtex_id: str | None = None) -> None:
    """Interactive update flow (called from the main menu).

    :param database_url: PostgreSQL DSN
    :param bibtex_id: optional BibTeX key; if omitted, triggers a search to find the paper
    """
    from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_text
    from paper_sorts.services.paper_service import search_by_title, update_field

    # If no ID given, search to pick the paper
    if bibtex_id is None:
        query = ask_text("Enter a title fragment to find the paper to update")
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
        print(f"Updating: {chosen.title} ({bibtex_id})")

    # Choose field
    field_labels = [label for label, _ in _FIELD_LABELS]
    chosen_label = ask_choice("Which field do you want to update?", field_labels, allow_quit=True)
    if chosen_label is None:
        print("Update cancelled.")
        return

    field: UpdateTarget = dict(_FIELD_LABELS)[chosen_label]

    # Collect new value
    if field == "author":
        old_name = ask_text("Current author name (exactly as stored)")
        new_name = ask_text("New author name")
        new_value = f"{old_name} -> {new_name}"
        confirm_msg = (
            f"Change author '{old_name}' to '{new_name}' "
            f"(affects all papers by this author). Proceed?"
        )
    else:
        new_value = ask_text(f"New value for '{chosen_label}'")
        confirm_msg = (
            f"Change '{chosen_label}' of paper '{bibtex_id}' to:\n  {new_value}\nProceed?"
        )

    if not ask_confirm(confirm_msg):
        print("Update cancelled.")
        return

    try:
        update_field(database_url, bibtex_id, field, new_value)
        print("Updated.")
    except (KeyError, ValueError) as exc:
        print(f"Could not update: {exc}")
        logger.error("update_field failed: %s", exc)
    except Exception as exc:
        logger.error("Unexpected error updating: %s", exc)
        print("Update failed — please check the logs.")


@app.command("update")
def update_cmd(
    ctx: typer.Context,
    paper_id: Annotated[
        str | None,
        typer.Option("--id", help="BibTeX key of the paper to update"),
    ] = None,
) -> None:
    """Update a field of an existing paper.

    :param ctx: Typer context carrying settings from the app callback
    :param paper_id: optional BibTeX key; if omitted, a search flow selects the paper
    """
    settings = ctx.obj["settings"] if ctx.obj else None
    database_url: str
    if settings is not None:
        database_url = settings.get_database_url()
    else:
        raise typer.BadParameter("No database URL configured.")

    run_update(database_url, paper_id)
