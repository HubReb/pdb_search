"""Update subcommand for pdbsearch CLI.

Interactively updates a single field in papers, bib, or authors_id.
Requires confirmation before applying the change (constitution Principle III).
"""

from __future__ import annotations

import logging
from typing import Literal

from rich.console import Console

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_text
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)
console = Console()


def update_callback(db_url: str) -> None:
    """Interactive update subcommand entrypoint.

    Asks the user which table to update, which column, the row identifier,
    and the new value. Presents a confirmation step before applying the change.

    Args:
        db_url: SQLAlchemy-compatible database URL from the app callback.
    """
    table_options = ["papers", "bib", "authors", "abort"]
    table_idx = ask_choice(
        "Which information do you want to update?",
        table_options,
    )

    if table_idx == 3:  # abort
        console.print("Update aborted.")
        return

    table_name: Literal["papers", "bib", "authors_id"]
    match table_idx:
        case 0:
            table_name = "papers"
            column = _get_papers_column()
            if column is None:
                return
        case 1:
            table_name = "bib"
            console.print("Only the bibtex string can be updated — the bibtex key cannot be changed.")
            column = "bibtex"
        case 2:
            table_name = "authors_id"
            console.print("Only an author name can be updated.")
            column = "author"
        case _:
            console.print("Invalid choice.")
            return

    identifier = ask_text(
        "Which entry do you want to update?\nPlease enter the respective id/key/name: "
    )
    new_value = ask_text("Enter the new value: ")

    confirmed = ask_confirm(
        f"Please verify: you wish to change {column!r} of entry {identifier!r} "
        f"to {new_value!r}.\nProceed?"
    )
    if not confirmed:
        console.print("Update cancelled.")
        return

    try:
        paper_service.update_field(db_url, table_name, column, identifier, new_value)
        console.print(
            f"[green]Updated {table_name}.{column} for {identifier!r}.[/green]"
        )
    except ValueError as exc:
        console.print(f"[red]Could not update entry — {exc}[/red]")
        logger.error("Update failed for %s.%s id=%r: %s", table_name, column, identifier, exc)


def _get_papers_column() -> str | None:
    """Prompt the user for which column of the papers table to update.

    Returns:
        Column name ('title' or 'contents'), or None if the user aborted.
    """
    col_options = ["title", "contents", "abort"]
    col_idx = ask_choice(
        "Which papers column do you want to update?",
        col_options,
    )
    if col_idx == 2:  # abort
        console.print("Update aborted.")
        return None
    return col_options[col_idx]
