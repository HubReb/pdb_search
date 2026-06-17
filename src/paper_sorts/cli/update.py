"""Interactive update flow: edit a paper, bib, or author field with confirmation."""

from __future__ import annotations

from rich.console import Console
from sqlalchemy import Engine

from paper_sorts.cli.prompts import ask_choice, ask_nonempty, confirm
from paper_sorts.logging_config import get_logger
from paper_sorts.services.paper_service import PaperService, UpdatableTable

console = Console()
logger = get_logger()


def run_update(engine: Engine) -> bool:
    """Prompt for the field to change, confirm, and apply the update.

    :param engine: the engine bound to the configured database.
    :returns: ``True`` on a confirmed, successful update; ``False`` otherwise.
    """
    table_choice = ask_choice(
        "Which information do you want to update?",
        ["papers", "bib", "authors"],
    )
    if table_choice is None:
        console.print("Stopping update process...")
        return False

    table: UpdatableTable
    if table_choice == 0:
        table = "papers"
        column_choice = ask_choice(
            "Which information do you want to update?", ["title", "contents"]
        )
        if column_choice is None:
            console.print("Stopping update process...")
            return False
        column = "title" if column_choice == 0 else "contents"
    elif table_choice == 1:
        table = "bib"
        console.print(
            "Only the bibtex can be updated - the bibtex identifier cannot be changed."
        )
        column = "bibtex"
    else:
        table = "authors_id"
        console.print("Only an author name can be updated.")
        column = "author"

    identifier = ask_nonempty(
        "Which entry do you want to update? Enter its id/key/name"
    )
    value = ask_nonempty("Enter the new information")

    if not confirm(
        f"You wish to change '{column}' of entry '{identifier}' to '{value}'."
    ):
        console.print("Stopping update process...")
        return False

    try:
        PaperService(engine).update_field(table, column, identifier, value)
    except ValueError as exc:
        logger.error("update failed: %s", exc)
        console.print("Could not update entry - please check logs.")
        return False
    console.print("Update applied.")
    return True
