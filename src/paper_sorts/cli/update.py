"""The ``update`` subcommand: change one editable field of an existing entry."""

from __future__ import annotations

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import (
    DuplicateBibtexKeyError,
    PaperNotFoundError,
)
from paper_sorts.services.paper_service import PaperService, UpdatableTable


def _choose_target() -> tuple[UpdatableTable, str] | None:
    """Choose which table and column to update.

    :return: a ``(table, column)`` pair, or ``None`` if the user aborts.
    """
    table_choice = prompts.ask_choice(
        "Which information do you want to update?",
        ["papers", "bib", "authors"],
    )
    if table_choice is None:
        prompts.info("Stopping update process...")
        return None
    if table_choice == 0:
        column_choice = prompts.ask_choice(
            "Which information do you want to update?",
            ["title", "contents"],
        )
        if column_choice is None:
            prompts.info("Stopping update process...")
            return None
        return "papers", ("title" if column_choice == 0 else "contents")
    if table_choice == 1:
        prompts.info("Only the BibTeX can be updated — the key cannot be changed.")
        return "bib", "bibtex"
    prompts.info("Only an author name can be updated.")
    return "authors_id", "author"


def run_update(service: PaperService) -> None:
    """Run the interactive update dialog with a summarising confirmation.

    :param service: the paper service to update through.
    """
    target = _choose_target()
    if target is None:
        return
    table, column = target
    identifier = prompts.ask_text(
        "Which entry do you want to update? Enter its id (or author name)"
    )
    new_value = prompts.ask_text("Enter the new information")
    if not prompts.confirm(
        f"You wish to change '{column}' of entry '{identifier}' to '{new_value}'. Proceed?"
    ):
        prompts.info("Stopping update process...")
        return
    try:
        service.update_field(table, column, identifier, new_value)
    except (ValueError, PaperNotFoundError, DuplicateBibtexKeyError):
        prompts.error("Could not update entry — please check logs.")
        return
    prompts.info("Entry updated.")
