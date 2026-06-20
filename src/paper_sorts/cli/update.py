"""Interactive ``update`` flow (presentation layer)."""

from __future__ import annotations

import logging

from paper_sorts.cli import prompts
from paper_sorts.services.paper_service import PaperService, UpdatableTable

_logger = logging.getLogger(__name__)


def run_update(service: PaperService) -> bool:
    """Drive the interactive update dialog.

    :param service: the bound paper service.
    :returns: ``True`` if a change was written, ``False`` if aborted/failed.
    """
    table_choice = prompts.ask_choice(
        "Which information do you want to update?",
        ["papers", "bib", "authors"],
    )
    if table_choice is None:
        prompts.info("Stopping update process...")
        return False

    table: UpdatableTable
    if table_choice == 0:
        table = "papers"
        column_choice = prompts.ask_choice(
            "Which column do you want to update?", ["title", "contents"]
        )
        if column_choice is None:
            prompts.info("Stopping update process...")
            return False
        column = "title" if column_choice == 0 else "contents"
    elif table_choice == 1:
        table = "bib"
        prompts.info("Only the bibtex can be updated — the bibtex key cannot be changed.")
        column = "bibtex"
    else:
        table = "authors_id"
        prompts.info("Only an author name can be updated.")
        column = "author"

    identifier = prompts.ask_text("Which entry? Please enter its id: ")
    new_value = prompts.ask_text("Enter the new information: ")

    if not prompts.confirm(
        f"You wish to change '{column}' of entry '{identifier}' to '{new_value}'."
    ):
        prompts.info("Stopping update process...")
        return False

    try:
        service.update_field(table, column, identifier, new_value)
    except ValueError as exc:
        _logger.error("update failed: %s", exc)
        prompts.info("Could not update the entry — please check the logs.")
        return False
    return True
