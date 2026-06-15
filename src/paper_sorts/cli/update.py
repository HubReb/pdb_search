"""Interactive update flow for the CLI.

Walks the table → column sub-menus, prompts for the identifier and new value, then requires a
dual-form confirmation summarising the exact change before it is applied. Declining writes
nothing.
"""

from __future__ import annotations

import logging

from paper_sorts.cli import prompts
from paper_sorts.services.paper_service import PaperService, UpdatableTable

logger = logging.getLogger(__name__)


def run_update(service: PaperService) -> bool:
    """Run the interactive update dialog.

    :param service: the paper service to update through.
    :return: ``True`` if a change was applied, ``False`` if aborted or on a handled failure.
    """
    table_choice = prompts.ask_choice(
        "Which information do you want to update?",
        ["papers", "bib", "authors", "abort"],
    )
    table: UpdatableTable
    if table_choice == 0:
        table = "papers"
        column_choice = prompts.ask_choice(
            "Which information do you want to update?", ["title", "contents", "abort"]
        )
        if column_choice == 2:
            prompts.show("Stopping update process...")
            return False
        column = "title" if column_choice == 0 else "contents"
    elif table_choice == 1:
        table = "bib"
        column = "bibtex"
        prompts.show("Only the BibTeX can be updated - the BibTeX key cannot be changed.")
    elif table_choice == 2:
        table = "authors_id"
        column = "author"
        prompts.show("Only an author name can be updated.")
    else:
        prompts.show("Stopping update process...")
        return False

    identifier = prompts.ask_nonempty("Which entry do you want to update? Enter its identifier")
    value = prompts.ask_nonempty("Enter the new information")

    if not prompts.confirm(
        f"You wish to change '{column}' of entry '{identifier}' to '{value}'. Proceed?"
    ):
        prompts.show("Stopping update process...")
        return False

    try:
        service.update_field(table, column, value, identifier)
    except ValueError as exc:
        logger.error("update failed: %s", exc)
        prompts.show("Could not update the entry - please check the logs.")
        return False
    prompts.show("Updated.")
    return True
