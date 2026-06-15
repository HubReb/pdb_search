"""The ``update`` command: update a paper's title/contents/bibtex/author."""

from __future__ import annotations

from sqlalchemy import Engine

from paper_sorts.cli import prompts
from paper_sorts.services import paper_service
from paper_sorts.services.paper_service import UpdatableTable


def run_update(engine: Engine) -> bool:
    """Drive the interactive update flow.

    Presents the table menu, then a column choice (for ``papers``), prompts for
    the row identifier and new value, confirms the exact change (dual-form),
    and applies it. Aborting at any menu or the confirmation makes no change.

    :param engine: the database engine.
    :returns: ``True`` if a change was applied, ``False`` otherwise.
    """
    table_choice = prompts.ask_choice(
        "Which information do you want to update?",
        ["papers", "bib", "authors"],
    )
    if table_choice is None:
        return False

    table: UpdatableTable
    if table_choice == 0:
        table = "papers"
        column_choice = prompts.ask_choice(
            "Which information do you want to update?",
            ["title", "contents"],
        )
        if column_choice is None:
            return False
        column = "title" if column_choice == 0 else "contents"
    elif table_choice == 1:
        table = "bib"
        prompts.info("Only the bibtex can be updated - the identifier cannot be changed.")
        column = "bibtex"
    else:
        table = "authors_id"
        prompts.info("Only an author name can be updated.")
        column = "author"

    identifier = prompts.ask_nonempty(
        "Which entry do you want to update? Please enter the respective id/key/name"
    )
    value = prompts.ask_nonempty("Enter the new information")
    if not prompts.ask_confirm(
        f"You wish to change '{column}' of entry '{identifier}' to '{value}'."
    ):
        prompts.info("Stopping update process...")
        return False

    try:
        paper_service.update_field(engine, table, column, identifier, value)
    except ValueError as exc:
        prompts.info(f"Could not update entry: {exc}")
        return False
    prompts.info("Update applied.")
    return True
