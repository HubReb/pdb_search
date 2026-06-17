"""Interactive ``update`` flow: choose table, column, row, new value, confirm.

Mirrors the legacy ``update_entry``: IDs are never editable, ``bib`` exposes only
``bibtex`` (the key is immutable), ``authors_id`` exposes only ``author``. The
change is summarised and confirmed (accepting numeric and word forms) before it
is written; declining writes nothing.
"""

from __future__ import annotations

import logging

from rich.console import Console

from paper_sorts.cli.prompts import ABORT, ask_choice, ask_text, confirm
from paper_sorts.db.repositories import DuplicateBibtexError, PaperNotFoundError
from paper_sorts.db.session import DbEngine
from paper_sorts.services import paper_service
from paper_sorts.services.paper_service import UnknownColumnError, UpdatableTable

_logger = logging.getLogger(__name__)
_console = Console()


def _choose_papers_column() -> str | None:
    """Choose an editable ``papers`` column.

    :return: ``"title"`` or ``"contents"``, or ``None`` if aborted.
    """
    choice = ask_choice("Which field?", ["title", "contents"])
    if isinstance(choice, str):
        return None
    return ["title", "contents"][choice]


def run_update(engine: DbEngine) -> None:
    """Drive the interactive update dialog.

    :param engine: the database engine.
    """
    table_choice = ask_choice(
        "Which table do you want to update?",
        ["papers", "bib", "authors"],
    )
    if table_choice == ABORT:
        return

    table: UpdatableTable
    column: str
    id_prompt: str
    if table_choice == 0:
        table = "papers"
        picked = _choose_papers_column()
        if picked is None:
            return
        column = picked
        id_prompt = "Paper id"
    elif table_choice == 1:
        table = "bib"
        column = "bibtex"
        id_prompt = "BibTeX key"
    else:
        table = "authors_id"
        column = "author"
        id_prompt = "Author id"

    identifier = ask_text(id_prompt)
    value = ask_text(f"New value for {column}")

    _console.print(f"About to set {table}.{column} = {value!r} for {id_prompt} {identifier!r}.")
    if not confirm("Apply this change?"):
        _console.print("No change made.")
        return

    try:
        paper_service.update_field(engine, table, column, value, identifier)
    except (PaperNotFoundError, UnknownColumnError, DuplicateBibtexError) as exc:
        _logger.warning("update_field failed: %s", exc)
        _console.print("Could not apply that update — nothing was changed.")
        return
    except ValueError as exc:
        _logger.warning("update_field bad identifier: %s", exc)
        _console.print("That identifier is not valid — nothing was changed.")
        return
    _console.print("Update applied.")
