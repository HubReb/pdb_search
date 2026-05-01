"""``pdbsearch update`` — update one editable field on a single row.

Preserves the legacy ``UserInteraction.update`` two-step menu verbatim:

* table: papers / bib / authors / abort
* field: depends on the chosen table; ``abort`` always present
* identifier: ``int`` for ``papers`` / ``authors``, ``str`` for ``bib``
* value: free-form text
* confirmation: ``1``/``y``/``yes`` proceed, ``2``/``n``/``no`` abort

Updating ``papers.bibtex_id`` or ``bib.bibtex_id`` is rejected by the
service with a plain-language ``ValueError`` (the BibTeX identifier
itself is not editable; only the source string is). This command catches
``ValueError`` / ``TypeError`` from the service and renders the message
without a stack trace.
"""

from __future__ import annotations

from typing import Literal

import typer

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_text
from paper_sorts.db.session import with_session
from paper_sorts.services.paper_service import PaperService

_TABLE_FIELDS: dict[str, list[str]] = {
    "papers": ["title", "contents", "abort"],
    "bib": ["bibtex", "abort"],
    "authors": ["author", "abort"],
}


def update(ctx: typer.Context) -> None:
    """Drive the legacy two-step update dialog and apply the change."""
    table = _pick_table()
    if table is None:
        return

    field = _pick_field(table)
    if field is None:
        return

    id_str = ask_text("Which entry do you want to update?\nPlease enter the respective id")
    identifier = _coerce_identifier(table, id_str)
    if identifier is None:
        return

    value = ask_text("Enter the new information")

    print(f"Please verify: You wish to change {field!r} of the entry {id_str!r} to {value!r}.")
    print(" Proceed?")
    print("1) (Y)es")
    print("2) (N)o")
    if not ask_confirm("Your choice"):
        return

    factory = ctx.obj
    try:
        with with_session(factory) as session:
            service = PaperService(session)
            service.update_field(table, field, identifier, value)
    except (ValueError, TypeError) as e:
        print(f"Error: {e}")
        return
    print(f"Updated {table}.{field} for entry {id_str}.")


def _pick_table() -> Literal["papers", "bib", "authors"] | None:
    """Show the first menu and return the chosen table, or ``None`` on abort."""
    options = ["papers", "bib", "authors", "abort"]
    choice = ask_choice("Which information do you want to update?", options)
    match choice:
        case 1:
            return "papers"
        case 2:
            return "bib"
        case 3:
            return "authors"
        case _:
            return None


def _pick_field(table: Literal["papers", "bib", "authors"]) -> str | None:
    """Show the table-specific field menu and return the choice, or ``None``."""
    options = _TABLE_FIELDS[table]
    choice = ask_choice("Which information do you want to update?", options)
    if choice == len(options):  # last entry is always "abort"
        return None
    return options[choice - 1]


def _coerce_identifier(table: Literal["papers", "bib", "authors"], id_str: str) -> int | str | None:
    """Coerce ``id_str`` to the table's identifier type or report a plain-language error."""
    if table == "bib":
        return id_str
    try:
        return int(id_str)
    except ValueError:
        print(f"Error: id {id_str!r} is not an integer.")
        return None
