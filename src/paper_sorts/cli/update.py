"""``pdbsearch update`` — update one editable field on a single row.

Two-step menu (table → field) plus a row-identification step that branches
on the chosen table:

* **papers** — the row is identified via the interactive search dialog
  (axis → query → disambiguate, factored out of :mod:`paper_sorts.cli.search`
  as :func:`search_and_pick`). When the optional ``--id <N>`` flag is
  supplied, the search step is skipped and the row is fetched directly by
  id — table, field, and new-value collection still run interactively. No
  ``--table``/``--field``/``--value`` flags are added in this feature
  (per spec 002-ux-polish § Clarifications).
* **bib / authors** — the legacy ``Please enter the respective id`` prompt
  is preserved verbatim. Search-by-bibtex-key and search-by-author-row are
  not part of this feature.

Confirmation summary diverges by table: the papers path echoes both the
picked paper's title and its id (`'... of the paper '<title>' (id <N>) ...'`)
so the user verifies what they recognised in the disambig list while the
id remains visible for log/audit traceability; the bib/authors paths
keep their legacy `'... of the entry '<id>' ...'` wording (the typed
identifier is already the canonical handle).

Updating ``papers.bibtex_id`` or ``bib.bibtex_id`` is rejected by the
service with a plain-language ``ValueError``. This command catches
``ValueError`` / ``TypeError`` from the service and renders the message
without a stack trace.
"""

from __future__ import annotations

from typing import Annotated, Literal

import typer
from sqlalchemy.orm import Session, sessionmaker

from paper_sorts.cli.prompts import ask_choice, ask_confirm, ask_text
from paper_sorts.cli.search import search_and_pick
from paper_sorts.db.session import with_session
from paper_sorts.services.paper_service import PaperService

# Type alias: each row-identification step yields (identifier, display_id, paper_title).
# `paper_title` is `None` on the bib/authors raw-id path (no canonical title in scope)
# and the picked paper's title on the papers path.
_Target = tuple[int | str, str, str | None]


def update(
    ctx: typer.Context,
    paper_id: Annotated[
        int | None,
        typer.Option(
            "--id",
            help="Paper id; bypasses search-then-pick on the papers table. "
            "No effect on bib/authors raw-id paths.",
        ),
    ] = None,
) -> None:
    """Drive the two-step update dialog and apply the change."""
    table = _pick_table()
    if table is None:
        return

    field = _pick_field(table)
    if field is None:
        return

    factory = ctx.obj
    target = (
        _resolve_papers_target(factory, paper_id)
        if table == "papers"
        else _resolve_legacy_target(table)
    )
    if target is None:
        return
    identifier, display_id, paper_title = target

    value = ask_text("Enter the new information")

    if not _confirm_update(table, field, identifier, paper_title, display_id, value):
        return

    try:
        with with_session(factory) as session:
            PaperService(session).update_field(table, field, identifier, value)
    except (ValueError, TypeError) as e:
        print(f"Error: {e}")
        return
    print(f"Updated {table}.{field} for entry {display_id}.")


def _resolve_papers_target(factory: sessionmaker[Session], paper_id: int | None) -> _Target | None:
    """Return the row identifier + display id + title for the papers path.

    With ``paper_id`` set, the row is fetched directly by id; an unknown
    id prints a plain-language error and returns ``None``. With
    ``paper_id`` unset, the interactive search-then-pick dialog runs.
    Returns ``None`` if the search matched nothing or the user aborted.
    """
    if paper_id is not None:
        with with_session(factory) as session:
            paper = PaperService(session).find_by_id(paper_id)
        if paper is None:
            print(f"Error: no paper with id {paper_id}.")
            return None
        return paper.id, str(paper.id), paper.title

    chosen = search_and_pick(factory)
    if chosen is None:
        return None
    return chosen.id, str(chosen.id), chosen.title


def _resolve_legacy_target(table: Literal["bib", "authors"]) -> _Target | None:
    """Return the row identifier + display id for the bib/authors raw-id path.

    Preserves the legacy ``Please enter the respective id`` prompt verbatim.
    Returns ``None`` if the user-typed id cannot be coerced to the table's
    identifier type.
    """
    id_str = ask_text("Which entry do you want to update?\nPlease enter the respective id")
    coerced = _coerce_identifier(table, id_str)
    if coerced is None:
        return None
    return coerced, id_str, None


def _confirm_update(
    table: Literal["papers", "bib", "authors"],
    field: str,
    identifier: int | str,
    paper_title: str | None,
    display_id: str,
    value: str,
) -> bool:
    """Print the table-aware confirmation summary and read the user's response."""
    if table == "papers":
        print(
            f"Please verify: You wish to change {field!r} of the paper "
            f"{paper_title!r} (id {identifier}) to {value!r}."
        )
    else:
        print(
            f"Please verify: You wish to change {field!r} of the entry {display_id!r} to {value!r}."
        )
    print(" Proceed?")
    print("1) (Y)es")
    print("2) (N)o")
    return ask_confirm("Your choice")


def _pick_table() -> Literal["papers", "bib", "authors"] | None:
    """Show the first menu and return the chosen table, or ``None`` on abort.

    The trailing slot is labelled ``"(q)uit"`` (alias ``q``) rather than
    ``"abort"`` because ``"authors"`` and ``"abort"`` would both
    auto-derive the alias ``a``, triggering the v1.4.0 collision-rejection
    rule on construction.
    """
    options = ["papers", "bib", "authors", "(q)uit"]
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
    """Show the table-specific field menu and return the choice, or ``None``.

    The trailing slot is ``"(q)uit"`` for menu-grammar consistency with
    ``_pick_table``.
    """
    match table:
        case "papers":
            options = ["title", "contents", "(q)uit"]
        case "bib":
            options = ["bibtex", "(q)uit"]
        case "authors":
            options = ["author", "(q)uit"]
    choice = ask_choice("Which information do you want to update?", options)
    if choice == len(options):  # last entry is always the abort/quit slot
        return None
    return options[choice - 1]


def _coerce_identifier(table: Literal["bib", "authors"], id_str: str) -> int | str | None:
    """Coerce ``id_str`` to the table's identifier type or report a plain-language error.

    The papers table no longer uses this helper — papers are identified via
    :func:`_resolve_papers_target` (search-pick or ``--id``).
    """
    if table == "bib":
        return id_str
    try:
        return int(id_str)
    except ValueError:
        print(f"Error: id {id_str!r} is not an integer.")
        return None
