"""``pdbsearch search`` — search papers by author or title.

Supports the non-interactive form (``--by`` + ``--query``) for scripts and
tests, and the interactive form for menu-driven sessions. Result rendering
matches the verbatim format specified in
``specs/001-modernize-stack/contracts/cli-commands.md``.

The :func:`search_and_pick` helper extracts the interactive
search-axis → query → disambiguate flow into a reusable function so that
``pdbsearch update`` (papers table) and ``pdbsearch delete`` can share
the same dialog without re-implementing it (per spec 002-ux-polish
FR-009 and FR-013).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

import typer
from sqlalchemy.orm import Session, sessionmaker

from paper_sorts.cli.prompts import ask_choice, ask_text
from paper_sorts.db.session import with_session
from paper_sorts.services.paper_service import PaperService

if TYPE_CHECKING:
    from paper_sorts.db.repositories import PaperSummary


def search(
    ctx: typer.Context,
    by: Annotated[
        str | None,
        typer.Option("--by", help="Search axis: 'author' or 'title'."),
    ] = None,
    query: Annotated[
        str | None,
        typer.Option("--query", help="Search query string."),
    ] = None,
) -> None:
    """Search the database for papers by author or title.

    With both ``--by`` and ``--query`` supplied, runs non-interactively.
    With either omitted, drops into the prompt-driven dialog described in
    the CLI command contract.
    """
    factory = ctx.obj
    if by is None and query is None:
        chosen = search_and_pick(factory)
        if chosen is not None:
            _render(chosen)
        return

    if by is None:
        choice = ask_choice(
            "Please choose a method:",
            ["Search by (a)uthor", "Search by (t)itle"],
        )
        by = "author" if choice == 1 else "title"

    if by not in {"author", "title"}:
        msg = f"--by must be 'author' or 'title', got {by!r}"
        raise typer.BadParameter(msg)

    if query is None:
        prompt_text = (
            "Please enter the author's name" if by == "author" else "Please enter the paper title"
        )
        query = ask_text(prompt_text)

    with with_session(factory) as session:
        service = PaperService(session)
        results = (
            service.search_by_author(query) if by == "author" else service.search_by_title(query)
        )

    if not results:
        print(
            "Author was not found in db_connector."
            if by == "author"
            else "Paper was not found in db_connector."
        )
        return

    chosen = disambiguate(results) if len(results) > 1 else results[0]
    if chosen is None:
        return
    _render(chosen)


def search_and_pick(factory: sessionmaker[Session]) -> PaperSummary | None:
    """Run the interactive search-axis → query → disambiguate flow and return the chosen paper.

    Opens a short-lived session for the search query, then closes it
    before the disambiguation prompt — the returned :class:`PaperSummary`
    is a frozen pydantic model and does not require an active session.
    Returns ``None`` if the search matched no rows or if the user
    aborted at the disambiguation step.

    Args:
        factory: A SQLAlchemy ``sessionmaker`` (typically ``ctx.obj``).

    Returns:
        The chosen :class:`PaperSummary`, or ``None`` if no match / aborted.
    """
    print("Search interface")
    choice = ask_choice(
        "Please choose a method:",
        ["Search by (a)uthor", "Search by (t)itle"],
    )
    by = "author" if choice == 1 else "title"

    prompt_text = (
        "Please enter the author's name" if by == "author" else "Please enter the paper title"
    )
    query = ask_text(prompt_text)

    with with_session(factory) as session:
        service = PaperService(session)
        results = (
            service.search_by_author(query) if by == "author" else service.search_by_title(query)
        )

    if not results:
        print(
            "Author was not found in db_connector."
            if by == "author"
            else "Paper was not found in db_connector."
        )
        return None

    return disambiguate(results) if len(results) > 1 else results[0]


def disambiguate(results: list[PaperSummary]) -> PaperSummary | None:
    """Show a numbered list with a trailing ``abort`` option; return the choice.

    Title rows are passed as ``(label, None)`` tuples so they remain
    digit-only on the menu — every title would otherwise alias to ``t``,
    which would be a collision under the v1.4.0 alias rule. The trailing
    ``"abort"`` row keeps its alias ``a`` (uncontested on this menu).
    """
    print("Following papers found:")
    options: list[str | tuple[str, str | None]] = [(f"title: {r.title}", None) for r in results]
    options.append("abort")
    idx = ask_choice("Choose paper to extract:", options)
    if idx == len(options):
        return None
    return results[idx - 1]


def _render(paper: PaperSummary) -> None:
    """Print a single paper in the contract's verbatim format."""
    print(f"title: {paper.title}")
    print(f"authors: {' and '.join(paper.authors)}")
    print(f"summary: {paper.contents}")
    print(f"bib entry: {paper.bibtex}")
