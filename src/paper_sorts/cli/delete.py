"""``pdbsearch delete`` — remove a paper and its dependents.

Reachable as a Typer subcommand only — *not* from the top-level menu —
because destructive operations get friction by design (the legacy code
likewise omits delete from ``UserInteraction.interact``'s menu, see
``contracts/cli-commands.md`` § "Why only four options").

Behaviour preserved from the data-model.md cascade rules:

* ``authors_papers`` rows for the deleted paper are dropped.
* Authors that have no remaining papers afterwards are removed too.
* The bib row is deleted only if no other paper still references it.
* The whole sequence runs in a single transaction.

The mandatory confirmation shows the paper's id *and* title so the user
sees what they are about to remove.
"""

from __future__ import annotations

from typing import Annotated

import typer

from paper_sorts.cli.prompts import ask_confirm, ask_text
from paper_sorts.db.session import with_session
from paper_sorts.services.paper_service import PaperService


def delete(
    ctx: typer.Context,
    paper_id: Annotated[
        int | None,
        typer.Option("--id", help="Paper id to delete."),
    ] = None,
) -> None:
    """Delete a paper after a mandatory confirmation."""
    factory = ctx.obj

    if paper_id is None:
        id_str = ask_text("Please enter the paper id to delete")
        try:
            paper_id = int(id_str)
        except ValueError:
            print(f"Error: id {id_str!r} is not an integer.")
            return

    with with_session(factory) as session:
        service = PaperService(session)
        paper = service.find_by_id(paper_id)
        if paper is None:
            print(f"Error: no paper with id {paper_id}.")
            return
        title = paper.title

    print(
        f"Please verify: You wish to DELETE paper id {paper_id} ({title!r}). This cannot be undone."
    )
    print("1) (Y)es")
    print("2) (N)o")
    if not ask_confirm("Your choice"):
        return

    with with_session(factory) as session:
        service = PaperService(session)
        service.delete_paper(paper_id)
    print(f"Deleted paper id {paper_id}.")
