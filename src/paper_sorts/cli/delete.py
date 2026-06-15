"""The ``delete`` command: find a paper, confirm, and remove it."""

from __future__ import annotations

from sqlalchemy import Engine

from paper_sorts.cli import prompts
from paper_sorts.services import paper_service


def run_delete(engine: Engine) -> bool:
    """Drive the interactive delete flow.

    Searches by title, disambiguates if needed, summarises the paper to be
    removed, confirms (dual-form), and deletes it along with its bib entry,
    author links, and now-orphaned authors. Aborting makes no change.

    :param engine: the database engine.
    :returns: ``True`` if a paper was deleted, ``False`` otherwise.
    """
    title = prompts.ask_nonempty("Title of the paper to delete")
    results = paper_service.search_by_title(engine, title.strip())
    if not results:
        prompts.info("Paper was not found.")
        return False
    if len(results) > 1:
        chosen = prompts.pick_from("Following papers found:", results)
        if chosen is None:
            return False
    else:
        chosen = results[0]

    if not prompts.ask_confirm(
        f"You are about to delete '{chosen.title}' by {chosen.authors} (key {chosen.bibtex_id})."
    ):
        prompts.info("Deletion aborted.")
        return False

    paper_service.delete_paper(engine, chosen.paper_id)
    prompts.info("Paper deleted.")
    return True
