"""The ``delete`` subcommand: remove a paper after a summarising confirmation."""

from __future__ import annotations

from paper_sorts.cli import prompts
from paper_sorts.cli.search import pick_paper
from paper_sorts.db.repositories import PaperNotFoundError
from paper_sorts.services.paper_service import PaperService


def run_delete(service: PaperService) -> None:
    """Locate a paper by title, confirm, and delete it with its dependents.

    :param service: the paper service to delete through.
    """
    title = prompts.ask_text("Enter the title of the paper to delete")
    papers = service.search_by_title(title)
    if not papers:
        prompts.error("Paper was not found in database.")
        return
    chosen = pick_paper(papers)
    if chosen is None:
        prompts.info("Stopping delete process...")
        return
    authors = " and ".join(chosen.authors)
    if not prompts.confirm(
        f"You wish to delete '{chosen.title}' by {authors} "
        f"(key '{chosen.bibtex_id}'). Proceed?"
    ):
        prompts.info("Stopping delete process...")
        return
    try:
        service.delete_paper(chosen)
    except PaperNotFoundError:
        prompts.error("Could not delete entry — please check logs.")
        return
    prompts.info("Entry deleted.")
