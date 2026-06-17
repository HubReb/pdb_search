"""Interactive delete flow: identify, summarise, confirm, and remove a paper."""

from __future__ import annotations

from rich.console import Console
from sqlalchemy import Engine

from paper_sorts.cli.prompts import ask_nonempty, confirm
from paper_sorts.db.repositories import NotFoundError
from paper_sorts.logging_config import get_logger
from paper_sorts.services.paper_service import PaperService

console = Console()
logger = get_logger()


def run_delete(engine: Engine) -> bool:
    """Prompt for a paper by BibTeX key, confirm, and delete it.

    :param engine: the engine bound to the configured database.
    :returns: ``True`` on a confirmed, successful deletion; ``False`` otherwise.
    """
    service = PaperService(engine)
    bibtex_id = ask_nonempty("Enter the bibtex key of the paper to delete")
    title = ask_nonempty("Enter the paper title to confirm")

    if not confirm(f"You wish to delete '{title}' ({bibtex_id})."):
        console.print("Stopping delete process...")
        return False

    try:
        service.delete_paper(bibtex_id)
    except NotFoundError as exc:
        logger.info("delete rejected: %s", exc)
        console.print("Could not delete: no such paper.")
        return False
    except ValueError as exc:
        logger.error("delete failed: %s", exc)
        console.print("Could not delete entry - please check logs.")
        return False
    console.print(f"Deleted '{title}'.")
    return True
