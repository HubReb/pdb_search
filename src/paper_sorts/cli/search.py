"""The ``search`` command: search by author or title, then display a paper."""

from __future__ import annotations

from sqlalchemy import Engine

from paper_sorts.cli import prompts
from paper_sorts.services import paper_service


def run_search(engine: Engine) -> None:
    """Drive the interactive search flow.

    Presents the author/title sub-menu, runs the chosen search, disambiguates
    when several papers share a title, and pretty-prints the result. Not-found
    yields a plain-language message.

    :param engine: the database engine.
    """
    choice = prompts.ask_choice(
        "Search interface\nPlease choose a method:",
        ["Search by author", "Search by paper title"],
    )
    if choice is None:
        return
    if choice == 0:
        author = prompts.ask_nonempty("Please enter the author's name")
        results = paper_service.search_by_author(engine, author.strip())
        if not results:
            prompts.info("Author was not found.")
            return
    else:
        title = prompts.ask_nonempty("Please enter the paper title")
        results = paper_service.search_by_title(engine, title.strip())
        if not results:
            prompts.info("Paper was not found.")
            return

    if len(results) > 1:
        chosen = prompts.pick_from("Following papers found:", results)
        if chosen is None:
            return
    else:
        chosen = results[0]
    prompts.display_paper(chosen)
