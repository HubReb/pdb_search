"""Interactive add flow for the CLI.

Collects author/title/key/bibtex/summary through ``cli/prompts`` (empty input re-prompts) and
persists the paper via the service. The BibTeX entry may be typed inline or read from a file.
"""

from __future__ import annotations

import logging

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.services.paper_service import PaperService

logger = logging.getLogger(__name__)


def run_add(service: PaperService) -> bool:
    """Run the interactive add dialog.

    :param service: the paper service to add through.
    :return: ``True`` if the paper was added, ``False`` on a handled failure.
    """
    author_line = prompts.ask_nonempty("Author(s), comma-separated as 'Last, First'")
    title = prompts.ask_nonempty("Paper title")
    bibtex_key = prompts.ask_nonempty("BibTeX key")
    source = prompts.ask_choice("Provide the BibTeX entry from a file?", ["Yes", "No"])
    if source == 0:
        filename = prompts.ask_nonempty("Filename")
        try:
            with open(filename, encoding="utf-8") as handle:
                bibtex = handle.read()
        except OSError as exc:
            logger.error("could not read bib file %s: %s", filename, exc)
            prompts.show("Could not read the BibTeX file - please check the path.")
            return False
    else:
        bibtex = prompts.ask_nonempty("BibTeX entry")
    summary = prompts.ask_nonempty("Summary")

    # Authors are entered as "Last, First" pairs separated by commas; rejoin the pairs.
    authors = _pair_authors(author_line)

    try:
        service.add_paper(
            PaperCreate(
                title=title,
                summary=summary,
                bibtex_id=bibtex_key,
                bibtex=bibtex,
                authors=authors,
            )
        )
    except ValueError as exc:
        logger.error("add failed: %s", exc)
        prompts.show("Could not add the entry - please check the logs.")
        return False
    prompts.show(f"Added: {title}")
    return True


def _pair_authors(author_line: str) -> list[str]:
    """Group a comma-separated ``Last, First`` author line into whole names.

    The input convention is ``Last, First`` per author, so consecutive comma-separated tokens
    are paired back into ``"Last, First"`` strings. A trailing unpaired token is kept as-is.

    :param author_line: the raw author line as typed by the user.
    :return: a list of ``"Last, First"`` author names.
    """
    tokens = [t.strip() for t in author_line.split(",") if t.strip()]
    authors: list[str] = []
    i = 0
    while i < len(tokens):
        if i + 1 < len(tokens):
            authors.append(f"{tokens[i]}, {tokens[i + 1]}")
            i += 2
        else:
            authors.append(tokens[i])
            i += 1
    return authors
