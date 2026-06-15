"""The ``add`` subcommand: add a paper inline or from a ``.bib`` file."""

from __future__ import annotations

from paper_sorts.cli import prompts
from paper_sorts.db.repositories import DuplicateBibtexKeyError, PaperCreate
from paper_sorts.services.paper_service import PaperService


def run_add(service: PaperService) -> None:
    """Prompt for a new paper's details and persist it.

    Authors are entered as a comma-separated ``"Last, First"`` list (split on
    ``", "`` to match the legacy behaviour). The BibTeX entry may be typed
    inline or read from a file.

    :param service: the paper service to add through.
    """
    author_line = prompts.ask_text(
        "Author(s) — provide a comma-separated 'Last, First' list"
    )
    title = prompts.ask_text("Paper title")
    bibtex_key = prompts.ask_text("BibTeX key")
    via_file = prompts.ask_choice(
        "Enter the BibTeX entry via a separate file?",
        ["Yes", "No"],
    )
    if via_file is None:
        prompts.info("Stopping add process...")
        return
    if via_file == 0:
        filename = prompts.ask_text("Enter filename")
        try:
            with open(filename, encoding="utf-8") as handle:
                bibtex = handle.read()
        except OSError as exc:
            prompts.error(f"Could not read file {filename} — please check the path.")
            prompts.error(str(exc))
            return
    else:
        bibtex = prompts.ask_text("BibTeX entry")
    summary = prompts.ask_text("Summary of the paper")
    authors = author_line.split(", ")
    try:
        service.add_paper(
            PaperCreate(
                title=title,
                authors=authors,
                summary=summary,
                bibtex_id=bibtex_key,
                bibtex=bibtex,
            )
        )
    except DuplicateBibtexKeyError:
        prompts.error(f"A paper with BibTeX key '{bibtex_key}' already exists.")
        return
    prompts.info(f"Added entry {', '.join(authors)}: {title}")
