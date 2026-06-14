"""All user-facing prompts for paper_sorts.

This is the ONLY module in src/paper_sorts/ permitted to import rich.prompt
(constitution Principle III). Every interactive prompt must go through this module.
No bare input() calls are allowed elsewhere in src/paper_sorts/.

Grammar rules (constitution III):
- Numbered menus are 1-indexed in display
- Every menu includes an explicit abort/quit option
- Destructive operations require a confirmation step
- Confirmation accepts 1/y/yes (proceed) or 2/n/no (cancel)
- Empty required input causes re-prompt until non-empty value given
"""

from __future__ import annotations

import logging

from rich.prompt import Prompt

from paper_sorts.db.repositories import PaperSummary

logger = logging.getLogger("paper_sorts.cli.prompts")

# Sentinel returned when the user selects the abort/quit option
ABORT = "__ABORT__"


def ask_text(prompt_text: str, *, allow_empty: bool = False) -> str:
    """Prompt the user for a non-empty text value.

    Loops until the user provides a non-empty string (unless allow_empty=True).

    :param prompt_text: instruction shown to the user
    :type prompt_text: str
    :param allow_empty: if True, allow empty responses
    :type allow_empty: bool
    :return: the user's input (stripped)
    :rtype: str
    """
    while True:
        response = Prompt.ask(prompt_text, default="")
        stripped = response.strip()
        if stripped or allow_empty:
            return stripped
        # re-prompt silently (loop)


def ask_confirmation(message: str) -> bool:
    """Ask the user to confirm a destructive operation.

    Displays a 1/y/yes or 2/n/no confirmation prompt.
    Accepts: 1, y, yes (→ True) or 2, n, no (→ False).
    Invalid input re-prompts until a valid response is given.

    :param message: description of the action to confirm
    :type message: str
    :return: True if user confirms, False if user denies
    :rtype: bool
    """
    print(message)
    while True:
        response = Prompt.ask("Proceed?\n1) (Y)es\n2) (N)o\nYour choice", default="")
        match response.strip().lower():
            case "1" | "y" | "yes":
                return True
            case "2" | "n" | "no":
                return False
            case _:
                print("Please enter 1/y/yes to proceed or 2/n/no to cancel.")


def ask_choice(
    items: list[str],
    header: str = "Please choose an option:",
    abort_label: str = "Abort",
) -> int | str:
    """Present a 1-indexed numbered menu and return the chosen index (0-based) or ABORT.

    Always includes an explicit abort option as the last item.
    Invalid input (out of range or non-numeric) re-prompts.

    :param items: list of option strings to display
    :type items: list[str]
    :param header: header line shown above the menu
    :type header: str
    :param abort_label: label for the abort/quit option
    :type abort_label: str
    :return: 0-based index of chosen item, or ABORT sentinel if user chose abort
    :rtype: int | str
    """
    display_items = [*items, f"({abort_label[0]}){abort_label[1:]}"]
    abort_index = len(display_items)  # 1-indexed position of abort option

    while True:
        print(header)
        for i, item in enumerate(display_items, start=1):
            print(f"{i}) {item}")

        response = Prompt.ask("Your choice", default="")
        stripped = response.strip().lower()

        # Accept abort by letter too
        if stripped == abort_label[0].lower() or stripped == abort_label.lower():
            return ABORT

        try:
            choice = int(stripped)
        except ValueError:
            print("Please enter a number.")
            continue

        if choice == abort_index:
            return ABORT
        if 1 <= choice <= len(items):
            return choice - 1  # convert to 0-based

        print(f"Please enter a number between 1 and {abort_index}.")


def ask_search_method() -> int | str:
    """Ask the user which search method to use.

    :return: 0 for search-by-author, 1 for search-by-title, or ABORT
    :rtype: int | str
    """
    return ask_choice(
        ["Search by author", "Search by paper title"],
        header="Search interface\nPlease choose a method:",
        abort_label="Quit",
    )


def ask_paper_from_list(papers: list[PaperSummary]) -> PaperSummary | None:
    """Ask the user to pick one paper from a list of results.

    Used for disambiguation when multiple papers match a search query.

    :param papers: list of papers to choose from
    :type papers: list[PaperSummary]
    :return: chosen PaperSummary or None if user aborted
    :rtype: PaperSummary | None
    """
    items = [f"{p.title} (key: {p.bibtex_id})" for p in papers]
    result = ask_choice(items, header="Following papers found:\nChoose a paper:")
    if result is ABORT:
        return None
    return papers[int(result)]


def pretty_print_paper(paper: PaperSummary) -> None:
    """Display a paper's details in a readable format.

    :param paper: PaperSummary DTO to display
    :type paper: PaperSummary
    """
    authors_str = " and ".join(paper.authors)
    print(f"title: {paper.title}")
    print(f"authors: {authors_str}")
    print(f"summary: {paper.contents}")
    print(f"bib entry: {paper.bibtex}")


def ask_update_table() -> str | None:
    """Ask which table/field group to update.

    :return: one of 'papers', 'bib', 'authors', or None if aborted
    :rtype: str | None
    """
    result = ask_choice(
        ["papers (title / summary)", "bib (BibTeX entry)", "authors (author name)"],
        header="Which information do you want to update?",
        abort_label="Abort",
    )
    if result is ABORT:
        return None
    mapping = {0: "papers", 1: "bib", 2: "authors"}
    return mapping[int(result)]


def ask_papers_column() -> str | None:
    """Ask which column of the papers table to update.

    :return: 'title', 'contents', or None if aborted
    :rtype: str | None
    """
    result = ask_choice(
        ["title", "summary (contents)"],
        header="Which field do you want to update?",
        abort_label="Abort",
    )
    if result is ABORT:
        return None
    mapping = {0: "title", 1: "contents"}
    return mapping[int(result)]


def ask_bibtex_source() -> str | None:
    """Ask whether to load BibTeX from a file or enter it inline.

    :return: 'file', 'inline', or None if aborted
    :rtype: str | None
    """
    result = ask_choice(
        ["Load BibTeX from file", "Enter BibTeX inline"],
        header="How do you want to provide the BibTeX entry?",
        abort_label="Abort",
    )
    if result is ABORT:
        return None
    mapping = {0: "file", 1: "inline"}
    return mapping[int(result)]
