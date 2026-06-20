"""Add subcommand for pdbsearch.

Provides :func:`add_cmd` — a Typer command that lets the user add a new paper
either by typing all fields inline or by pointing at a ``.bib`` file for the
BibTeX data.
"""

from __future__ import annotations

import logging

import typer

from paper_sorts.cli.prompts import ask_choice, ask_nonempty
from paper_sorts.db.repositories import PaperCreate
from paper_sorts.db.session import with_session
from paper_sorts.services import paper_service

logger = logging.getLogger(__name__)

app = typer.Typer(help="Add a new paper to the database.")


def _parse_bib_file(bib_path: str) -> tuple[str, list[str], str]:
    """Parse a single-entry .bib file and return (bibtex_text, authors, bibtex_key).

    :param bib_path: Filesystem path to the ``.bib`` file.
    :returns: Tuple of (full bibtex source, list of author names, citation key).
    :raises ValueError: If the file cannot be parsed or contains more than one
        entry.
    """
    from pybtex.database import parse_file

    bib_graph = parse_file(bib_path, bib_format="bibtex")
    keys = list(bib_graph.entries.keys())
    if len(keys) != 1:
        raise ValueError(
            f"Expected exactly one BibTeX entry in {bib_path!r}, got {len(keys)}"
        )
    key = keys[0]
    entry = bib_graph.entries[key]
    bibtex_text = entry.to_string("bibtex")
    authors: list[str] = []
    for person in entry.persons.get("author", []):
        last = " ".join(person.last_names) if person.last_names else ""
        first = " ".join(person.first_names) if person.first_names else ""
        authors.append(f"{last}, {first}".strip(", "))
    return bibtex_text, authors, str(key)


def run_add(database_url: str) -> None:
    """Interactive add flow (called from interactive menu or subcommand).

    :param database_url: PostgreSQL DSN.
    """
    title = ask_nonempty("Paper title")
    summary = ask_nonempty("Summary (one sentence)")

    # Choose between inline entry and .bib file
    input_options = ["Enter BibTeX key and authors manually", "Load from .bib file", "Abort"]
    choice = ask_choice(input_options, "BibTeX source")
    if choice == 3:
        print("Add aborted.")
        return

    if choice == 1:
        bibtex_key = ask_nonempty("BibTeX citation key (e.g. Wang2021LargeScale)")
        bibtex_text = ask_nonempty("Full BibTeX entry (paste text)")
        author_count_raw = ask_nonempty("Number of authors")
        try:
            n_authors = int(author_count_raw)
        except ValueError:
            n_authors = 1
        authors: list[str] = []
        for i in range(max(1, n_authors)):
            name = ask_nonempty(f"Author {i + 1} name (Last, First)")
            authors.append(name)
    else:
        bib_path = ask_nonempty("Path to .bib file")
        try:
            bibtex_text, authors, bibtex_key = _parse_bib_file(bib_path)
        except (ValueError, OSError) as exc:
            logger.error("Failed to parse .bib file: %s", exc)
            print(f"Error reading .bib file: {exc}")
            return

    paper = PaperCreate(
        title=title,
        authors=authors,
        bibtex_key=bibtex_key,
        summary=summary,
        bibtex_text=bibtex_text,
    )
    try:
        result = paper_service.add_paper(
            paper, database_url=database_url, with_session_fn=with_session
        )
        print(f"\nAdded paper: {result.title!r} (id={result.paper_id})")
    except Exception as exc:
        logger.error("Failed to add paper: %s", exc)
        print("Error: could not add paper. Check logs for details.")


@app.callback(invoke_without_command=True)
def add_cmd(
    ctx: typer.Context,
    database_url: str = typer.Option(
        None, "--database-url", envvar="PDBSEARCH_DATABASE_URL", help="PostgreSQL DSN"
    ),
) -> None:
    """Add a new paper to the database."""
    if ctx.invoked_subcommand is not None:
        return
    if not database_url:
        typer.echo("Error: database URL not configured.", err=True)
        raise typer.Exit(1)
    run_add(database_url)
